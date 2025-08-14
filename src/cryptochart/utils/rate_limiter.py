import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Self


class AsyncRateLimiter:
    """
    An asynchronous rate limiter using the token bucket algorithm.

    This class provides a mechanism to limit the rate of operations, which is
    essential for respecting API rate limits. It uses an `asyncio.Queue` as
    the token bucket, making it efficient and safe for concurrent use.

    The limiter can be used as an async context manager for automatic
    startup and shutdown of the token refill mechanism.

    Usage:
        limiter = AsyncRateLimiter(10, 1)  # 10 requests per second
        async with limiter:
            for _ in range(25):
                async with limiter.acquire():
                    # This block will be entered at most 10 times per second.
                    await make_api_call()
    """

    def __init__(self, rate_limit: int, period_sec: float = 1.0):
        """
        Initializes the rate limiter.

        Args:
            rate_limit: The maximum number of requests allowed in a period.
            period_sec: The time period in seconds.
        """
        if not isinstance(rate_limit, int) or rate_limit <= 0:
            raise ValueError("Rate limit must be a positive integer.")
        if not isinstance(period_sec, (int, float)) or period_sec <= 0:
            raise ValueError("Period must be a positive number.")

        self.rate_limit = rate_limit
        self.period_sec = period_sec
        self._tokens = asyncio.Queue[None](maxsize=rate_limit)
        self._refill_task: asyncio.Task[None] | None = None

        # Pre-fill the bucket with initial tokens
        for _ in range(rate_limit):
            self._tokens.put_nowait(None)

    async def _refiller(self) -> None:
        """The background task that refills the token bucket periodically."""
        try:
            while True:
                await asyncio.sleep(self.period_sec)
                # Refill only the tokens that have been consumed.
                # This prevents the bucket from "saving up" tokens beyond its capacity
                # and handles cases where the refill interval isn't perfectly timed.
                num_to_add = self.rate_limit - self._tokens.qsize()
                for _ in range(num_to_add):
                    # put_nowait is safe because we know there is space.
                    self._tokens.put_nowait(None)
        except asyncio.CancelledError:
            # Allow the task to exit cleanly on cancellation.
            pass
        except Exception as e:
            # In a real application, you might want to log this.
            # For now, we'll just print it to stderr.
            print(f"FATAL: Rate limiter refill task failed: {e}")

    async def start(self) -> None:
        """Starts the background token refiller task."""
        if self._refill_task is None or self._refill_task.done():
            self._refill_task = asyncio.create_task(self._refiller())

    async def stop(self) -> None:
        """Stops the background token refiller task gracefully."""
        if self._refill_task and not self._refill_task.done():
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass
        self._refill_task = None

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[None, None]:
        """
        Acquires a token, waiting if necessary. Use as an async context manager.

        This is the primary method for using the rate limiter. It will block
        until a token is available.
        """
        # `get()` is cancellation-safe.
        await self._tokens.get()
        try:
            yield
        finally:
            # In a simple token bucket, the token is consumed, not returned.
            # If you needed a concurrency limiter (like a Semaphore),
            # you would return the token here.
            pass

    async def __aenter__(self) -> Self:
        """Starts the refiller task when entering the context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stops the refiller task when exiting the context."""
        await self.stop()


### `tests/unit/test_rate_limiter.py` — Tests for the RateLimiter utility.

```python
import asyncio
import time

import pytest

from cryptochart.utils.rate_limiter import AsyncRateLimiter


@pytest.mark.asyncio
async def test_initialization() -> None:
    """Tests that the limiter initializes with a full token bucket."""
    limiter = AsyncRateLimiter(10, 1)
    assert limiter.rate_limit == 10
    assert limiter.period_sec == 1
    assert limiter._tokens.full()
    assert limiter._tokens.qsize() == 10

    with pytest.raises(ValueError):
        AsyncRateLimiter(0, 1)
    with pytest.raises(ValueError):
        AsyncRateLimiter(10, -1)


@pytest.mark.asyncio
async def test_acquire_consumes_tokens() -> None:
    """Tests that acquiring tokens reduces the count in the bucket."""
    limiter = AsyncRateLimiter(5, 1)
    async with limiter:
        assert limiter._tokens.qsize() == 5
        async with limiter.acquire():
            assert limiter._tokens.qsize() == 4
        # acquire does not return the token, so it should still be 4
        assert limiter._tokens.qsize() == 4

        # Acquire all remaining tokens
        for _ in range(4):
            await limiter._tokens.get()

        assert limiter._tokens.empty()


@pytest.mark.asyncio
async def test_rate_limiting_blocks_when_exhausted() -> None:
    """Tests that the limiter blocks when all tokens are used."""
    limiter = AsyncRateLimiter(1, 0.2)
    async with limiter:
        # Acquire the single token
        async with limiter.acquire():
            pass  # Success

        # The next acquire should block. We test this with a timeout.
        try:
            await asyncio.wait_for(limiter.acquire().__aenter__(), timeout=0.05)
            pytest.fail("Rate limiter did not block when tokens were exhausted.")
        except asyncio.TimeoutError:
            pass  # Expected behavior


@pytest.mark.asyncio
async def test_refill_mechanism() -> None:
    """Tests that the token bucket is refilled after the period."""
    limiter = AsyncRateLimiter(2, 0.1)
    async with limiter:
        # Consume all tokens
        async with limiter.acquire():
            pass
        async with limiter.acquire():
            pass
        assert limiter._tokens.empty()

        # Wait for the refill period
        await asyncio.sleep(0.15)

        # The bucket should now be full again
        assert limiter._tokens.full()
        assert limiter._tokens.qsize() == 2

        # We should be able to acquire again
        async with limiter.acquire():
            pass


@pytest.mark.asyncio
async def test_timing_of_requests() -> None:
    """
    Tests the end-to-end timing to ensure requests are spread out correctly.
    """
    rate = 5
    period = 0.5
    limiter = AsyncRateLimiter(rate, period)
    num_requests = 12  # More than 2 periods worth

    start_time = time.monotonic()

    async with limiter:
        tasks = [
            asyncio.create_task(limiter.acquire().__aenter__())
            for _ in range(num_requests)
        ]
        await asyncio.gather(*tasks)

    end_time = time.monotonic()
    duration = end_time - start_time

    # Expected duration:
    # The first `rate` requests are immediate.
    # The next `rate` requests are available after `period` seconds.
    # The final `num_requests % rate` requests are available after `2 * period` seconds.
    # So, we need at least 2 full periods to process 12 requests (5, 5, 2).
    expected_minimum_duration = 2 * period
    # Allow for some scheduling overhead
    expected_maximum_duration = expected_minimum_duration + 0.2

    assert duration >= expected_minimum_duration
    assert duration <= expected_maximum_duration


@pytest.mark.asyncio
async def test_start_stop_behavior() -> None:
    """Tests manual start and stop of the refiller task."""
    limiter = AsyncRateLimiter(1, 0.1)
    # Do not use context manager here
    await limiter.start()
    assert limiter._refill_task is not None
    assert not limiter._refill_task.done()

    # Consume token
    async with limiter.acquire():
        pass
    assert limiter._tokens.empty()

    # Wait for refill
    await asyncio.sleep(0.15)
    assert limiter._tokens.full()

    await limiter.stop()
    assert limiter._refill_task is None or limiter._refill_task.done()


@pytest.mark.asyncio
async def test_cancellation_safety() -> None:
    """Tests that a waiting acquire call can be cancelled."""
    limiter = AsyncRateLimiter(1, 1)
    async with limiter:
        # Take the only token
        async with limiter.acquire():
            pass

        # This task will block waiting for a token
        blocked_task = asyncio.create_task(limiter.acquire().__aenter__())
        await asyncio.sleep(0.01)  # Give it time to start and block

        assert not blocked_task.done()
        blocked_task.cancel()

        try:
            await blocked_task
            pytest.fail("Cancelled task did not raise CancelledError")
        except asyncio.CancelledError:
            pass  # Expected behavior

        # Ensure the limiter is still in a good state
        assert limiter._tokens.qsize() == 0