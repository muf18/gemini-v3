import asyncio
import contextlib
import types
from collections.abc import AsyncGenerator
from typing import Any, Self

from loguru import logger


class AsyncRateLimiter:
    """An asynchronous rate limiter using the token bucket algorithm.

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

    def __init__(self, rate_limit: int, period_sec: float = 1.0) -> None:
        """Initializes the rate limiter.

        Args:
            rate_limit: The maximum number of requests allowed in a period.
            period_sec: The time period in seconds.
        """
        if not isinstance(rate_limit, int) or rate_limit <= 0:
            err_msg = "Rate limit must be a positive integer."
            raise ValueError(err_msg)
        if not isinstance(period_sec, int | float) or period_sec <= 0:
            err_msg = "Period must be a positive number."
            raise ValueError(err_msg)

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
            logger.error(f"FATAL: Rate limiter refill task failed: {e}")

    async def start(self) -> None:
        """Starts the background token refiller task."""
        if self._refill_task is None or self._refill_task.done():
            self._refill_task = asyncio.create_task(self._refiller())

    async def stop(self) -> None:
        """Stops the background token refiller task gracefully."""
        if self._refill_task and not self._refill_task.done():
            self._refill_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refill_task
        self._refill_task = None

    @contextlib.asynccontextmanager
    async def acquire(self) -> AsyncGenerator[None, None]:
        """Acquires a token, waiting if necessary. Use as an async context manager.

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

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Stops the refiller task when exiting the context."""
        await self.stop()