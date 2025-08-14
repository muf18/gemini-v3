import abc
import asyncio
import random
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import httpx
import websockets
from loguru import logger

from cryptochart.types import models_pb2

# --- Constants for Reconnection Logic ---
INITIAL_RECONNECT_DELAY_S = 1.0
MAX_RECONNECT_DELAY_S = 60.0
RECONNECT_BACKOFF_FACTOR = 2.0
JITTER_FACTOR = 0.2  # 20% jitter


class ExchangeAdapter(abc.ABC):
    """
    An abstract base class for all exchange adapters.

    This class defines the common interface and provides a robust run loop
    with built-in reconnection logic (exponential backoff with jitter) for
    handling network interruptions.

    Subclasses are responsible for implementing the exchange-specific details,
    such as WebSocket endpoints, subscription messages, and data normalization.
    """

    def __init__(
        self,
        symbols: list[str],
        output_queue: asyncio.Queue[models_pb2.PriceUpdate],
        http_client: httpx.AsyncClient,
    ):
        """
        Initializes the adapter.

        Args:
            symbols: A list of trading pair symbols to subscribe to (e.g., ["BTC/USD"]).
            output_queue: An asyncio queue to push normalized PriceUpdate messages into.
            http_client: A shared httpx.AsyncClient for making REST API calls.
        """
        self.symbols = symbols
        self.output_queue = output_queue
        self.http_client = http_client
        self._running = asyncio.Event()
        self._main_task: asyncio.Task[None] | None = None

    @property
    @abc.abstractmethod
    def venue_name(self) -> str:
        """A unique, lowercase identifier for the exchange (e.g., 'coinbase')."""
        raise NotImplementedError

    def start(self) -> None:
        """Starts the adapter's main execution loop in a background task."""
        if self._main_task is None or self._main_task.done():
            self._running.set()
            self._main_task = asyncio.create_task(self._run_with_reconnect())
            logger.info(f"[{self.venue_name}] Adapter started for symbols: {self.symbols}")
        else:
            logger.warning(f"[{self.venue_name}] Adapter is already running.")

    async def stop(self) -> None:
        """Stops the adapter's main execution loop gracefully."""
        if not self._running.is_set():
            logger.warning(f"[{self.venue_name}] Adapter is not running.")
            return

        logger.info(f"[{self.venue_name}] Stopping adapter...")
        self._running.clear()
        if self._main_task:
            try:
                # The run loop should see the flag and exit, but we cancel
                # as a fallback to ensure termination.
                self._main_task.cancel()
                await self._main_task
            except asyncio.CancelledError:
                pass  # Expected cancellation.
            finally:
                self._main_task = None
        logger.info(f"[{self.venue_name}] Adapter stopped.")

    async def _run_with_reconnect(self) -> None:
        """The main run loop that handles connections and reconnections."""
        delay = INITIAL_RECONNECT_DELAY_S
        while self._running.is_set():
            try:
                logger.info(f"[{self.venue_name}] Connecting...")
                async for message in self._stream_messages():
                    if not self._running.is_set():
                        break
                    normalized_update = self._normalize_message(message)
                    if normalized_update:
                        await self.output_queue.put(normalized_update)

                if not self._running.is_set():
                    break
                delay = INITIAL_RECONNECT_DELAY_S  # Reset delay after a clean disconnect.
                logger.info(f"[{self.venue_name}] Stream ended. Reconnecting...")

            except (
                websockets.exceptions.ConnectionClosed,
                asyncio.TimeoutError,
                OSError,
            ) as e:
                logger.warning(f"[{self.venue_name}] Connection lost: {type(e).__name__}. Reconnecting...")
            except Exception:
                logger.exception(f"[{self.venue_name}] Unexpected error in run loop. Reconnecting...")

            if self._running.is_set():
                jitter = delay * JITTER_FACTOR * (random.random() * 2 - 1)
                sleep_duration = min(MAX_RECONNECT_DELAY_S, abs(delay + jitter))
                logger.info(f"[{self.venue_name}] Reconnecting in {sleep_duration:.2f} seconds.")
                try:
                    await asyncio.sleep(sleep_duration)
                except asyncio.CancelledError:
                    break  # Exit if cancelled during sleep.
                delay = min(MAX_RECONNECT_DELAY_S, delay * RECONNECT_BACKOFF_FACTOR)

        logger.info(f"[{self.venue_name}] Run loop has terminated.")

    @abc.abstractmethod
    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """
        Connects to the WebSocket, handles subscriptions, and yields raw messages.

        This is the core async generator that should be implemented by each subclass.
        It should handle the entire lifecycle of a single connection. If the
        connection is lost, this generator should exit (e.g., by raising an
        exception or simply returning), allowing the `_run_with_reconnect`
        loop to handle the reconnection logic.
        """
        # This is an abstract method; the yield makes it a generator.
        if False:  # pragma: no cover
            yield {}

    @abc.abstractmethod
    def _normalize_message(self, message: dict[str, Any]) -> models_pb2.PriceUpdate | None:
        """
        Normalizes a raw, exchange-specific message into the canonical PriceUpdate model.

        Args:
            message: The raw message dictionary received from the WebSocket.

        Returns:
            A populated PriceUpdate protobuf message, or None if the message
            is not a trade update (e.g., a heartbeat or subscription confirmation).
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_historical_candles(
        self, symbol: str, timeframe: str, start_dt: datetime, end_dt: datetime
    ) -> list[models_pb2.Candle]:
        """
        Fetches historical OHLCV data from the exchange's REST API.

        Args:
            symbol: The trading pair symbol.
            timeframe: The timeframe identifier (e.g., '1m', '1h', '1d').
            start_dt: The start datetime for the data range (UTC).
            end_dt: The end datetime for the data range (UTC).

        Returns:
            A list of populated Candle protobuf messages.
        """
        raise NotImplementedError```

### `tests/unit/test_adapters_base.py` — Tests for the base adapter logic and contracts.

```python
import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import httpx
import pytest
import websockets
from pytest_mock import MockerFixture

from cryptochart.adapters.base import (
    INITIAL_RECONNECT_DELAY_S,
    RECONNECT_BACKOFF_FACTOR,
    ExchangeAdapter,
)
from cryptochart.types import models_pb2


class MockExchangeAdapter(ExchangeAdapter):
    """A concrete implementation of ExchangeAdapter for testing purposes."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.connection_attempts = 0
        self.stream_generator: AsyncGenerator[dict[str, Any], None] = self._default_stream_generator()

    @property
    def venue_name(self) -> str:
        return "mockex"

    def set_stream_generator(self, generator: AsyncGenerator[dict[str, Any], None]) -> None:
        self.stream_generator = generator

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        self.connection_attempts += 1
        async for message in self.stream_generator:
            yield message

    def _normalize_message(self, message: dict[str, Any]) -> models_pb2.PriceUpdate | None:
        if message.get("type") == "trade":
            return models_pb2.PriceUpdate(symbol=message["symbol"], price=str(message["price"]))
        return None

    async def get_historical_candles(
        self, symbol: str, timeframe: str, start_dt: datetime, end_dt: datetime
    ) -> list[models_pb2.Candle]:
        return []

    async def _default_stream_generator(self) -> AsyncGenerator[dict[str, Any], None]:
        yield {"type": "trade", "symbol": "BTC/USD", "price": 50000}
        yield {"type": "heartbeat"}
        yield {"type": "trade", "symbol": "ETH/USD", "price": 4000}


@pytest.fixture
def mock_adapter() -> MockExchangeAdapter:
    """Provides a fresh MockExchangeAdapter instance for each test."""
    return MockExchangeAdapter(
        symbols=["BTC/USD", "ETH/USD"],
        output_queue=asyncio.Queue(),
        http_client=httpx.AsyncClient(),
    )


@pytest.mark.asyncio
async def test_start_stop_lifecycle(mock_adapter: MockExchangeAdapter) -> None:
    """Tests the basic start and stop functionality."""
    assert mock_adapter._main_task is None
    mock_adapter.start()
    assert isinstance(mock_adapter._main_task, asyncio.Task)
    assert not mock_adapter._main_task.done()

    await mock_adapter.stop()
    assert mock_adapter._main_task is None or mock_adapter._main_task.done()
    assert not mock_adapter._running.is_set()


@pytest.mark.asyncio
async def test_successful_stream_and_processing(mock_adapter: MockExchangeAdapter) -> None:
    """Tests a normal run where messages are received and put on the queue."""
    mock_adapter.start()

    update1 = await asyncio.wait_for(mock_adapter.output_queue.get(), timeout=1)
    update2 = await asyncio.wait_for(mock_adapter.output_queue.get(), timeout=1)

    assert mock_adapter.output_queue.empty()
    assert update1.symbol == "BTC/USD" and update1.price == "50000"
    assert update2.symbol == "ETH/USD" and update2.price == "4000"

    await mock_adapter.stop()


@pytest.mark.asyncio
async def test_reconnect_on_connection_closed(
    mock_adapter: MockExchangeAdapter, mocker: MockerFixture
) -> None:
    """Tests that the adapter tries to reconnect after a connection error."""
    sleep_spy = mocker.patch("asyncio.sleep", return_value=None)

    async def error_generator() -> AsyncGenerator[dict[str, Any], None]:
        yield {"type": "trade", "symbol": "BTC/USD", "price": 1}
        raise websockets.exceptions.ConnectionClosedError(None, None)

    mock_adapter.set_stream_generator(error_generator())
    mock_adapter.start()

    await mock_adapter.output_queue.get()
    await asyncio.sleep(0.01)  # Allow loop to process exception and call sleep

    assert mock_adapter.connection_attempts == 1
    sleep_spy.assert_called_once()
    assert sleep_spy.call_args[0][0] > 0

    await mock_adapter.stop()


@pytest.mark.asyncio
async def test_reconnect_exponential_backoff(
    mock_adapter: MockExchangeAdapter, mocker: MockerFixture
) -> None:
    """Tests that the reconnect delay increases exponentially."""
    sleep_spy = mocker.patch("asyncio.sleep", return_value=None)

    async def always_fail_generator() -> AsyncGenerator[dict[str, Any], None]:
        raise websockets.exceptions.ConnectionClosedError(None, None)
        yield  # Make it a generator

    mock_adapter.set_stream_generator(always_fail_generator())
    mock_adapter.start()

    await asyncio.sleep(0.01)
    assert mock_adapter.connection_attempts == 1
    sleep_spy.assert_called_once()
    first_delay = sleep_spy.call_args[0][0]
    assert first_delay == pytest.approx(INITIAL_RECONNECT_DELAY_S, abs=INITIAL_RECONNECT_DELAY_S * 0.2)

    await asyncio.sleep(0.01)
    assert mock_adapter.connection_attempts == 2
    assert sleep_spy.call_count == 2
    second_delay = sleep_spy.call_args[0][0]
    expected_second_delay = INITIAL_RECONNECT_DELAY_S * RECONNECT_BACKOFF_FACTOR
    assert second_delay == pytest.approx(expected_second_delay, abs=expected_second_delay * 0.2)

    await mock_adapter.stop()


@pytest.mark.asyncio
async def test_stop_during_reconnect_sleep(
    mock_adapter: MockExchangeAdapter, mocker: MockerFixture
) -> None:
    """Tests that the adapter can be stopped while waiting to reconnect."""
    sleep_future = asyncio.Future()
    mocker.patch("asyncio.sleep", side_effect=lambda delay: sleep_future)

    async def fail_once_generator() -> AsyncGenerator[dict[str, Any], None]:
        raise websockets.exceptions.ConnectionClosedError(None, None)
        yield

    mock_adapter.set_stream_generator(fail_once_generator())
    mock_adapter.start()

    await asyncio.sleep(0.01)
    assert mock_adapter.connection_attempts == 1

    await mock_adapter.stop()

    assert mock_adapter._main_task is None or mock_adapter._main_task.done()
    assert sleep_future.cancelled()