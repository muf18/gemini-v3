import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.aggregator import Aggregator
from cryptochart.normalizer import Normalizer
from cryptochart.types import models_pb2


class ControllableMockAdapter(ExchangeAdapter):
    """
    A mock adapter that allows programmatic control over its message stream.

    It also controls the connection state for testing resilience.
    """

    def __init__(self, venue: str, *args: Any, **kwargs: Any) -> None:
        """Initializes the controllable mock adapter."""
        super().__init__(*args, **kwargs)
        self._venue = venue
        self._stream_queue = asyncio.Queue()
        self._is_connected = True

    @property
    def venue_name(self) -> str:
        """Returns the mock venue name."""
        return self._venue

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        while self._is_connected:
            try:
                message = await self._stream_queue.get()
                yield message
                self._stream_queue.task_done()
            except asyncio.CancelledError:
                break
        # Simulate a connection loss by exiting the generator
        err_msg = f"Connection lost for {self.venue_name}"
        raise ConnectionAbortedError(err_msg)

    def _normalize_message(
        self, message: dict[str, Any]
    ) -> models_pb2.PriceUpdate | None:
        return models_pb2.PriceUpdate(
            symbol="BTC/USD",
            venue=self.venue_name,
            price=str(message["price"]),
            size=str(message["size"]),
        )

    async def get_historical_candles(
        self, *args: Any, **kwargs: Any
    ) -> list[models_pb2.Candle]:
        """Returns an empty list for historical data."""
        return []

    # --- Control methods for the test ---
    async def send_trade(self, price: float, size: float) -> None:
        """Pushes a trade message into the adapter's stream."""
        await self._stream_queue.put({"price": price, "size": size})

    def disconnect(self) -> None:
        """Simulates a disconnection."""
        self._is_connected = False
        # Put a sentinel value to unblock the queue.get() if it's waiting
        self._stream_queue.put_nowait(None)


@pytest.mark.asyncio
async def test_aggregator_resilience_to_adapter_disconnection() -> None:
    """
    Tests that the aggregator continues to process data from remaining adapters.

    This is tested for a scenario where one of them disconnects.
    """
    # --- Setup ---
    # Create the data pipeline components
    adapter_q = asyncio.Queue()
    aggregator_q = asyncio.Queue()
    output_q = asyncio.Queue()

    normalizer = Normalizer(adapter_q, aggregator_q)
    aggregator = Aggregator(aggregator_q, output_q)

    # Create two controllable mock adapters
    adapter1 = ControllableMockAdapter(
        venue="venue1", symbols=["BTC/USD"], output_queue=adapter_q, http_client=None
    )  # type: ignore
    adapter2 = ControllableMockAdapter(
        venue="venue2", symbols=["BTC/USD"], output_queue=adapter_q, http_client=None
    )  # type: ignore

    # Start all components
    normalizer.start()
    aggregator.start()
    adapter1.start()
    adapter2.start()

    # --- Execution & Verification ---
    # 1. Verify that trades from both adapters are processed initially.
    await adapter1.send_trade(price=100, size=1)
    update1 = await asyncio.wait_for(output_q.get(), timeout=1)
    assert update1.venue == "venue1"
    assert update1.vwap == "100.0"

    await adapter2.send_trade(price=102, size=1)
    update2 = await asyncio.wait_for(output_q.get(), timeout=1)
    assert update2.venue == "venue2"
    assert update2.vwap == "101.0"  # VWAP of (100*1 + 102*1) / (1+1)

    # 2. Simulate a disconnection of the first adapter.
    adapter1.disconnect()
    # The adapter's run loop will catch the exception and try to reconnect,
    # but it won't be able to send more messages for now.

    # 3. Verify that the system remains stable and can still process trades
    #    from the second, still-connected adapter.
    await adapter2.send_trade(price=104, size=2)
    update3 = await asyncio.wait_for(output_q.get(), timeout=1)
    assert update3.venue == "venue2"
    # The window now contains the trade from adapter2 (102, 1) and this new
    # trade (104, 2).
    # The trade from the disconnected adapter1 (100, 1) is still in the window.
    # VWAP = (100*1 + 102*1 + 104*2) / (1+1+2) = 410 / 4 = 102.5
    assert update3.vwap == "102.5"

    # 4. Ensure no unexpected messages are in the queue and the pipeline is healthy.
    assert output_q.empty()

    # --- Teardown ---
    await adapter1.stop()
    await adapter2.stop()
    await aggregator.stop()
    await normalizer.stop()