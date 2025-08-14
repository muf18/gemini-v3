import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any, Final

import numpy as np
import pytest
from loguru import logger

from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.aggregator import Aggregator
from cryptochart.normalizer import Normalizer
from cryptochart.publisher import Publisher
from cryptochart.types import models_pb2
from cryptochart.utils.time import get_current_rfc3339_timestamp

# --- Test Configuration ---
TARGET_P99_LATENCY_MS: Final[int] = 300
NUM_TRADES_TO_SEND: Final[int] = 5000
SEND_RATE_PER_SECOND: Final[int] = 1000  # Simulate a high-volume scenario


class LatencyTestAdapter(ExchangeAdapter):
    """A mock adapter designed specifically for the latency test."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._stream_queue = asyncio.Queue()

    @property
    def venue_name(self) -> str:
        return "latency-test-venue"

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        while True:
            message = await self._stream_queue.get()
            if message is None:  # Sentinel for stopping
                break
            yield message

    def _normalize_message(self, message: dict[str, Any]) -> models_pb2.PriceUpdate | None:
        # The message is already a PriceUpdate object in this test
        return message

    async def get_historical_candles(self, *args: Any, **kwargs: Any) -> list[models_pb2.Candle]:
        return []

    async def send_trade(self, trade: models_pb2.PriceUpdate) -> None:
        await self._stream_queue.put(trade)

    async def finish(self) -> None:
        await self._stream_queue.put(None)


@pytest.mark.asyncio
@pytest.mark.timeout(30)  # Set a generous timeout for the entire test
async def test_end_to_end_latency_under_load() -> None:
    """
    Measures the p99 latency from adapter output to publisher output
    under a synthetic load of trades.
    """
    # --- Setup ---
    # 1. Create the full data pipeline
    adapter_q = asyncio.Queue()
    aggregator_q = asyncio.Queue()
    publisher_q = asyncio.Queue()
    output_q = asyncio.Queue()  # The final destination for our measurements

    normalizer = Normalizer(adapter_q, aggregator_q)
    aggregator = Aggregator(aggregator_q, publisher_q)
    publisher = Publisher(publisher_q)

    # 2. Create and start the test adapter
    adapter = LatencyTestAdapter(symbols=["BTC/USD"], output_queue=adapter_q, http_client=None) # type: ignore

    # 3. Start all pipeline components
    normalizer.start()
    aggregator.start()
    publisher.start()
    adapter.start()

    # 4. Subscribe the final output queue to the publisher
    await publisher.subscribe(symbol="BTC/USD", timeframe="1m", queue=output_q)

    latencies_ms = []

    # --- Execution ---
    # 1. Define the receiver task that will consume final messages and measure latency
    async def receiver() -> None:
        num_received = 0
        while num_received < NUM_TRADES_TO_SEND:
            final_update = await output_q.get()
            
            # The normalizer adds this timestamp upon receipt from the adapter queue
            received_time_ns = time.time_ns()
            
            # The original timestamp was stored in the 'side' field by the sender
            sent_time_ns = int(final_update.side)
            
            latency_ns = received_time_ns - sent_time_ns
            latencies_ms.append(latency_ns / 1_000_000)
            
            num_received += 1
            output_q.task_done()

    # 2. Define the sender task that generates the load
    async def sender() -> None:
        send_interval_s = 1.0 / SEND_RATE_PER_SECOND
        for i in range(NUM_TRADES_TO_SEND):
            # We hijack the 'side' field to carry the send timestamp (in nanoseconds)
            # This avoids modifying the protobuf schema just for a test.
            sent_time_ns = time.time_ns()
            trade = models_pb2.PriceUpdate(
                symbol="BTC/USD",
                price="50000.0",
                size="0.01",
                sequence_number=i,
                side=str(sent_time_ns),
                exchange_timestamp=get_current_rfc3339_timestamp(),
            )
            await adapter.send_trade(trade)
            await asyncio.sleep(send_interval_s)
        await adapter.finish()

    # 3. Run sender and receiver concurrently
    receiver_task = asyncio.create_task(receiver())
    sender_task = asyncio.create_task(sender())

    await asyncio.gather(sender_task, receiver_task)

    # --- Verification ---
    # 1. Teardown the pipeline
    await adapter.stop()
    await aggregator.stop()
    await normalizer.stop()
    await publisher.stop()

    # 2. Analyze the results
    assert len(latencies_ms) == NUM_TRADES_TO_SEND, "Did not receive all messages"

    p99_latency = np.percentile(latencies_ms, 99)
    avg_latency = np.mean(latencies_ms)
    max_latency = np.max(latencies_ms)
    min_latency = np.min(latencies_ms)

    logger.info("--- Latency Test Results ---")
    logger.info(f"Trades Processed: {len(latencies_ms)}")
    logger.info(f"Average Latency:  {avg_latency:.4f} ms")
    logger.info(f"Min Latency:      {min_latency:.4f} ms")
    logger.info(f"Max Latency:      {max_latency:.4f} ms")
    logger.info(f"p99 Latency:      {p99_latency:.4f} ms")
    logger.info("----------------------------")

    # 3. Assert that the p99 latency is within the budget
    assert (
        p99_latency <= TARGET_P99_LATENCY_MS
    ), f"p99 latency ({p99_latency:.2f}ms) exceeds target budget of {TARGET_P99_LATENCY_MS}ms"