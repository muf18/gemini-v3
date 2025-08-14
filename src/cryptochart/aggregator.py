import asyncio
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final

from loguru import logger

from cryptochart.types import models_pb2

# --- Constants ---

# Use a fixed-point precision of 10^8. Prices and sizes will be stored
# internally as 64-bit integers scaled by this factor.
FIXED_POINT_SCALE: Final[int] = 10**8

# Supported timeframes and their duration in seconds.
TIMEFRAME_SECONDS: Final[dict[str, int]] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
}


@dataclass
class TimeframeState:
    """Holds the state for a single timeframe's aggregation."""

    # A deque of (timestamp_ns, price_int, size_int) tuples.
    buffer: "deque[tuple[int, int, int]]"
    # Sum of (price * size) for all items in the buffer.
    total_price_volume: int = 0
    # Sum of size for all items in the buffer.
    total_volume: int = 0


class SymbolAggregator:
    """Manages real-time aggregation for a single trading symbol (e.g., BTC/USD).

    This class maintains separate rolling windows for each supported timeframe,
    calculating VWAP and cumulative volume efficiently as new trades arrive.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.timeframes: dict[str, TimeframeState] = {
            tf: TimeframeState(buffer=deque()) for tf in TIMEFRAME_SECONDS
        }
        logger.info(f"[{symbol}] New symbol aggregator initialized.")

    def process_update(
        self, update: models_pb2.PriceUpdate
    ) -> list[models_pb2.PriceUpdate]:
        """Processes a new trade update and returns a list of aggregated updates.

        Args:
            update: The normalized PriceUpdate from the Normalizer.

        Returns:
            A list of PriceUpdate messages, one for each timeframe, with
            VWAP and cumulative volume fields populated.
        """
        try:
            price_int = int(Decimal(update.price) * FIXED_POINT_SCALE)
            size_int = int(Decimal(update.size) * FIXED_POINT_SCALE)
        except (InvalidOperation, TypeError) as e:
            logger.warning(
                f"[{self.symbol}] Could not parse price/size. "
                f"Price: '{update.price}', Size: '{update.size}'. Error: {e}"
            )
            return []

        if size_int <= 0:
            return []  # Ignore trades with no or negative volume

        # Use a high-resolution timestamp for window calculations
        current_time_ns = time.time_ns()
        aggregated_updates = []

        for tf_str, tf_state in self.timeframes.items():
            window_ns = TIMEFRAME_SECONDS[tf_str] * 1_000_000_000

            # 1. Add new trade to the buffer and update totals
            trade_tuple = (current_time_ns, price_int, size_int)
            tf_state.buffer.append(trade_tuple)
            tf_state.total_price_volume += price_int * size_int
            tf_state.total_volume += size_int

            # 2. Purge old trades from the left of the buffer
            while tf_state.buffer and (
                current_time_ns - tf_state.buffer[0][0] > window_ns
            ):
                _old_ts, old_price, old_size = tf_state.buffer.popleft()
                tf_state.total_price_volume -= old_price * old_size
                tf_state.total_volume -= old_size

            # 3. Calculate VWAP and create the aggregated update message
            if tf_state.total_volume > 0:
                vwap_int = tf_state.total_price_volume // tf_state.total_volume
                vwap_str = str(Decimal(vwap_int) / FIXED_POINT_SCALE)
                cum_vol_str = str(Decimal(tf_state.total_volume) / FIXED_POINT_SCALE)

                agg_update = models_pb2.PriceUpdate()
                agg_update.CopyFrom(update)  # Preserve original trade info
                agg_update.timeframe = tf_str
                agg_update.vwap = vwap_str
                agg_update.cum_volume = cum_vol_str
                aggregated_updates.append(agg_update)

        return aggregated_updates


class Aggregator:
    """The main aggregation engine.

    It receives normalized updates, dispatches them to the appropriate
    SymbolAggregator, and forwards the results to a publisher.
    """

    def __init__(
        self,
        input_queue: "asyncio.Queue[models_pb2.PriceUpdate]",
        output_queue: "asyncio.Queue[models_pb2.PriceUpdate]",
    ) -> None:
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.symbol_aggregators: dict[str, SymbolAggregator] = {}
        self._task: asyncio.Task[None] | None = None
        self._running = asyncio.Event()

    def start(self) -> None:
        """Starts the aggregator's processing loop."""
        if self._task is None or self._task.done():
            self._running.set()
            self._task = asyncio.create_task(self._run())
            logger.info("Aggregator started.")
        else:
            logger.warning("Aggregator is already running.")

    async def stop(self) -> None:
        """Stops the aggregator's processing loop."""
        if not self._running.is_set():
            logger.warning("Aggregator is not running.")
            return

        logger.info("Stopping Aggregator...")
        self._running.clear()
        if self._task:
            try:
                self.input_queue.put_nowait(None)  # type: ignore[arg-type]
                self._task.cancel()
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None
        logger.info("Aggregator stopped.")

    async def _run(self) -> None:
        """The main processing loop."""
        while self._running.is_set():
            try:
                update = await self.input_queue.get()
                if update is None:  # Sentinel for shutdown
                    continue

                symbol = update.symbol
                if not symbol:
                    continue

                if symbol not in self.symbol_aggregators:
                    self.symbol_aggregators[symbol] = SymbolAggregator(symbol)

                symbol_agg = self.symbol_aggregators[symbol]
                aggregated_updates = symbol_agg.process_update(update)

                for agg_update in aggregated_updates:
                    await self.output_queue.put(agg_update)

                self.input_queue.task_done()

            except asyncio.CancelledError:
                logger.info("Aggregator run loop cancelled.")
                break
            except Exception:
                logger.exception("Unexpected error in Aggregator run loop.")
                await asyncio.sleep(1)
        logger.info("Aggregator run loop terminated.")