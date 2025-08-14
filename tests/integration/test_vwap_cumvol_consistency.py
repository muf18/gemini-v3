import time
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from cryptochart.aggregator import SymbolAggregator
from cryptochart.types import models_pb2

# --- Synthetic Trade Data ---
# A deterministic stream of trades (timestamp_offset_s, price, size)
SYNTHETIC_TRADES = [
    (0, "100.0", "1.0"),
    (5, "101.5", "0.5"),
    (15, "101.0", "2.0"),
    (30, "102.0", "1.5"),
    (50, "101.8", "0.8"),
    # A trade after 60s that will push the first trade out of the 1m window
    (65, "103.0", "1.2"),
    # A trade after 70s that will push the second trade out
    (70, "104.0", "0.7"),
]


def pandas_vwap_calculator(
    trades: list[tuple[int, str, str]], window_seconds: int
) -> tuple[Decimal, Decimal]:
    """
    Calculates VWAP and cumulative volume for a series of trades using Pandas.
    This serves as the ground truth for our test.

    Args:
        trades: A list of (timestamp, price, size) tuples.
        window_seconds: The rolling window size in seconds.

    Returns:
        A tuple of (final_vwap, final_cumulative_volume).
    """
    if not trades:
        return Decimal("0"), Decimal("0")

    df = pd.DataFrame(trades, columns=["timestamp", "price", "size"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df["price"] = df["price"].astype(float)
    df["size"] = df["size"].astype(float)
    df = df.set_index("timestamp")

    df["price_volume"] = df["price"] * df["size"]

    # Perform rolling calculations
    rolling_sum_price_volume = df["price_volume"].rolling(
        f"{window_seconds}s"
    ).sum()
    rolling_sum_volume = df["size"].rolling(f"{window_seconds}s").sum()

    # Calculate VWAP, handling potential division by zero
    vwap = rolling_sum_price_volume / rolling_sum_volume
    vwap = vwap.fillna(0)

    final_vwap = Decimal(str(vwap.iloc[-1]))
    final_cum_vol = Decimal(str(rolling_sum_volume.iloc[-1]))

    return final_vwap, final_cum_vol


@pytest.mark.parametrize(
    "timeframe, window_seconds",
    [
        ("1m", 60),
        ("5m", 300),
    ],
)
def test_aggregator_consistency_with_pandas(
    timeframe: str, window_seconds: int
) -> None:
    """
    Tests that the SymbolAggregator's VWAP and cumulative volume match the
    output of a trusted Pandas-based calculation for a given timeframe.
    """
    # --- Setup ---
    symbol_aggregator = SymbolAggregator("BTC/USD")
    start_time = time.time()
    
    # Create a list of trades with absolute timestamps
    trades_with_abs_ts = [
        (int(start_time + offset), price, size)
        for offset, price, size in SYNTHETIC_TRADES
    ]

    # --- Execution ---
    final_agg_update = None
    for ts, price, size in trades_with_abs_ts:
        # Simulate time passing for the aggregator's internal clock
        time.time_ns = lambda: ts * 1_000_000_000

        trade_update = models_pb2.PriceUpdate(
            symbol="BTC/USD",
            venue="test",
            price=price,
            size=size,
            exchange_timestamp=datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        )
        
        aggregated_updates = symbol_aggregator.process_update(trade_update)
        
        # Find the update for the specific timeframe we are testing
        for update in aggregated_updates:
            if update.timeframe == timeframe:
                final_agg_update = update
                break

    assert final_agg_update is not None, f"No update found for timeframe {timeframe}"

    # --- Verification ---
    # 1. Calculate the expected result using the trusted Pandas calculator
    expected_vwap, expected_cum_vol = pandas_vwap_calculator(
        trades_with_abs_ts, window_seconds
    )

    # 2. Get the actual result from our aggregator
    actual_vwap = Decimal(final_agg_update.vwap)
    actual_cum_vol = Decimal(final_agg_update.cum_volume)

    # 3. Compare the results
    # We use a relative tolerance to account for potential minor floating point
    # differences between the pure Python Decimal math and NumPy/Pandas float math.
    # A tolerance of 1e-9 is very strict and suitable for financial calculations.
    assert float(actual_vwap) == pytest.approx(float(expected_vwap), rel=1e-9)
    assert float(actual_cum_vol) == pytest.approx(float(expected_cum_vol), rel=1e-9)

    logger.info(f"[{timeframe}] Consistency check PASSED.")
    logger.info(f"  Aggregator VWAP: {actual_vwap}, Pandas VWAP: {expected_vwap}")
    logger.info(f"  Aggregator CumVol: {actual_cum_vol}, Pandas CumVol: {expected_cum_vol}")