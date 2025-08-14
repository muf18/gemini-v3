import asyncio
from decimal import Decimal

import pytest

from cryptochart.aggregator import (
    TIMEFRAME_SECONDS,
    Aggregator,
    SymbolAggregator,
)
from cryptochart.types import models_pb2


def create_trade(symbol: str, price: str, size: str) -> models_pb2.PriceUpdate:
    """Helper to create a PriceUpdate message."""
    return models_pb2.PriceUpdate(
        symbol=symbol,
        venue="testex",
        price=price,
        size=size,
        exchange_timestamp="2023-10-27T12:00:00Z",
    )


@pytest.fixture
def symbol_aggregator() -> SymbolAggregator:
    """Provides a SymbolAggregator for BTC/USD."""
    return SymbolAggregator("BTC/USD")


def test_symbol_aggregator_initialization(symbol_aggregator: SymbolAggregator) -> None:
    """Tests that the SymbolAggregator is initialized correctly."""
    assert symbol_aggregator.symbol == "BTC/USD"
    assert len(symbol_aggregator.timeframes) == len(TIMEFRAME_SECONDS)
    assert "1m" in symbol_aggregator.timeframes
    assert "1w" in symbol_aggregator.timeframes


def test_basic_vwap_calculation(symbol_aggregator: SymbolAggregator) -> None:
    """Tests a simple VWAP and cumulative volume calculation."""
    trade1 = create_trade("BTC/USD", "100", "2")  # Total value: 200
    trade2 = create_trade("BTC/USD", "110", "3")  # Total value: 330

    symbol_aggregator.process_update(trade1)
    aggregated_updates = symbol_aggregator.process_update(trade2)

    # Total value = 530, Total volume = 5. VWAP = 106.
    expected_vwap = "106.0"
    expected_cum_vol = "5.0"

    # Check the 1m timeframe result
    update_1m = next(u for u in aggregated_updates if u.timeframe == "1m")
    assert update_1m.vwap == expected_vwap
    assert update_1m.cum_volume == expected_cum_vol
    # Check another timeframe to ensure they are all updated
    update_1h = next(u for u in aggregated_updates if u.timeframe == "1h")
    assert update_1h.vwap == expected_vwap
    assert update_1h.cum_volume == expected_cum_vol


def test_fixed_point_precision(symbol_aggregator: SymbolAggregator) -> None:
    """Tests calculations with fractional numbers using fixed-point math."""
    trade = create_trade("BTC/USD", "100.12345678", "0.5")
    updates = symbol_aggregator.process_update(trade)

    update_1m = next(u for u in updates if u.timeframe == "1m")

    # VWAP of a single trade is its price.
    # We check against the original string, but rounded to the scale's precision.
    expected_price = str(Decimal("100.12345678"))
    assert update_1m.vwap == expected_price
    assert update_1m.cum_volume == "0.5"


@pytest.mark.asyncio
async def test_rolling_window_purges_old_trades(
    symbol_aggregator: SymbolAggregator, mocker
) -> None:
    """Tests that old trades are correctly purged from the window."""
    # Use a short timeframe for testing
    original_1m = TIMEFRAME_SECONDS["1m"]
    TIMEFRAME_SECONDS["1m"] = 1  # Temporarily set to 1 second

    try:
        # 1. First trade
        mocker.patch("time.time_ns", return_value=int(time.time() * 1e9))
        trade1 = create_trade("BTC/USD", "100", "1")
        updates1 = symbol_aggregator.process_update(trade1)
        update1_1m = next(u for u in updates1 if u.timeframe == "1m")
        assert update1_1m.vwap == "100.0"
        assert update1_1m.cum_volume == "1.0"

        # 2. Wait for the window to pass
        await asyncio.sleep(1.1)
        mocker.patch("time.time_ns", return_value=int((time.time() + 1.1) * 1e9))

        # 3. Second trade
        trade2 = create_trade("BTC/USD", "200", "2")
        updates2 = symbol_aggregator.process_update(trade2)
        update2_1m = next(u for u in updates2 if u.timeframe == "1m")

        # The first trade should be purged. VWAP should be based only on the second.
        assert update2_1m.vwap == "200.0"
        assert update2_1m.cum_volume == "2.0"

    finally:
        # Restore the original value
        TIMEFRAME_SECONDS["1m"] = original_1m


@pytest.mark.asyncio
async def test_aggregator_main_loop() -> None:
    """Tests the main Aggregator class dispatching logic."""
    in_q = asyncio.Queue()
    out_q = asyncio.Queue()
    aggregator = Aggregator(in_q, out_q)

    aggregator.start()

    # Send trades for two different symbols
    await in_q.put(create_trade("BTC/USD", "100", "1"))
    await in_q.put(create_trade("ETH/USD", "50", "10"))

    # Expect updates for all timeframes for both symbols
    num_timeframes = len(TIMEFRAME_SECONDS)
    results_btc = []
    results_eth = []

    for _ in range(num_timeframes * 2):
        update = await asyncio.wait_for(out_q.get(), timeout=1)
        if update.symbol == "BTC/USD":
            results_btc.append(update)
        elif update.symbol == "ETH/USD":
            results_eth.append(update)

    await aggregator.stop()

    assert len(aggregator.symbol_aggregators) == 2
    assert "BTC/USD" in aggregator.symbol_aggregators
    assert "ETH/USD" in aggregator.symbol_aggregators

    assert len(results_btc) == num_timeframes
    assert results_btc.vwap == "100.0"
    assert results_btc.cum_volume == "1.0"

    assert len(results_eth) == num_timeframes
    assert results_eth.vwap == "50.0"
    assert results_eth.cum_volume == "10.0"