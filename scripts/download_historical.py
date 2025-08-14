#!/usr/bin/env python
"""A command-line utility to download historical candlestick data.

This script is useful for offline analysis, backtesting, or pre-populating
a data store. It uses the application's adapter infrastructure to fetch data.

Usage:
    python scripts/download_historical.py <EXCHANGE> <SYMBOL> <TIMEFRAME> \\
        <START_DATE> [END_DATE]

Example:
    python scripts/download_historical.py coinbase BTC/USD 1h 2023-01-01 2023-02-01
    python scripts/download_historical.py binance ETH/USDT 1d 2022-01-01
"""

import asyncio
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
from loguru import logger

# Add project root to path to allow imports from src
try:
    ROOT_DIR = Path(__file__).resolve().parent.parent
except NameError:
    ROOT_DIR = Path.cwd()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from cryptochart.adapters import (
    binance,
    bitget,
    bitstamp,
    bitvavo,
    coinbase,
    digifinex,
    kraken,
    okx,
)
from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.types import models_pb2

# --- Adapter Mapping ---
ADAPTER_MAP: dict[str, type[ExchangeAdapter]] = {
    "coinbase": coinbase.CoinbaseAdapter,
    "kraken": kraken.KrakenAdapter,
    "bitstamp": bitstamp.BitstampAdapter,
    "binance": binance.BinanceAdapter,
    "okx": okx.OKXAdapter,
    "bitget": bitget.BitgetAdapter,
    "digifinex": digifinex.DigifinexAdapter,
    "bitvavo": bitvavo.BitvavoAdapter,
}


def print_usage() -> None:
    """Prints the script's usage instructions."""
    usage_string = (
        "Usage: python download_historical.py <EXCHANGE> <SYMBOL> <TIMEFRAME> "
        "<START_DATE> [END_DATE]"
    )
    print(usage_string)
    print("\nArguments:")
    print("  EXCHANGE:   The exchange to download from.")
    print(f"              Available: {', '.join(ADAPTER_MAP.keys())}")
    print("  SYMBOL:     The trading pair in 'BASE/QUOTE' format (e.g., BTC/USD).")
    print("  TIMEFRAME:  The candle interval (e.g., 1m, 5m, 1h, 1d).")
    print("  START_DATE: The start date in YYYY-MM-DD format.")
    print("  END_DATE:   (Optional) The end date in YYYY-MM-DD format. Defaults to today.")
    print("\nExample:")
    print("  python scripts/download_historical.py coinbase BTC/USD 1h 2023-01-01 2023-02-01")


async def main() -> int:
    """Main async function to parse args, fetch data, and write to CSV."""
    if not 5 <= len(sys.argv) <= 6:
        print_usage()
        return 1

    try:
        exchange_name = sys.argv.lower()
        symbol = sys.argv.upper()
        timeframe = sys.argv.lower()
        start_date_str = sys.argv
        end_date_str = (
            sys.argv
            if len(sys.argv) == 6
            else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )

        # --- Argument Validation ---
        if exchange_name not in ADAPTER_MAP:
            logger.error(
                f"Invalid exchange '{exchange_name}'. "
                f"Available: {list(ADAPTER_MAP.keys())}"
            )
            return 1

        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if start_dt >= end_dt:
            logger.error("Start date must be before the end date.")
            return 1

    except (ValueError, IndexError) as e:
        logger.error(f"Invalid argument provided: {e}")
        print_usage()
        return 1

    # --- Data Fetching ---
    logger.info(f"Initializing adapter for '{exchange_name}'...")
    adapter_class = ADAPTER_MAP[exchange_name]
    # The adapter needs a dummy queue and http client for initialization
    adapter = adapter_class(
        symbols=[symbol], output_queue=asyncio.Queue(), http_client=httpx.AsyncClient()
    )

    logger.info(
        f"Fetching data for {symbol} ({timeframe}) from {start_date_str} to {end_date_str}..."
    )
    try:
        candles = await adapter.get_historical_candles(symbol, timeframe, start_dt, end_dt)
    except Exception as e:
        logger.error(f"An error occurred during data fetching: {e}")
        return 1
    finally:
        await adapter.http_client.aclose()

    if not candles:
        logger.warning(
            "No data was returned from the exchange for the specified range."
        )
        return 0

    logger.success(f"Successfully downloaded {len(candles)} candles.")

    # --- CSV Writing ---
    sanitized_symbol = symbol.replace("/", "-")
    filename = (
        f"{exchange_name}_{sanitized_symbol}_{timeframe}_"
        f"{start_date_str}_to_{end_date_str}.csv"
    )
    output_path = Path.cwd() / filename

    logger.info(f"Writing data to '{output_path}'...")
    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(models_pb2.Candle.DESCRIPTOR.fields_by_name.keys())
            # Write data rows
            for candle in candles:
                writer.writerow(
                    [
                        candle.symbol,
                        candle.timeframe,
                        candle.open_time,
                        candle.close_time,
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume,
                    ]
                )
        logger.success("File written successfully.")
    except OSError as e:
        logger.error(f"Failed to write to file: {e}")
        return 1

    return 0


if __name__ == "__main__":
    # Setup basic logging for the script
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Download cancelled by user.")
        sys.exit(1)