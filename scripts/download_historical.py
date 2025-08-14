#!/usr/bin/env python
r"""A command-line utility to download historical candlestick data.

This script is useful for offline analysis, backtesting, or pre-populating
a data store. It uses the application's adapter infrastructure to fetch data.

Usage:
    python scripts/download_historical.py <EXCHANGE> <SYMBOL> <TIMEFRAME> \
        <START_DATE> [END_DATE]

Example:
    python scripts/download_historical.py coinbase BTC/USD 1h 2023-01-01
"""