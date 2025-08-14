#!/usr/bin/env python
"""A standalone script to run the latency benchmark test.

This script provides an easy way to execute the latency integration test
without running the full pytest suite. It uses pytest's infrastructure
to discover and run the specific test, providing clear output.

This is useful for developers who want to quickly check the performance
impact of their changes on the core data processing pipeline.

Usage:
    python scripts/run_latency_bench.py
"""

import sys
from pathlib import Path

import pytest

# Add the project root to the Python path to allow imports from `src`
# This makes the script runnable from the repository root.
try:
    ROOT_DIR = Path(__file__).resolve().parent.parent
except NameError:
    ROOT_DIR = Path.cwd()

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# --- Configuration ---
# The path to the specific test file to run.
LATENCY_TEST_FILE = ROOT_DIR / "tests" / "integration" / "test_latency_budget.py"


def main() -> int:
    """Runs the latency benchmark using pytest.

    Returns:
        The exit code from the pytest run. 0 for success, non-zero for failure.
    """
    print("--- CryptoChart Latency Benchmark ---")
    print(f"Target test file: {LATENCY_TEST_FILE}")
    print(
        "This test will simulate a high volume of trades to measure pipeline "
        "performance."
    )
    print("Please wait, this may take a few seconds...")
    print("-" * 35)

    if not LATENCY_TEST_FILE.exists():
        print(f"Error: Test file not found at '{LATENCY_TEST_FILE}'.")
        print("Please ensure you are running this script from the repository root.")
        return 1

    # Pytest arguments:
    # -v: Verbose output
    # -s: Show print statements (and loguru output) during the test run
    # --log-cli-level=INFO: Set the log level for the test output
    # --disable-warnings: Suppress warnings that can clutter the output
    args = [
        str(LATENCY_TEST_FILE),
        "-v",
        "-s",
        "--log-cli-level=INFO",
        "--disable-warnings",
    ]

    try:
        exit_code = pytest.main(args)
    except Exception as e:
        print(f"\nAn unexpected error occurred while running pytest: {e}")
        return 1

    print("-" * 35)
    if exit_code == 0:
        print("Latency benchmark completed successfully.")
    else:
        print(f"Latency benchmark failed with exit code: {exit_code}")
        print("   Check the output above for details on the failure.")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())