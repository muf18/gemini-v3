# CryptoChart: High-Performance Real-Time Charting Client

![CI/CD Pipeline](https://github.com/example/cryptochart/actions/workflows/ci.yml/badge.svg)

CryptoChart is a self-contained, cross-platform desktop and mobile application for real-time cryptocurrency price charting. It connects directly to multiple exchange WebSocket APIs to stream, process, and visualize trade data with a target p99 latency of under 300ms from data receipt to screen render.

The core mission of this project is to provide a high-performance, low-latency, and resource-efficient tool for financial data analysis without relying on any intermediary servers or paid data subscriptions.

---

## Core Features

*   **Multi-Exchange Aggregation**: Merges real-time trade data from multiple exchanges for a single trading pair into one unified view.
*   **Real-Time Analytics**: Calculates rolling Volume-Weighted Average Price (VWAP) and cumulative volume on-the-fly for multiple timeframes.
*   **Low-Latency Processing**: Engineered from the ground up using Python's `asyncio` framework and high-performance libraries like NumPy to meet a sub-300ms processing budget.
*   **High-Performance Charting**: Utilizes `PyQtGraph` for fast, hardware-accelerated rendering of candlestick, volume, and overlay data.
*   **Cross-Platform**: Designed to run on Windows, macOS, and Linux desktops, with a build configuration for Android.
*   **Resilient Networking**: Implements robust WebSocket reconnection logic with exponential backoff and jitter to handle network interruptions gracefully.
*   **Extensible Design**: A clean, adapter-based architecture makes it straightforward to add support for new exchanges.

## Supported Exchanges & Pairs

This application is configured to support the following pairs and exchanges out-of-the-box:

| Trading Pair | Supported Exchanges                               |
| :----------- | :------------------------------------------------ |
| **BTC/USD**  | Coinbase, Bitstamp, Kraken                        |
| **BTC/EUR**  | Kraken, Bitvavo                                   |
| **BTC/USDT** | Binance, OKX, Bitget, DigiFinex                   |

## Technical Architecture

CryptoChart is a single-process, asynchronous application built entirely within the Python ecosystem.

*   **Core Logic**: `asyncio` for all I/O-bound operations, ensuring a non-blocking architecture.
*   **Data Processing**: `NumPy` for vectorized, fixed-point calculations in the aggregation engine to ensure performance and precision.
*   **Data Serialization**: `Protobuf` for defining a canonical, type-safe internal data model, which decouples components and improves efficiency.
*   **Networking**: `websockets` for real-time streaming and `httpx` for asynchronous REST API calls.
*   **GUI Framework**: `PySide6` (the official Qt for Python project).
*   **Charting Engine**: `PyQtGraph`, chosen for its exceptional performance with real-time data and direct integration with NumPy.
*   **Cross-Platform Packaging**: `PyInstaller` for desktop and `Buildozer` for Android.

### Component Layers

1.  **Exchange Adapters**: Manage WebSocket/REST communication and normalization for a specific exchange.
2.  **Normalizer**: Enriches incoming data with client-side metadata.
3.  **Aggregator**: The core engine that merges data streams and computes VWAP/volume using rolling windows.
4.  **Publisher**: A fan-out service that distributes aggregated data to subscribers (like the UI).
5.  **UI Layer**: The PyQt-based frontend that visualizes the data.

## Getting Started (Running from Source)

Follow these instructions to run the application in a development environment.

### Prerequisites

*   Python 3.10 or newer.
*   A C++ compiler (required by some dependencies on certain platforms).

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/example/cryptochart.git
    cd cryptochart
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    # For Unix/macOS
    python3 -m venv venv
    source venv/bin/activate

    # For Windows
    python -m venv venv
    .\venv\Scripts\activate
    ```

3.  **Install dependencies:**
    Install the application in editable mode along with all development dependencies.
    ```bash
    pip install -e .[dev]
    ```

4.  **Compile Protobuf schemas:**
    This step generates the Python data models from the `.proto` files.
    ```bash
    python proto/compile_proto.py
    ```

5.  **Run the application:**
    You can now launch the GUI using the installed script entry point.
    ```bash
    cryptochart
    ```
    Alternatively, you can run the main module directly:
    ```bash
    python -m src.cryptochart.ui.main
    ```

## Building Executables

### Desktop (Windows, macOS)

`PyInstaller` is used to package the application into a single executable.

```bash
# First, ensure all dependencies and protobufs are up to date
pip install -e .[dev]
python proto/compile_proto.py

# Run PyInstaller
pyinstaller --noconfirm --onefile --windowed --name cryptochart src/cryptochart/ui/main.py