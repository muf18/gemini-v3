# src/cryptochart/__init__.py
"""CryptoChart: A high-performance, real-time cryptocurrency charting client.

This package contains the core application logic, including exchange adapters,
data processing pipelines, and the user interface.

The application is designed around Python's asyncio for high-concurrency I/O,
with a decoupled architecture to ensure performance and maintainability.

Key sub-packages:
- `adapters`: Connectors for individual cryptocurrency exchanges.
- `ui`: The PyQt-based graphical user interface.
- `utils`: Shared utilities like ring buffers and rate limiters.
- `types`: Data models, including generated Protobuf messages.
"""

# The version is managed in pyproject.toml and is dynamically
# retrieved here using importlib.metadata. This is the modern
# way to handle package versioning and avoids duplicating the version string.
import importlib.metadata

try:
    __version__: str = importlib.metadata.version("cryptochart")
except importlib.metadata.PackageNotFoundError:
    # This fallback is useful for development environments where the package
    # might not be formally installed yet.
    __version__ = "0.0.0-dev"