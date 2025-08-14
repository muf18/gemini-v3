# src/cryptochart/adapters/__init__.py
"""This package contains the exchange-specific adapters.

Each adapter is a self-contained module responsible for connecting to an
exchange's real-time data stream, normalizing the data into a canonical
format, and handling API-specific details like authentication, rate limiting,
and heartbeats.

All adapters inherit from the `ExchangeAdapter` abstract base class defined
in `cryptochart.adapters.base`.
"""