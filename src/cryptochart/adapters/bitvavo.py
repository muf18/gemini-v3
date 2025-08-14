import json
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import websockets
from loguru import logger

from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.types import models_pb2
from cryptochart.utils.time import normalize_timestamp_to_rfc3339


class BitvavoAdapter(ExchangeAdapter):
    """Adapter for connecting to the Bitvavo WebSocket and REST APIs."""

    _BASE_WSS_URL: str = "wss://ws.bitvavo.com/v2/"
    _BASE_API_URL: str = "https://api.bitvavo.com/v2"

    @property
    def venue_name(self) -> str:
        """Returns the unique, lowercase identifier for the exchange."""
        return "bitvavo"

    def _normalize_symbol_to_venue(self, symbol: str) -> str:
        """Converts a 'BASE/QUOTE' symbol to Bitvavo's 'BASE-QUOTE' format."""
        return symbol.replace("/", "-")

    def _normalize_symbol_from_venue(self, venue_symbol: str) -> str:
        """Converts Bitvavo's 'BASE-QUOTE' symbol back to 'BASE/QUOTE' format."""
        return venue_symbol.replace("-", "/")

    def _map_timeframe_to_interval(self, timeframe: str) -> str:
        """Maps a standard timeframe string to Bitvavo's interval string."""
        # Bitvavo supports 1m, 5m, 15m, 30m, 1h, 4h, 1d. It does not support 1w.
        supported_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
        if timeframe not in supported_timeframes:
            err_msg = f"Unsupported timeframe for Bitvavo: {timeframe}"
            raise ValueError(err_msg)
        return timeframe

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """Connects to the WebSocket, subscribes, and yields trade messages."""
        venue_symbols = [self._normalize_symbol_to_venue(s) for s in self.symbols]
        subscription_message = {
            "action": "subscribe",
            "channels": [{"name": "trades", "markets": venue_symbols}],
        }

        async with websockets.connect(self._BASE_WSS_URL) as websocket:
            await websocket.send(json.dumps(subscription_message))
            logger.info(
                f"[{self.venue_name}] Subscribed to 'trades' for: {venue_symbols}"
            )

            while True:
                message_raw = await websocket.recv()
                message = json.loads(message_raw)

                if message.get("event") == "trade":
                    # The message data is a single trade object
                    yield message
                elif message.get("event") == "subscribed":
                    logger.debug(
                        f"[{self.venue_name}] Subscription confirmation: {message}"
                    )
                elif "error" in message:
                    logger.error(f"[{self.venue_name}] Received error: {message}")
                else:
                    logger.debug(
                        f"[{self.venue_name}] Received other message: {message}"
                    )

    def _normalize_message(
        self, message: dict[str, Any]
    ) -> models_pb2.PriceUpdate | None:
        """Normalizes a raw WebSocket message from Bitvavo into a PriceUpdate."""
        if message.get("event") != "trade":
            return None

        try:
            # Timestamp is in nanoseconds
            ts_ns = int(message["timestamp"])
            return models_pb2.PriceUpdate(
                symbol=self._normalize_symbol_from_venue(message["market"]),
                venue=self.venue_name,
                # Use nanosecond timestamp as sequence number
                sequence_number=ts_ns,
                price=str(message["price"]),
                size=str(message["amount"]),
                side=str(message["side"]),
                exchange_timestamp=normalize_timestamp_to_rfc3339(ts_ns / 1e9),
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(
                f"[{self.venue_name}] Could not parse trade message: {message}. "
                f"Error: {e}"
            )
            return None

    async def get_historical_candles(
        self, symbol: str, timeframe: str, start_dt: datetime, end_dt: datetime
    ) -> list[models_pb2.Candle]:
        """Fetches historical OHLCV data from Bitvavo's REST API."""
        venue_symbol = self._normalize_symbol_to_venue(symbol)
        interval = self._map_timeframe_to_interval(timeframe)
        all_candles: list[models_pb2.Candle] = []
        max_candles_per_req = 1000

        current_start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        logger.info(
            f"[{self.venue_name}] Fetching historical data for {symbol} "
            f"from {start_dt} to {end_dt}"
        )

        while current_start_ms < end_ms:
            params = {
                "interval": interval,
                "limit": max_candles_per_req,
                "start": current_start_ms,
                "end": end_ms,
            }
            try:
                response = await self.http_client.get(
                    f"{self._BASE_API_URL}/{venue_symbol}/candles", params=params
                )
                response.raise_for_status()
                data = response.json()

                if not data:
                    break  # No more data

                for c in data:
                    # Candle format: [timestamp_ms, open, high, low, close, volume]
                    ts_ms = int(c[0])
                    open_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    close_time = open_time + timedelta(
                        seconds=self._map_interval_to_seconds(interval)
                    )
                    all_candles.append(
                        models_pb2.Candle(
                            symbol=symbol,
                            timeframe=timeframe,
                            open_time=normalize_timestamp_to_rfc3339(open_time),
                            close_time=normalize_timestamp_to_rfc3339(close_time),
                            open=str(c[1]),
                            high=str(c[2]),
                            low=str(c[3]),
                            close=str(c[4]),
                            volume=str(c[5]),
                        )
                    )

                # Move to the next time window. The last candle's timestamp is the
                # new start.
                last_ts_in_batch = int(data[-1][0])
                current_start_ms = last_ts_in_batch + 1

            except Exception as e:
                logger.error(
                    f"[{self.venue_name}] Failed to fetch historical data for "
                    f"{symbol}: {e}"
                )
                break

        # Sort and de-duplicate
        unique_candles = {c.open_time: c for c in all_candles}
        sorted_candles = sorted(unique_candles.values(), key=lambda c: c.open_time)

        logger.success(
            f"[{self.venue_name}] Fetched {len(sorted_candles)} unique "
            f"candles for {symbol}."
        )
        return sorted_candles

    def _map_interval_to_seconds(self, interval: str) -> int:
        """Helper to convert Bitvavo interval string to seconds."""
        unit = interval[-1]
        value = int(interval[:-1])
        if unit == "m":
            return value * 60
        if unit == "h":
            return value * 3600
        if unit == "d":
            return value * 86400
        return 0