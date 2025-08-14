import json
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import websockets
from loguru import logger

from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.types import models_pb2
from cryptochart.utils.time import normalize_timestamp_to_rfc3339


class CoinbaseAdapter(ExchangeAdapter):
    """Adapter for connecting to the Coinbase Exchange (formerly GDAX/Coinbase Pro).

    This adapter handles WebSocket and REST APIs.
    """

    _BASE_WSS_URL: str = "wss://ws-feed.exchange.coinbase.com"
    _BASE_API_URL: str = "https://api.exchange.coinbase.com"

    @property
    def venue_name(self) -> str:
        """Returns the unique, lowercase identifier for the exchange."""
        return "coinbase"

    def _normalize_symbol_to_venue(self, symbol: str) -> str:
        """Converts a 'BASE/QUOTE' symbol to Coinbase's 'BASE-QUOTE' format."""
        return symbol.replace("/", "-")

    def _normalize_symbol_from_venue(self, venue_symbol: str) -> str:
        """Converts Coinbase's 'BASE-QUOTE' symbol to 'BASE/QUOTE' format."""
        return venue_symbol.replace("-", "/")

    def _map_timeframe_to_granularity(self, timeframe: str) -> int:
        """Maps a standard timeframe string to Coinbase's granularity in seconds."""
        granularity_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }
        if timeframe not in granularity_map:
            err_msg = f"Unsupported timeframe for Coinbase: {timeframe}"
            raise ValueError(err_msg)
        return granularity_map[timeframe]

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """Connects to the WebSocket, subscribes, and yields trade messages."""
        venue_symbols = [self._normalize_symbol_to_venue(s) for s in self.symbols]
        subscription_message = {
            "type": "subscribe",
            "product_ids": venue_symbols,
            "channels": ["matches"],
        }

        async with websockets.connect(self._BASE_WSS_URL) as websocket:
            await websocket.send(json.dumps(subscription_message))
            logger.info(
                f"[{self.venue_name}] Subscribed to 'matches' for: {venue_symbols}"
            )

            while True:
                message_raw = await websocket.recv()
                message = json.loads(message_raw)
                yield message

    def _normalize_message(
        self, message: dict[str, Any]
    ) -> models_pb2.PriceUpdate | None:
        """Normalizes a raw WebSocket message from Coinbase into a PriceUpdate."""
        msg_type = message.get("type")

        if msg_type in ("match", "last_match"):
            try:
                return models_pb2.PriceUpdate(
                    symbol=self._normalize_symbol_from_venue(message["product_id"]),
                    venue=self.venue_name,
                    sequence_number=int(message["trade_id"]),
                    price=message["price"],
                    size=message["size"],
                    side=message["side"],
                    exchange_timestamp=normalize_timestamp_to_rfc3339(message["time"]),
                )
            except (KeyError, ValueError) as e:
                logger.warning(
                    f"[{self.venue_name}] Could not parse match message: {message}. "
                    f"Error: {e}"
                )
                return None

        elif msg_type == "subscriptions":
            logger.debug(f"[{self.venue_name}] Subscription confirmation: {message}")
        elif msg_type == "heartbeat":
            pass  # Heartbeats are good, no action needed
        else:
            logger.debug(f"[{self.venue_name}] Received non-trade message: {message}")

        return None

    async def get_historical_candles(
        self, symbol: str, timeframe: str, start_dt: datetime, end_dt: datetime
    ) -> list[models_pb2.Candle]:
        """Fetches historical OHLCV data from Coinbase's REST API."""
        venue_symbol = self._normalize_symbol_to_venue(symbol)
        granularity = self._map_timeframe_to_granularity(timeframe)
        all_candles: list[models_pb2.Candle] = []
        max_candles_per_req = 300

        current_end_dt = end_dt

        logger.info(
            f"[{self.venue_name}] Fetching historical data for {symbol} "
            f"from {start_dt} to {end_dt}"
        )

        while current_end_dt > start_dt:
            # Calculate the start time for this batch
            delta_seconds = max_candles_per_req * granularity
            current_start_dt = current_end_dt - timedelta(seconds=delta_seconds)

            # Ensure we don't fetch data before the requested start time
            current_start_dt = max(current_start_dt, start_dt)

            params = {
                "start": current_start_dt.isoformat(),
                "end": current_end_dt.isoformat(),
                "granularity": granularity,
            }

            try:
                response = await self.http_client.get(
                    f"{self._BASE_API_URL}/products/{venue_symbol}/candles",
                    params=params,
                )
                response.raise_for_status()
                data = response.json()

                if not data:
                    break  # No more data in this range

                for c in data:
                    # Candle format: [time, low, high, open, close, volume]
                    open_time = datetime.fromtimestamp(c[0], tz=timezone.utc)
                    close_time = open_time + timedelta(seconds=granularity)
                    all_candles.append(
                        models_pb2.Candle(
                            symbol=symbol,
                            timeframe=timeframe,
                            open_time=normalize_timestamp_to_rfc3339(open_time),
                            close_time=normalize_timestamp_to_rfc3339(close_time),
                            open=str(c[3]),
                            high=str(c[2]),
                            low=str(c[1]),
                            close=str(c[4]),
                            volume=str(c[5]),
                        )
                    )

                # The first candle in the response is the oldest one
                oldest_ts_in_batch = data[-1][0]
                current_end_dt = datetime.fromtimestamp(
                    oldest_ts_in_batch, tz=timezone.utc
                )

            except Exception as e:
                logger.error(
                    f"[{self.venue_name}] Failed to fetch historical data for "
                    f"{symbol}: {e}"
                )
                # Stop trying on error to avoid spamming the API
                break

        # Sort data chronologically (oldest first) and remove duplicates
        unique_candles = {c.open_time: c for c in all_candles}
        sorted_candles = sorted(unique_candles.values(), key=lambda c: c.open_time)

        logger.success(
            f"[{self.venue_name}] Fetched {len(sorted_candles)} unique "
            f"candles for {symbol}."
        )
        return sorted_candles