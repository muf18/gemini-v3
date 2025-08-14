import json
import time
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

import websockets
from loguru import logger

from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.types import models_pb2
from cryptochart.utils.time import normalize_timestamp_to_rfc3339


class KrakenAdapter(ExchangeAdapter):
    """Adapter for connecting to the Kraken WebSocket (v2) and REST APIs."""

    _BASE_WSS_URL: str = "wss://ws.kraken.com/v2"
    _BASE_API_URL: str = "https://api.kraken.com/0/public"

    @property
    def venue_name(self) -> str:
        """Returns the unique, lowercase identifier for the exchange."""
        return "kraken"

    def _normalize_symbol_to_venue(self, symbol: str) -> str:
        """Kraken uses the standard 'BASE/QUOTE' format, so no change is needed."""
        # Kraken sometimes uses XBT for Bitcoin, but BTC is widely supported.
        # We will assume the standard symbols work.
        return symbol

    def _normalize_symbol_from_venue(self, venue_symbol: str) -> str:
        """Converts Kraken's symbol back to the standard format."""
        return venue_symbol

    def _map_timeframe_to_interval(self, timeframe: str) -> int:
        """Maps a standard timeframe string to Kraken's interval in minutes."""
        interval_map = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440,
            "1w": 10080,
        }
        if timeframe not in interval_map:
            err_msg = f"Unsupported timeframe for Kraken: {timeframe}"
            raise ValueError(err_msg)
        return interval_map[timeframe]

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """Connects to Kraken's WebSocket, subscribes, and yields trade data."""
        venue_symbols = [self._normalize_symbol_to_venue(s) for s in self.symbols]
        subscription_message = {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": venue_symbols},
            "req_id": int(time.time() * 1000),
        }

        async with websockets.connect(self._BASE_WSS_URL) as websocket:
            await websocket.send(json.dumps(subscription_message))
            logger.info(
                f"[{self.venue_name}] Subscribed to 'trade' for: {venue_symbols}"
            )

            while True:
                message_raw = await websocket.recv()
                message = json.loads(message_raw)

                if "channel" in message and message["channel"] == "trade":
                    # A single message can contain multiple trades
                    for trade_data in message["data"]:
                        yield trade_data
                elif message.get("method") == "heartbeat":
                    pass  # Heartbeats are good
                elif "method" in message and message.get("success"):
                    logger.debug(
                        f"[{self.venue_name}] Subscription status: {message}"
                    )
                elif "error" in message:
                    logger.error(
                        f"[{self.venue_name}] Received error: {message['error']}"
                    )
                else:
                    logger.debug(
                        f"[{self.venue_name}] Received other message: {message}"
                    )

    def _normalize_message(
        self, message: dict[str, Any]
    ) -> models_pb2.PriceUpdate | None:
        """Normalizes a single raw trade data object from Kraken into a PriceUpdate."""
        try:
            # Kraken timestamp is a float string with nanosecond precision
            ts_float = float(message["timestamp"])
            return models_pb2.PriceUpdate(
                symbol=self._normalize_symbol_from_venue(message["symbol"]),
                venue=self.venue_name,
                # Kraken does not provide a unique trade ID in the v2 feed,
                # so we use the timestamp as a sequence number.
                sequence_number=int(ts_float * 1e9),
                price=str(message["price"]),
                size=str(message["qty"]),
                side=message["side"],
                exchange_timestamp=normalize_timestamp_to_rfc3339(ts_float),
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
        """Fetches historical OHLCV data from Kraken's REST API."""
        venue_symbol = self._normalize_symbol_to_venue(symbol)
        interval = self._map_timeframe_to_interval(timeframe)
        all_candles: list[models_pb2.Candle] = []

        # Kraken uses 'since' for pagination, which is an inclusive timestamp.
        since_ts = int(start_dt.timestamp())

        logger.info(
            f"[{self.venue_name}] Fetching historical data for {symbol} "
            f"from {start_dt} to {end_dt}"
        )

        while True:
            params = {"pair": venue_symbol, "interval": interval, "since": since_ts}
            try:
                response = await self.http_client.get(
                    f"{self._BASE_API_URL}/OHLC", params=params
                )
                response.raise_for_status()
                data = response.json()

                if data.get("error"):
                    logger.error(
                        f"[{self.venue_name}] API error fetching candles: "
                        f"{data['error']}"
                    )
                    break

                result = data.get("result", {})
                candles_data = result.get(
                    next(iter(result.keys()), None) if result else None, []
                )

                if not candles_data:
                    break  # No more data

                for c in candles_data:
                    # Candle format: [time, open, high, low, close, vwap, volume, count]
                    ts = int(c[0])
                    if ts > end_dt.timestamp():
                        continue

                    open_time = datetime.fromtimestamp(ts, tz=timezone.utc)
                    close_time = datetime.fromtimestamp(
                        ts + interval * 60, tz=timezone.utc
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
                            volume=str(c[6]),
                        )
                    )

                # The 'last' value is the timestamp for the next page of data
                last_ts = result.get("last")
                if last_ts is None or last_ts <= since_ts:
                    break
                since_ts = last_ts

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