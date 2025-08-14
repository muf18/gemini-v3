import json
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import websockets
from loguru import logger

from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.types import models_pb2
from cryptochart.utils.time import normalize_timestamp_to_rfc3339


class BitstampAdapter(ExchangeAdapter):
    """Adapter for connecting to the Bitstamp WebSocket and REST APIs."""

    _BASE_WSS_URL: str = "wss://ws.bitstamp.net"
    _BASE_API_URL: str = "https://www.bitstamp.net/api/v2"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Create a mapping to easily convert venue symbols back to standard format
        self._venue_to_standard_symbol_map = {
            self._normalize_symbol_to_venue(s): s for s in self.symbols
        }

    @property
    def venue_name(self) -> str:
        """Returns the unique, lowercase identifier for the exchange."""
        return "bitstamp"

    def _normalize_symbol_to_venue(self, symbol: str) -> str:
        """Converts a 'BASE/QUOTE' symbol to Bitstamp's 'basequote' format."""
        return symbol.replace("/", "").lower()

    def _normalize_symbol_from_venue(self, venue_symbol: str) -> str:
        """Converts Bitstamp's 'basequote' symbol back to 'BASE/QUOTE' format."""
        return self._venue_to_standard_symbol_map.get(
            venue_symbol, venue_symbol.upper()
        )

    def _map_timeframe_to_step(self, timeframe: str) -> int:
        """Maps a standard timeframe string to Bitstamp's 'step' in seconds."""
        step_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }
        # Bitstamp does not support 1w, so we raise an error.
        if timeframe not in step_map:
            err_msg = f"Unsupported timeframe for Bitstamp: {timeframe}"
            raise ValueError(err_msg)
        return step_map[timeframe]

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """Connects to the WebSocket, subscribes, and yields trade messages."""
        async with websockets.connect(self._BASE_WSS_URL) as websocket:
            for symbol in self.symbols:
                venue_symbol = self._normalize_symbol_to_venue(symbol)
                subscription_message = {
                    "event": "bts:subscribe",
                    "data": {"channel": f"live_trades_{venue_symbol}"},
                }
                await websocket.send(json.dumps(subscription_message))
                logger.info(
                    f"[{self.venue_name}] Subscribing to channel for: {venue_symbol}"
                )

            while True:
                message_raw = await websocket.recv()
                message = json.loads(message_raw)

                if message.get("event") == "trade":
                    yield message
                elif message.get("event") == "bts:subscription_succeeded":
                    logger.debug(
                        f"[{self.venue_name}] Subscription success: "
                        f"{message['channel']}"
                    )
                elif message.get("event") == "bts:request_reconnect":
                    logger.info(
                        f"[{self.venue_name}] Server requested reconnect. "
                        "Closing connection."
                    )
                    break  # Exit to allow the run loop to handle reconnection
                else:
                    logger.debug(
                        f"[{self.venue_name}] Received non-trade message: {message}"
                    )

    def _normalize_message(
        self, message: dict[str, Any]
    ) -> models_pb2.PriceUpdate | None:
        """Normalizes a raw WebSocket message from Bitstamp into a PriceUpdate."""
        if message.get("event") != "trade":
            return None

        try:
            data = message["data"]
            # Channel name is like "live_trades_btcusd"
            venue_symbol = message["channel"].replace("live_trades_", "")

            return models_pb2.PriceUpdate(
                symbol=self._normalize_symbol_from_venue(venue_symbol),
                venue=self.venue_name,
                sequence_number=int(data["id"]),
                price=data["price_str"],
                size=data["amount_str"],
                # 0 for buy, 1 for sell
                side="buy" if data["type"] == 0 else "sell",
                exchange_timestamp=normalize_timestamp_to_rfc3339(
                    int(data["microtimestamp"])
                ),
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
        """Fetches historical OHLCV data from Bitstamp's REST API."""
        venue_symbol = self._normalize_symbol_to_venue(symbol)
        step = self._map_timeframe_to_step(timeframe)
        all_candles: list[models_pb2.Candle] = []
        max_candles_per_req = 1000

        current_start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        logger.info(
            f"[{self.venue_name}] Fetching historical data for {symbol} "
            f"from {start_dt} to {end_dt}"
        )

        while current_start_ts < end_ts:
            params = {
                "step": step,
                "limit": max_candles_per_req,
                "start": current_start_ts,
            }
            try:
                response = await self.http_client.get(
                    f"{self._BASE_API_URL}/ohlc/{venue_symbol}/", params=params
                )
                response.raise_for_status()
                data = response.json()

                ohlc_data = data.get("data", {}).get("ohlc", [])
                if not ohlc_data:
                    break  # No more data

                for c in ohlc_data:
                    # Candle format: {'high': '...', 'timestamp': '...', ...}
                    ts = int(c["timestamp"])
                    open_time = datetime.fromtimestamp(ts, tz=timezone.utc)
                    close_time = open_time + timedelta(seconds=step)
                    all_candles.append(
                        models_pb2.Candle(
                            symbol=symbol,
                            timeframe=timeframe,
                            open_time=normalize_timestamp_to_rfc3339(open_time),
                            close_time=normalize_timestamp_to_rfc3339(close_time),
                            open=str(c["open"]),
                            high=str(c["high"]),
                            low=str(c["low"]),
                            close=str(c["close"]),
                            volume=str(c["volume"]),
                        )
                    )

                # Move to the next time window
                last_ts_in_batch = int(ohlc_data[-1]["timestamp"])
                current_start_ts = last_ts_in_batch + step

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