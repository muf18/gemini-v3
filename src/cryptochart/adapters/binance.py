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


class BinanceAdapter(ExchangeAdapter):
    """Adapter for connecting to the Binance WebSocket and REST APIs."""

    _BASE_WSS_URL: str = "wss://stream.binance.com:9443/stream"
    _BASE_API_URL: str = "https://api.binance.com/api/v3"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Create a mapping to easily convert venue symbols back to standard format
        self._venue_to_standard_symbol_map = {
            self._normalize_symbol_to_venue(s): s for s in self.symbols
        }

    @property
    def venue_name(self) -> str:
        """Returns the unique, lowercase identifier for the exchange."""
        return "binance"

    def _normalize_symbol_to_venue(self, symbol: str) -> str:
        """Converts a 'BASE/QUOTE' symbol to Binance's 'BASEQUOTE' format."""
        return symbol.replace("/", "").lower()

    def _normalize_symbol_from_venue(self, venue_symbol: str) -> str:
        """Converts Binance's 'BASEQUOTE' symbol back to 'BASE/QUOTE' format."""
        # This is a simple heuristic. A robust solution would use the
        # /exchangeInfo endpoint.
        known_quotes = ["USDT", "BUSD", "BTC", "ETH", "EUR", "GBP", "USD"]
        for quote in known_quotes:
            if venue_symbol.endswith(quote):
                base = venue_symbol[: -len(quote)]
                return f"{base}/{quote}"
        # Fallback for unknown pairs
        return venue_symbol

    def _map_timeframe_to_interval(self, timeframe: str) -> str:
        """Maps a standard timeframe string to Binance's interval string."""
        # Binance uses the same strings for the most common timeframes.
        supported_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
        if timeframe not in supported_timeframes:
            err_msg = f"Unsupported timeframe for Binance: {timeframe}"
            raise ValueError(err_msg)
        return timeframe

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """Connects to the WebSocket, subscribes, and yields messages."""
        streams = [
            f"{self._normalize_symbol_to_venue(s)}@trade" for s in self.symbols
        ]
        subscription_message = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": int(time.time()),
        }

        async with websockets.connect(self._BASE_WSS_URL) as websocket:
            await websocket.send(json.dumps(subscription_message))
            logger.info(f"[{self.venue_name}] Subscribed to streams: {streams}")

            while True:
                message_raw = await websocket.recv()
                message = json.loads(message_raw)

                if "stream" in message and "data" in message:
                    yield message["data"]
                elif "result" in message and message["result"] is None:
                    logger.debug(
                        f"[{self.venue_name}] Subscription confirmation: {message}"
                    )
                else:
                    logger.debug(
                        f"[{self.venue_name}] Received other message: {message}"
                    )

    def _normalize_message(
        self, message: dict[str, Any]
    ) -> models_pb2.PriceUpdate | None:
        """Normalizes a raw WebSocket message from Binance into a PriceUpdate."""
        if message.get("e") != "trade":
            return None

        try:
            # If 'm' is True, the buyer is the maker, which means it was a
            # sell-side initiated trade.
            side = "sell" if message["m"] else "buy"

            return models_pb2.PriceUpdate(
                symbol=self._normalize_symbol_from_venue(message["s"]),
                venue=self.venue_name,
                sequence_number=int(message["t"]),
                price=str(message["p"]),
                size=str(message["q"]),
                side=side,
                exchange_timestamp=normalize_timestamp_to_rfc3339(message["T"]),
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
        """Fetches historical OHLCV data from Binance's REST API."""
        venue_symbol = self._normalize_symbol_to_venue(symbol).upper()
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
                "symbol": venue_symbol,
                "interval": interval,
                "startTime": current_start_ms,
                "limit": max_candles_per_req,
            }
            try:
                response = await self.http_client.get(
                    f"{self._BASE_API_URL}/klines", params=params
                )
                response.raise_for_status()
                data = response.json()

                if not data:
                    break  # No more data

                for c in data:
                    # Candle format: [Open time, Open, High, Low, Close, Volume, ...]
                    open_time_ms = c[0]
                    open_time = datetime.fromtimestamp(
                        open_time_ms / 1000, tz=timezone.utc
                    )
                    close_time_ms = c[6]
                    close_time = datetime.fromtimestamp(
                        close_time_ms / 1000, tz=timezone.utc
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

                # Move to the next time window
                current_start_ms = data[-1][0] + 1

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