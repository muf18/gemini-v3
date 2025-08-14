import json
import time
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import websockets
from loguru import logger

from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.types import models_pb2
from cryptochart.utils.time import normalize_timestamp_to_rfc3339


class DigifinexAdapter(ExchangeAdapter):
    """Adapter for connecting to the DigiFinex WebSocket and REST APIs."""

    _BASE_WSS_URL: str = "wss://openapi.digifinex.com/ws/v1/"
    _BASE_API_URL: str = "https://openapi.digifinex.com/v3"

    @property
    def venue_name(self) -> str:
        """Returns the unique, lowercase identifier for the exchange."""
        return "digifinex"

    def _normalize_symbol_to_venue(self, symbol: str, for_ws: bool = False) -> str:
        """Converts 'BASE/QUOTE' to DigiFinex's format."""
        # REST API uses 'btc_usdt' (lowercase)
        # WebSocket uses 'BTC_USDT' (uppercase)
        base, quote = symbol.split("/")
        if for_ws:
            return f"{base.upper()}_{quote.upper()}"
        return f"{base.lower()}_{quote.lower()}"

    def _normalize_symbol_from_venue(self, venue_symbol: str) -> str:
        """Converts DigiFinex's 'BASE_QUOTE' back to 'BASE/QUOTE' format."""
        return venue_symbol.replace("_", "/").upper()

    def _map_timeframe_to_period(self, timeframe: str) -> int:
        """Maps a standard timeframe string to DigiFinex's period in minutes."""
        period_map = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440,
            "1w": 10080,
        }
        if timeframe not in period_map:
            err_msg = f"Unsupported timeframe for DigiFinex: {timeframe}"
            raise ValueError(err_msg)
        return period_map[timeframe]

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """Connects to the WebSocket, subscribes, and yields trade messages."""
        venue_symbols = [
            self._normalize_symbol_to_venue(s, for_ws=True) for s in self.symbols
        ]
        subscription_message = {
            "id": int(time.time()),
            "method": "trades.subscribe",
            "params": venue_symbols,
        }

        async with websockets.connect(self._BASE_WSS_URL) as websocket:
            await websocket.send(json.dumps(subscription_message))
            logger.info(
                f"[{self.venue_name}] Subscribed to 'trades' for: {venue_symbols}"
            )

            while True:
                message_raw = await websocket.recv()
                message = json.loads(message_raw)

                if message.get("method") == "server.ping":
                    pong_message = {"id": None, "method": "server.pong", "params": []}
                    await websocket.send(json.dumps(pong_message))
                    logger.debug(
                        f"[{self.venue_name}] Responded to server ping with pong."
                    )
                    continue

                if message.get("method") == "trades.update":
                    # Params format: [is_full_update, [trade_list], symbol]
                    trade_list = message["params"][1]
                    symbol = message["params"][2]
                    for trade in trade_list:
                        trade["symbol"] = symbol  # Add symbol for normalization
                        yield trade
                elif "result" in message and message.get("error") is None:
                    logger.debug(
                        f"[{self.venue_name}] Subscription confirmation: {message}"
                    )
                elif "error" in message and message["error"] is not None:
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
        """Normalizes a raw trade data object from DigiFinex into a PriceUpdate."""
        try:
            return models_pb2.PriceUpdate(
                symbol=self._normalize_symbol_from_venue(message["symbol"]),
                venue=self.venue_name,
                sequence_number=int(message["id"]),
                price=str(message["price"]),
                size=str(message["amount"]),
                side=str(message["type"]),
                exchange_timestamp=normalize_timestamp_to_rfc3339(message["time"]),
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
        """Fetches historical OHLCV data from DigiFinex's REST API.

        Note: This endpoint may have an undocumented limit on the number of
        candles returned.
        """
        venue_symbol = self._normalize_symbol_to_venue(symbol, for_ws=False)
        period = self._map_timeframe_to_period(timeframe)
        all_candles: list[models_pb2.Candle] = []

        params = {
            "symbol": venue_symbol,
            "period": period,
            "start_time": int(start_dt.timestamp()),
            "end_time": int(end_dt.timestamp()),
        }
        logger.info(
            f"[{self.venue_name}] Fetching historical data for {symbol} "
            f"from {start_dt} to {end_dt}"
        )

        try:
            response = await self.http_client.get(
                f"{self._BASE_API_URL}/kline", params=params
            )
            response.raise_for_status()
            data = response.json()

            if data.get("code") != 0:
                logger.error(
                    f"[{self.venue_name}] API error fetching candles: {data}"
                )
                return []

            candles_data = data.get("data", [])
            if not candles_data:
                return []

            for c in candles_data:
                # Candle format: [timestamp, volume, close, high, low, open]
                ts = int(c[0])
                open_time = datetime.fromtimestamp(ts, tz=timezone.utc)
                close_time = open_time + timedelta(minutes=period)
                all_candles.append(
                    models_pb2.Candle(
                        symbol=symbol,
                        timeframe=timeframe,
                        open_time=normalize_timestamp_to_rfc3339(open_time),
                        close_time=normalize_timestamp_to_rfc3339(close_time),
                        open=str(c[5]),
                        high=str(c[3]),
                        low=str(c[4]),
                        close=str(c[2]),
                        volume=str(c[1]),
                    )
                )

        except Exception as e:
            logger.error(
                f"[{self.venue_name}] Failed to fetch historical data for "
                f"{symbol}: {e}"
            )
            return []

        # The API should return sorted data, but we sort just in case.
        unique_candles = {c.open_time: c for c in all_candles}
        sorted_candles = sorted(unique_candles.values(), key=lambda c: c.open_time)

        logger.success(
            f"[{self.venue_name}] Fetched {len(sorted_candles)} unique "
            f"candles for {symbol}."
        )
        return sorted_candles