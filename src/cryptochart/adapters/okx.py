import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import websockets
from loguru import logger

from cryptochart.adapters.base import ExchangeAdapter
from cryptochart.types import models_pb2
from cryptochart.utils.time import normalize_timestamp_to_rfc3339


class OKXAdapter(ExchangeAdapter):
    """Adapter for connecting to the OKX WebSocket and REST APIs."""

    _BASE_WSS_URL: str = "wss://ws.okx.com:8443/ws/v5/public"
    _BASE_API_URL: str = "https://www.okx.com/api/v5"

    @property
    def venue_name(self) -> str:
        """Returns the unique, lowercase identifier for the exchange."""
        return "okx"

    def _normalize_symbol_to_venue(self, symbol: str) -> str:
        """Converts a 'BASE/QUOTE' symbol to OKX's 'BASE-QUOTE' format."""
        return symbol.replace("/", "-")

    def _normalize_symbol_from_venue(self, venue_symbol: str) -> str:
        """Converts OKX's 'BASE-QUOTE' symbol back to 'BASE/QUOTE' format."""
        return venue_symbol.replace("-", "/")

    def _map_timeframe_to_bar(self, timeframe: str) -> str:
        """Maps a standard timeframe string to OKX's 'bar' string format."""
        bar_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1H",
            "4h": "4H",
            "1d": "1D",
            "1w": "1W",
        }
        if timeframe not in bar_map:
            err_msg = f"Unsupported timeframe for OKX: {timeframe}"
            raise ValueError(err_msg)
        return bar_map[timeframe]

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """Connects to the WebSocket, subscribes, and yields trade messages."""
        args = [
            {"channel": "trades", "instId": self._normalize_symbol_to_venue(s)}
            for s in self.symbols
        ]
        subscription_message = {"op": "subscribe", "args": args}

        async with websockets.connect(self._BASE_WSS_URL) as websocket:
            await websocket.send(json.dumps(subscription_message))
            logger.info(
                f"[{self.venue_name}] Subscribed to 'trades' for: "
                f"{[a['instId'] for a in args]}"
            )

            while True:
                # OKX requires a pong response within 30s of a ping.
                try:
                    message_raw = await asyncio.wait_for(websocket.recv(), timeout=35.0)
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[{self.venue_name}] WebSocket timeout. "
                        "Sending keepalive ping."
                    )
                    await websocket.ping()
                    continue

                if message_raw == "ping":
                    await websocket.send("pong")
                    continue

                message = json.loads(message_raw)

                if "event" in message:
                    if message["event"] == "subscribe":
                        logger.debug(
                            f"[{self.venue_name}] Subscription confirmation: "
                            f"{message['arg']}"
                        )
                    elif message["event"] == "error":
                        logger.error(f"[{self.venue_name}] Received error: {message}")
                    continue

                if "arg" in message and message["arg"]["channel"] == "trades":
                    for trade_data in message["data"]:
                        yield trade_data
                else:
                    logger.debug(
                        f"[{self.venue_name}] Received other message: {message}"
                    )

    def _normalize_message(
        self, message: dict[str, Any]
    ) -> models_pb2.PriceUpdate | None:
        """Normalizes a raw trade data object from OKX into a PriceUpdate."""
        try:
            return models_pb2.PriceUpdate(
                symbol=self._normalize_symbol_from_venue(message["instId"]),
                venue=self.venue_name,
                sequence_number=int(message["tradeId"]),
                price=str(message["px"]),
                size=str(message["sz"]),
                side=str(message["side"]),
                exchange_timestamp=normalize_timestamp_to_rfc3339(int(message["ts"])),
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
        """Fetches historical OHLCV data from OKX's REST API."""
        venue_symbol = self._normalize_symbol_to_venue(symbol)
        bar = self._map_timeframe_to_bar(timeframe)
        all_candles: list[models_pb2.Candle] = []
        max_candles_per_req = 100

        current_end_ms = int(end_dt.timestamp() * 1000)
        start_ms = int(start_dt.timestamp() * 1000)

        logger.info(
            f"[{self.venue_name}] Fetching historical data for {symbol} "
            f"from {start_dt} to {end_dt}"
        )

        while current_end_ms > start_ms:
            params = {
                "instId": venue_symbol,
                "bar": bar,
                "after": current_end_ms,
                "limit": max_candles_per_req,
            }
            try:
                response = await self.http_client.get(
                    f"{self._BASE_API_URL}/market/history-candles", params=params
                )
                response.raise_for_status()
                data = response.json()

                if data.get("code") != "0":
                    logger.error(
                        f"[{self.venue_name}] API error fetching candles: "
                        f"{data.get('msg')}"
                    )
                    break

                candles_data = data.get("data", [])
                if not candles_data:
                    break  # No more data

                for c in candles_data:
                    # Candle format: [ts, o, h, l, c, vol, volCcy, ...]
                    ts_ms = int(c[0])
                    open_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    close_time = open_time + timedelta(
                        seconds=self._map_bar_to_seconds(bar)
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

                # Paginate backwards by setting the new 'after' to the timestamp
                # of the oldest candle received
                oldest_ts_in_batch = int(candles_data[-1][0])
                current_end_ms = oldest_ts_in_batch

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

    def _map_bar_to_seconds(self, bar: str) -> int:
        """Helper to convert OKX bar string to seconds for close time calculation."""
        unit = bar[-1].lower()
        value = int(bar[:-1])
        if unit == "m":
            return value * 60
        if unit == "h":
            return value * 3600
        if unit == "d":
            return value * 86400
        if unit == "w":
            return value * 604800
        return 0