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


class BitgetAdapter(ExchangeAdapter):
    """Adapter for connecting to the Bitget WebSocket and REST APIs."""

    _BASE_WSS_URL: str = "wss://ws.bitget.com/v2/spot/public"
    _BASE_API_URL: str = "https://api.bitget.com/api/v2/spot"

    @property
    def venue_name(self) -> str:
        """Returns the unique, lowercase identifier for the exchange."""
        return "bitget"

    def _normalize_symbol_to_venue(self, symbol: str) -> str:
        """Converts a 'BASE/QUOTE' symbol to Bitget's 'BASEQUOTE' format."""
        return symbol.replace("/", "").upper()

    def _normalize_symbol_from_venue(self, venue_symbol: str) -> str:
        """Converts Bitget's 'BASEQUOTE' symbol back to 'BASE/QUOTE' format."""
        # Heuristic similar to Binance's
        known_quotes = ["USDT", "USDC", "BTC", "ETH", "BGB"]
        for quote in known_quotes:
            if venue_symbol.endswith(quote):
                base = venue_symbol[: -len(quote)]
                return f"{base}/{quote}"
        return venue_symbol

    def _map_timeframe_to_granularity(self, timeframe: str) -> str:
        """Maps a standard timeframe string to Bitget's granularity string."""
        granularity_map = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1H",
            "4h": "4H",
            "1d": "1D",
            "1w": "1W",
        }
        if timeframe not in granularity_map:
            err_msg = f"Unsupported timeframe for Bitget: {timeframe}"
            raise ValueError(err_msg)
        return granularity_map[timeframe]

    async def _stream_messages(self) -> AsyncGenerator[dict[str, Any], None]:
        """Connects to the WebSocket, subscribes, and yields trade messages."""
        args = [
            {
                "instType": "SPOT",
                "channel": "trade",
                "instId": self._normalize_symbol_to_venue(s),
            }
            for s in self.symbols
        ]
        subscription_message = {"op": "subscribe", "args": args}

        async with websockets.connect(self._BASE_WSS_URL) as websocket:
            await websocket.send(json.dumps(subscription_message))
            logger.info(
                f"[{self.venue_name}] Subscribed to 'trade' for: "
                f"{[a['instId'] for a in args]}"
            )

            while True:
                try:
                    message_raw = await asyncio.wait_for(websocket.recv(), timeout=25.0)
                except asyncio.TimeoutError:
                    await websocket.send("ping")
                    logger.debug(
                        f"[{self.venue_name}] Sent ping to keep connection alive."
                    )
                    continue

                if message_raw == "pong":
                    logger.debug(f"[{self.venue_name}] Received pong.")
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

                if (
                    "action" in message
                    and "arg" in message
                    and message["arg"]["channel"] == "trade"
                ):
                    # Both snapshot and update actions contain trade data
                    for trade_data in message["data"]:
                        # Add instId to each trade for normalization
                        trade_with_symbol = {
                            "instId": message["arg"]["instId"],
                            "trade": trade_data,
                        }
                        yield trade_with_symbol
                else:
                    logger.debug(
                        f"[{self.venue_name}] Received other message: {message}"
                    )

    def _normalize_message(
        self, message: dict[str, Any]
    ) -> models_pb2.PriceUpdate | None:
        """Normalizes a raw trade data object from Bitget into a PriceUpdate."""
        try:
            # Data format: [timestamp_ms, price, size, side]
            trade_info = message["trade"]
            ts_ms = int(trade_info[0])
            return models_pb2.PriceUpdate(
                symbol=self._normalize_symbol_from_venue(message["instId"]),
                venue=self.venue_name,
                # No unique trade ID, use nanosecond timestamp as sequence
                sequence_number=ts_ms * 1_000_000,
                price=str(trade_info[1]),
                size=str(trade_info[2]),
                side=str(trade_info[3]),
                exchange_timestamp=normalize_timestamp_to_rfc3339(ts_ms),
            )
        except (KeyError, ValueError, TypeError, IndexError) as e:
            logger.warning(
                f"[{self.venue_name}] Could not parse trade message: {message}. "
                f"Error: {e}"
            )
            return None

    async def get_historical_candles(
        self, symbol: str, timeframe: str, start_dt: datetime, end_dt: datetime
    ) -> list[models_pb2.Candle]:
        """Fetches historical OHLCV data from Bitget's REST API."""
        venue_symbol = self._normalize_symbol_to_venue(symbol)
        granularity = self._map_timeframe_to_granularity(timeframe)
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
                "symbol": venue_symbol,
                "granularity": granularity,
                "endTime": current_end_ms,
                "limit": max_candles_per_req,
            }
            try:
                response = await self.http_client.get(
                    f"{self._BASE_API_URL}/market/candles", params=params
                )
                response.raise_for_status()
                data = response.json()

                if data.get("code") != "00000":
                    logger.error(
                        f"[{self.venue_name}] API error fetching candles: "
                        f"{data.get('msg')}"
                    )
                    break

                candles_data = data.get("data", [])
                if not candles_data:
                    break  # No more data

                for c in candles_data:
                    ts_ms = int(c[0])
                    open_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    close_time = open_time + timedelta(
                        seconds=self._map_granularity_to_seconds(granularity)
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
                            volume=str(c[5]),  # Base volume
                        )
                    )

                # Paginate backwards by setting the new endTime to the timestamp
                # of the oldest candle received
                oldest_ts_in_batch = int(candles_data[-1][0])
                current_end_ms = oldest_ts_in_batch - 1

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

    def _map_granularity_to_seconds(self, granularity: str) -> int:
        """Helper to convert Bitget granularity string to seconds."""
        unit = granularity[-1].lower()
        value = int(granularity[:-1])
        if unit == "m":
            return value * 60
        if unit == "h":
            return value * 3600
        if unit == "d":
            return value * 86400
        if unit == "w":
            return value * 604800
        return 0