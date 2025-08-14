import asyncio
import csv
import io
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

import aiofiles
from loguru import logger

from cryptochart.types import models_pb2

# --- Constants ---

# The header row for all generated CSV files.
CSV_HEADER: list[str] = [
    "symbol",
    "venue",
    "sequence",
    "timestamp_rfc3339",
    "price",
    "size",
    "bid",
    "ask",
    "vwap",
    "cum_volume",
    "timeframe",
]

# Write to disk after this many records have been buffered.
WRITE_BATCH_SIZE: int = 500

# Or write to disk after this many seconds, whichever comes first.
FLUSH_INTERVAL_SECONDS: float = 10.0


class Persistence:
    """
    Handles optional, non-blocking writing of aggregated data to CSV files.

    This component runs as a dedicated background task, consuming data from a
    queue. It batches writes to minimize I/O overhead and uses `aiofiles` to
    ensure the main application event loop is never blocked by disk operations.
    File logging can be enabled or disabled at runtime.
    """

    def __init__(
        self,
        input_queue: "asyncio.Queue[models_pb2.PriceUpdate]",
        output_directory: Path,
    ) -> None:
        """
        Initializes the Persistence engine.

        Args:
            input_queue: The queue to receive aggregated PriceUpdate messages from.
            output_directory: The base directory where CSV files will be stored.
        """
        self.input_queue = input_queue
        self.output_directory = output_directory
        self._enabled = False
        self._buffer: list[models_pb2.PriceUpdate] = []
        self._open_files: dict[Path, Any] = {}  # Using Any for aiofiles handle
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._running = asyncio.Event()

    @property
    def is_enabled(self) -> bool:
        """Returns True if CSV persistence is currently enabled."""
        return self._enabled

    async def set_enabled(self, enabled: bool) -> None:
        """
        Enables or disables CSV persistence at runtime.

        When disabling, it performs a final flush of any buffered data.

        Args:
            enabled: True to enable persistence, False to disable it.
        """
        async with self._lock:
            if self._enabled == enabled:
                return  # No change
            self._enabled = enabled
            logger.info(
                f"CSV persistence has been {'ENABLED' if enabled else 'DISABLED'}."
            )
            if not enabled:
                # Flush any remaining data in the buffer before stopping.
                await self._flush_buffer()

    def start(self) -> None:
        """Starts the persistence writer loop in a background task."""
        if self._task is None or self._task.done():
            self._running.set()
            self.output_directory.mkdir(parents=True, exist_ok=True)
            self._task = asyncio.create_task(self._run())
            logger.info(
                f"Persistence engine started. Output dir: '{self.output_directory}'"
            )
        else:
            logger.warning("Persistence engine is already running.")

    async def stop(self) -> None:
        """Stops the writer loop and ensures all data is flushed and files are closed."""
        if not self._running.is_set():
            logger.warning("Persistence engine is not running.")
            return

        logger.info("Stopping persistence engine...")
        self._running.clear()
        if self._task:
            try:
                self.input_queue.put_nowait(None)  # type: ignore[arg-type]
                self._task.cancel()
                await self._task
            except asyncio.CancelledError:
                pass  # This is expected
            finally:
                self._task = None
        logger.info("Persistence engine stopped.")

    async def _run(self) -> None:
        """The main writer loop."""
        while self._running.is_set():
            try:
                update = await asyncio.wait_for(
                    self.input_queue.get(), timeout=FLUSH_INTERVAL_SECONDS
                )
                if update is None:  # Sentinel value
                    continue

                async with self._lock:
                    if self._enabled:
                        self._buffer.append(update)

                if len(self._buffer) >= WRITE_BATCH_SIZE:
                    await self._flush_buffer()

            except asyncio.TimeoutError:
                # Timeout acts as our periodic flush trigger for low-volume periods.
                await self._flush_buffer()
            except asyncio.CancelledError:
                logger.info("Persistence writer loop cancelled.")
                break
            except Exception:
                logger.exception("Unexpected error in persistence writer loop.")
                await asyncio.sleep(1)

        # Perform a final flush and close all files upon exit.
        logger.info("Persistence loop finishing. Performing final flush and cleanup.")
        await self._flush_buffer()
        await self._close_all_files()

    async def _flush_buffer(self) -> None:
        """Writes the contents of the buffer to the appropriate CSV files."""
        async with self._lock:
            if not self._buffer:
                return

            logger.debug(f"Flushing {len(self._buffer)} records to disk.")
            # Group records by their target filename
            records_by_file = defaultdict(list)
            for record in self._buffer:
                filename = self._get_filename_for_record(record)
                records_by_file[filename].append(record)

            for filename, records in records_by_file.items():
                try:
                    file_handle = await self._get_file_handle(filename)
                    # Use io.StringIO to build the CSV in memory
                    string_io = io.StringIO()
                    writer = csv.writer(string_io)
                    for record in records:
                        writer.writerow(self._format_record_for_csv(record))

                    await file_handle.write(string_io.getvalue())
                    await file_handle.flush()
                except Exception:
                    logger.exception(f"Failed to write batch to file: {filename}")

            self._buffer.clear()

    def _get_filename_for_record(self, record: models_pb2.PriceUpdate) -> Path:
        """Determines the correct filename for a given record."""
        sanitized_symbol = record.symbol.replace("/", "-").upper()
        # Use the exchange timestamp to determine the date for the filename.
        try:
            dt = datetime.fromisoformat(record.exchange_timestamp.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            date_str = datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

        return self.output_directory / f"{sanitized_symbol}_{date_str}.csv"

    async def _get_file_handle(self, filename: Path) -> TextIO:
        """Gets or creates a file handle, writing a header if the file is new."""
        if filename in self._open_files:
            return self._open_files[filename]

        needs_header = not await aiofiles.os.path.exists(filename)
        # Use 'a' for append mode, which creates the file if it doesn't exist.
        handle = await aiofiles.open(filename, mode="a", encoding="utf-8", newline="")

        if needs_header:
            logger.info(f"Creating new CSV file with header: {filename}")
            string_io = io.StringIO()
            writer = csv.writer(string_io)
            writer.writerow(CSV_HEADER)
            await handle.write(string_io.getvalue())
            await handle.flush()

        self._open_files[filename] = handle
        return handle

    async def _close_all_files(self) -> None:
        """Closes all open file handles."""
        logger.debug(f"Closing {len(self._open_files)} open file handles.")
        for handle in self._open_files.values():
            try:
                await handle.close()
            except Exception:
                logger.exception("Error closing a file handle.")
        self._open_files.clear()

    @staticmethod
    def _format_record_for_csv(update: models_pb2.PriceUpdate) -> list[str]:
        """Converts a PriceUpdate message to a list of strings for CSV writing."""
        return [
            update.symbol,
            update.venue,
            str(update.sequence_number),
            update.exchange_timestamp,
            update.price,
            update.size,
            update.best_bid,
            update.best_ask,
            update.vwap,
            update.cum_volume,
            update.timeframe,
        ]