import asyncio
import itertools
from collections import defaultdict
from typing import Any

from loguru import logger

from cryptochart.types import models_pb2


class Publisher:
    """A fan-out service that distributes aggregated data to subscribers.

    This class receives aggregated `PriceUpdate` messages from the Aggregator
    and forwards them to any part of the application that has subscribed to
    a specific symbol and timeframe combination. This decouples the data
    processing core from the UI or any other data consumers.
    """

    def __init__(self, input_queue: "asyncio.Queue[models_pb2.PriceUpdate]") -> None:
        """Initializes the Publisher.

        Args:
            input_queue: The queue from which to receive aggregated updates.
        """
        self.input_queue = input_queue
        # A mapping from (symbol, timeframe) to a dict of {subscription_id: queue}
        self._subscriptions: defaultdict[
            tuple[str, str], dict[int, asyncio.Queue[Any]]
        ] = defaultdict(dict)
        # A reverse mapping from subscription_id to its (symbol, timeframe) key
        self._id_to_key: dict[int, tuple[str, str]] = {}
        self._id_generator = itertools.count(1)
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._running = asyncio.Event()

    def start(self) -> None:
        """Starts the publisher's processing loop in a background task."""
        if self._task is None or self._task.done():
            self._running.set()
            self._task = asyncio.create_task(self._run())
            logger.info("Publisher started.")
        else:
            logger.warning("Publisher is already running.")

    async def stop(self) -> None:
        """Stops the publisher's processing loop gracefully."""
        if not self._running.is_set():
            logger.warning("Publisher is not running.")
            return

        logger.info("Stopping Publisher...")
        self._running.clear()
        if self._task:
            try:
                self.input_queue.put_nowait(None)  # type: ignore[arg-type]
                self._task.cancel()
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None
        logger.info("Publisher stopped.")

    async def subscribe(
        self, symbol: str, timeframe: str, queue: "asyncio.Queue[Any]"
    ) -> int:
        """Subscribes a queue to receive updates for a symbol and timeframe.

        Args:
            symbol: The trading symbol (e.g., "BTC/USD").
            timeframe: The timeframe (e.g., "1m", "5m").
            queue: The asyncio.Queue to which updates will be sent.

        Returns:
            A unique subscription ID that can be used to unsubscribe.
        """
        async with self._lock:
            sub_id = next(self._id_generator)
            key = (symbol, timeframe)
            self._subscriptions[key][sub_id] = queue
            self._id_to_key[sub_id] = key
            logger.info(f"New subscription (ID: {sub_id}) for {key}.")
            return sub_id

    async def unsubscribe(self, sub_id: int) -> None:
        """Unsubscribes a queue using its subscription ID.

        Args:
            sub_id: The unique ID returned by the `subscribe` method.
        """
        async with self._lock:
            if sub_id not in self._id_to_key:
                logger.warning(f"Attempted to unsubscribe with invalid ID: {sub_id}")
                return

            key = self._id_to_key.pop(sub_id)
            if key in self._subscriptions and sub_id in self._subscriptions[key]:
                del self._subscriptions[key][sub_id]
                logger.info(f"Unsubscribed ID {sub_id} from {key}.")
                # Clean up the key if no subscribers are left
                if not self._subscriptions[key]:
                    del self._subscriptions[key]
                    logger.debug(f"Removed empty subscription key: {key}.")
            else:
                logger.warning(
                    f"Subscription key {key} or ID {sub_id} not found during "
                    "unsubscribe."
                )

    async def _run(self) -> None:
        """The main processing loop that fans out messages."""
        while self._running.is_set():
            try:
                update = await self.input_queue.get()
                if update is None:  # Sentinel for shutdown
                    continue

                key = (update.symbol, update.timeframe)

                async with self._lock:
                    if key in self._subscriptions:
                        # Create a copy of the values to iterate over,
                        # making it safe from modification during iteration.
                        sub_queues = list(self._subscriptions[key].values())

                for queue in sub_queues:
                    try:
                        queue.put_nowait(update)
                    except asyncio.QueueFull:
                        logger.warning(
                            f"Subscriber queue for {key} is full. "
                            "Update was dropped. This may indicate a slow consumer."
                        )

                self.input_queue.task_done()

            except asyncio.CancelledError:
                logger.info("Publisher run loop cancelled.")
                break
            except Exception:
                logger.exception("Unexpected error in Publisher run loop.")
                await asyncio.sleep(1)
        logger.info("Publisher run loop terminated.")