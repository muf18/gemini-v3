import asyncio

from loguru import logger

from cryptochart.types import models_pb2
from cryptochart.utils.time import get_current_rfc3339_timestamp


class Normalizer:
    """A central processing component between exchange adapters and the aggregator.

    The Normalizer's primary responsibilities are:
    1.  Receiving normalized `PriceUpdate` messages from various adapters.
    2.  Enriching these messages with client-side metadata, such as the
        time the message was received by the application.
    3.  Acting as a buffer and a single entry point into the core
        data processing pipeline (the Aggregator).
    4.  Providing a clean decoupling point in the architecture.
    """

    def __init__(
        self,
        input_queue: "asyncio.Queue[models_pb2.PriceUpdate]",
        output_queue: "asyncio.Queue[models_pb2.PriceUpdate]",
    ) -> None:
        """Initializes the Normalizer.

        Args:
            input_queue: The queue from which to receive messages from adapters.
            output_queue: The queue to which enriched messages will be sent for
                aggregation.
        """
        self.input_queue = input_queue
        self.output_queue = output_queue
        self._task: asyncio.Task[None] | None = None
        self._running = asyncio.Event()

    def start(self) -> None:
        """Starts the normalizer's processing loop in a background task."""
        if self._task is None or self._task.done():
            self._running.set()
            self._task = asyncio.create_task(self._run())
            logger.info("Normalizer started.")
        else:
            logger.warning("Normalizer is already running.")

    async def stop(self) -> None:
        """Stops the normalizer's processing loop gracefully."""
        if not self._running.is_set():
            logger.warning("Normalizer is not running.")
            return

        logger.info("Stopping Normalizer...")
        self._running.clear()
        if self._task:
            try:
                # Unblock the queue.get() call to allow the task to exit
                self.input_queue.put_nowait(None)  # type: ignore[arg-type]
                self._task.cancel()
                await self._task
            except asyncio.CancelledError:
                pass  # Expected cancellation
            finally:
                self._task = None
        logger.info("Normalizer stopped.")

    async def _run(self) -> None:
        """The main processing loop."""
        logger.info("Normalizer run loop initiated.")
        while self._running.is_set():
            try:
                # Wait for a message from any of the adapters
                update = await self.input_queue.get()

                if update is None:  # Sentinel value for shutdown
                    continue

                # --- Enrichment Step ---
                # Add the client-side received timestamp. This is crucial for
                # measuring internal latency.
                update.client_received_timestamp = get_current_rfc3339_timestamp()

                # In the future, other normalization or validation steps could go here.
                # For example, validating numerical string formats or checking for
                # stale data based on the exchange_timestamp.

                # Forward the enriched message to the aggregator
                await self.output_queue.put(update)

                # Acknowledge that the task from the input queue is done
                self.input_queue.task_done()

            except asyncio.CancelledError:
                logger.info("Normalizer run loop cancelled.")
                break
            except Exception:
                logger.exception(
                    "An unexpected error occurred in the Normalizer run loop."
                )
                # Avoid a tight loop on continuous errors
                await asyncio.sleep(1)
        logger.info("Normalizer run loop terminated.")