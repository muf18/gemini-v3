import asyncio
from datetime import datetime, timezone

import pytest

from cryptochart.normalizer import Normalizer
from cryptochart.types import models_pb2


@pytest.fixture
def queues() -> (
    tuple["asyncio.Queue[models_pb2.PriceUpdate]", "asyncio.Queue[models_pb2.PriceUpdate]"]
):
    """Provides a tuple of input and output asyncio Queues."""
    return asyncio.Queue(), asyncio.Queue()


@pytest.fixture
def normalizer(
    queues: tuple[
        "asyncio.Queue[models_pb2.PriceUpdate]",
        "asyncio.Queue[models_pb2.PriceUpdate]",
    ]
) -> Normalizer:
    """Provides a Normalizer instance initialized with the queues."""
    return Normalizer(input_queue=queues[0], output_queue=queues[1])


@pytest.mark.asyncio
async def test_start_stop_lifecycle(normalizer: Normalizer) -> None:
    """Tests that the normalizer starts and stops its background task correctly."""
    assert normalizer._task is None
    normalizer.start()
    assert isinstance(normalizer._task, asyncio.Task)
    assert not normalizer._task.done()

    await normalizer.stop()
    assert normalizer._task is None or normalizer._task.done()
    assert not normalizer._running.is_set()


@pytest.mark.asyncio
async def test_message_processing_and_enrichment(
    normalizer: Normalizer,
    queues: tuple[
        "asyncio.Queue[models_pb2.PriceUpdate]",
        "asyncio.Queue[models_pb2.PriceUpdate]",
    ],
) -> None:
    """
    Tests that a message is correctly taken from the input queue.

    It should be enriched and put onto the output queue.
    """
    input_queue, output_queue = queues

    # Create a sample message as an adapter would
    original_update = models_pb2.PriceUpdate(
        symbol="BTC/USD",
        venue="test-exchange",
        price="60000.00",
        size="0.01",
        exchange_timestamp="2023-10-27T12:00:00Z",
    )

    normalizer.start()

    # Put the message on the input queue
    await input_queue.put(original_update)

    # Retrieve the processed message from the output queue
    processed_update = await asyncio.wait_for(output_queue.get(), timeout=1)

    await normalizer.stop()

    # --- Verification ---
    # 1. Check that the original data is preserved
    assert processed_update.symbol == original_update.symbol
    assert processed_update.venue == original_update.venue
    assert processed_update.price == original_update.price
    assert processed_update.size == original_update.size
    assert processed_update.exchange_timestamp == original_update.exchange_timestamp

    # 2. Check that the client_received_timestamp was added and is valid
    assert processed_update.client_received_timestamp is not None
    assert len(processed_update.client_received_timestamp) > 0

    # Verify the timestamp format is valid RFC3339
    try:
        parsed_ts = datetime.fromisoformat(
            processed_update.client_received_timestamp.replace("Z", "+00:00")
        )
        assert parsed_ts.tzinfo == timezone.utc
    except ValueError:
        pytest.fail(
            "client_received_timestamp is not a valid ISO 8601 / RFC3339 string"
        )


@pytest.mark.asyncio
async def test_graceful_shutdown_while_waiting(normalizer: Normalizer) -> None:
    """
    Tests that the normalizer can be stopped cleanly.

    This should work even when it's blocked waiting for an item on the input queue.
    """
    normalizer.start()

    # Give the task a moment to start and block on `input_queue.get()`
    await asyncio.sleep(0.01)
    assert normalizer._task is not None
    assert not normalizer._task.done()

    # Stop the normalizer
    await normalizer.stop()

    # The task should now be completed/cancelled
    assert normalizer._task is None or normalizer._task.done()


@pytest.mark.asyncio
async def test_multiple_messages_are_processed_in_order(
    normalizer: Normalizer,
    queues: tuple[
        "asyncio.Queue[models_pb2.PriceUpdate]",
        "asyncio.Queue[models_pb2.PriceUpdate]",
    ],
) -> None:
    """Ensures a sequence of messages is processed correctly and in order."""
    input_queue, output_queue = queues
    normalizer.start()

    messages_in = [
        models_pb2.PriceUpdate(price=str(i), sequence_number=i) for i in range(5)
    ]

    # Put all messages
    for msg in messages_in:
        await input_queue.put(msg)

    # Retrieve all messages
    messages_out = []
    for _ in range(len(messages_in)):
        msg = await asyncio.wait_for(output_queue.get(), timeout=1)
        messages_out.append(msg)

    await normalizer.stop()

    # Verify the order and enrichment
    assert len(messages_out) == len(messages_in)
    for msg_in, msg_out in zip(messages_in, messages_out, strict=True):
        assert msg_out.sequence_number == msg_in.sequence_number
        assert msg_out.price == msg_in.price
        assert len(msg_out.client_received_timestamp) > 0