import pytest

from cryptochart.utils.ringbuffer import RingBuffer


def test_initialization() -> None:
    """Tests the constructor and initial state of the RingBuffer."""
    buf = RingBuffer[int](5)
    assert buf.capacity == 5
    assert len(buf) == 0
    assert not buf.is_full

    with pytest.raises(ValueError, match="Capacity must be a positive integer."):
        RingBuffer[int](0)
    with pytest.raises(ValueError, match="Capacity must be a positive integer."):
        RingBuffer[int](-1)
    with pytest.raises(ValueError, match="Capacity must be a positive integer."):
        RingBuffer[float](2.5)  # type: ignore[arg-type]


def test_append_and_len() -> None:
    """Tests appending elements and tracking the length."""
    buf = RingBuffer[int](3)
    buf.append(10)
    assert len(buf) == 1
    assert list(buf) == [10]

    buf.append(20)
    assert len(buf) == 2
    assert list(buf) == [10, 20]

    buf.append(30)
    assert len(buf) == 3
    assert list(buf) == [10, 20, 30]


def test_is_full() -> None:
    """Tests the is_full property."""
    buf = RingBuffer[str](2)
    assert not buf.is_full
    buf.append("a")
    assert not buf.is_full
    buf.append("b")
    assert buf.is_full
    buf.append("c")
    assert buf.is_full


def test_overflow_behavior_on_append() -> None:
    """Tests that the buffer correctly discards the oldest element when full."""
    buf = RingBuffer[int](3)
    buf.append(1)
    buf.append(2)
    buf.append(3)
    assert list(buf) == [1, 2, 3]

    # This append should push '1' out
    buf.append(4)
    assert len(buf) == 3
    assert list(buf) == [2, 3, 4]
    assert buf.is_full

    # This append should push '2' out
    buf.append(5)
    assert len(buf) == 3
    assert list(buf) == [3, 4, 5]


def test_extend() -> None:
    """Tests extending the buffer with an iterable."""
    buf = RingBuffer[int](5)
    buf.extend([1, 2, 3])
    assert len(buf) == 3
    assert list(buf) == [1, 2, 3]

    buf.extend([4, 5])
    assert len(buf) == 5
    assert list(buf) == [1, 2, 3, 4, 5]
    assert buf.is_full


def test_overflow_behavior_on_extend() -> None:
    """Tests overflow when extending the buffer."""
    buf = RingBuffer[int](3)
    buf.extend([1, 2, 3, 4, 5])
    assert len(buf) == 3
    assert list(buf) == [3, 4, 5]
    assert buf.is_full

    buf.clear()
    buf.append(1)
    buf.extend([2, 3, 4, 5])
    assert len(buf) == 3
    assert list(buf) == [3, 4, 5]


def test_pop_and_popleft() -> None:
    """Tests removing elements from both ends."""
    buf = RingBuffer[str](4)
    buf.extend(["a", "b", "c", "d"])

    item_d = buf.pop()
    assert item_d == "d"
    assert list(buf) == ["a", "b", "c"]
    assert len(buf) == 3

    item_a = buf.popleft()
    assert item_a == "a"
    assert list(buf) == ["b", "c"]
    assert len(buf) == 2

    buf.pop()
    buf.pop()
    assert len(buf) == 0

    # Test popping from an empty buffer
    with pytest.raises(IndexError):
        buf.pop()
    with pytest.raises(IndexError):
        buf.popleft()


def test_indexing() -> None:
    """Tests accessing elements by index."""
    buf = RingBuffer[int](5)
    buf.extend([10, 20, 30, 40])

    assert buf[0] == 10
    assert buf[3] == 40
    assert buf[-1] == 40
    assert buf[-2] == 30

    with pytest.raises(IndexError):
        _ = buf[4]
    with pytest.raises(IndexError):
        _ = buf[-5]


def test_clear() -> None:
    """Tests clearing all elements from the buffer."""
    buf = RingBuffer[int](5)
    buf.extend([1, 2, 3, 4, 5])
    assert buf.is_full
    assert len(buf) == 5

    buf.clear()
    assert len(buf) == 0
    assert not buf.is_full
    assert list(buf) == []


def test_iteration() -> None:
    """Tests iterating over the buffer."""
    items = [10, 20, 30]
    buf = RingBuffer[int](3)
    buf.extend(items)

    # Test __iter__
    collected = list(buf)
    assert collected == items

    # Test that it works after overflow
    buf.append(40)  # Now contains [20, 30, 40]
    collected_after_overflow = list(buf)
    assert collected_after_overflow == [20, 30, 40]


def test_repr() -> None:
    """Tests the string representation of the buffer."""
    buf = RingBuffer[int](5)
    buf.extend([1, 2])
    expected_repr = "RingBuffer(capacity=5, size=2, data=[1, 2])"
    assert repr(buf) == expected_repr

    buf.extend([3, 4, 5])
    expected_repr_full = "RingBuffer(capacity=5, size=5, data=[1, 2, 3, 4, 5])"
    assert repr(buf) == expected_repr_full