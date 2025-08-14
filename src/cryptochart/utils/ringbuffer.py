from collections import deque
from collections.abc import Iterable, Sized
from typing import Generic, TypeVar

T = TypeVar("T")


class RingBuffer(Sized, Generic[T]):
    """A high-performance, fixed-capacity ring buffer.

    This implementation uses `collections.deque` with a `maxlen` for
    efficient appends and pops from both ends. When the buffer is full,
    adding a new element automatically discards the oldest element.

    It is generic, allowing it to store any type of object.
    """

    def __init__(self, capacity: int) -> None:
        """Initializes the RingBuffer.

        Args:
            capacity: The maximum number of elements the buffer can hold.

        Raises:
            ValueError: If the capacity is not a positive integer.
        """
        if not isinstance(capacity, int) or capacity <= 0:
            err_msg = "Capacity must be a positive integer."
            raise ValueError(err_msg)
        self._capacity = capacity
        self._data: deque[T] = deque(maxlen=capacity)

    @property
    def capacity(self) -> int:
        """The maximum number of elements the buffer can hold."""
        return self._capacity

    @property
    def is_full(self) -> bool:
        """Returns True if the buffer has reached its maximum capacity."""
        return len(self._data) == self._capacity

    def append(self, item: T) -> None:
        """Adds an element to the right side of the buffer.

        If the buffer is full, the oldest element (from the left) is
        automatically discarded.

        Args:
            item: The element to add.
        """
        self._data.append(item)

    def extend(self, items: Iterable[T]) -> None:
        """Extends the buffer by appending elements from the iterable.

        If the buffer overflows, the oldest elements are discarded until
        the buffer is back within its capacity, containing only the newest
        elements from the iterable.

        Args:
            items: An iterable of elements to add.
        """
        self._data.extend(items)

    def pop(self) -> T:
        """Removes and returns the rightmost element.

        Raises:
            IndexError: If the buffer is empty.
        """
        return self._data.pop()

    def popleft(self) -> T:
        """Removes and returns the leftmost element.

        Raises:
            IndexError: If the buffer is empty.
        """
        return self._data.popleft()

    def clear(self) -> None:
        """Removes all elements from the buffer."""
        self._data.clear()

    def __len__(self) -> int:
        """Returns the current number of elements in the buffer."""
        return len(self._data)

    def __getitem__(self, index: int) -> T:
        """Returns the element at the specified index.

        Supports standard list-like indexing, including negative indices.

        Args:
            index: The index of the element to retrieve.

        Raises:
            IndexError: If the index is out of range.
        """
        return self._data[index]

    def __iter__(self) -> "iter[T]":
        """Returns an iterator over the elements in the buffer."""
        return iter(self._data)

    def __repr__(self) -> str:
        """Returns a developer-friendly representation of the buffer."""
        return (
            f"RingBuffer(capacity={self.capacity}, size={len(self)}, "
            f"data={list(self._data)})"
        )