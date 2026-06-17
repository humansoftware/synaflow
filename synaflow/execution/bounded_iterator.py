"""
Bounded iterator wrapper for max_in_flight stream handoff.

Provides a deque-backed adapter that limits how far ahead a producer
iterator can get relative to a downstream consumer. Used by both
sync and async executors to enforce max_in_flight semantics.
"""

from collections import deque
from collections.abc import Iterator


class BoundedIterator(Iterator):
    """Wraps a source iterator with bounded prefetch capacity.

    The wrapper reads up to *maxsize* items from *source* into an internal
    deque.  When the consumer pulls an item, a slot is freed and the wrapper
    may pull one more from the source.

    max_in_flight=1 degenerates to strict lockstep: produce one, consume one.
    """

    def __init__(self, source: Iterator, maxsize: int) -> None:
        if maxsize < 1:
            raise ValueError(f"maxsize must be >= 1, got {maxsize}")
        self._source = source
        self._maxsize = maxsize
        self._buffer: deque = deque()
        self._exhausted = False

    def __iter__(self):
        return self

    def __next__(self):
        if not self._buffer:
            self._fill()

        if not self._buffer:
            raise StopIteration

        return self._buffer.popleft()

    def _fill(self) -> None:
        """Pull from source until buffer reaches maxsize or source ends."""
        while len(self._buffer) < self._maxsize and not self._exhausted:
            try:
                item = next(self._source)
            except StopIteration:
                self._exhausted = True
                break
            self._buffer.append(item)
