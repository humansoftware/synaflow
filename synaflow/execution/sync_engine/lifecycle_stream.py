from collections.abc import Generator, Iterator
from typing import Any, Callable


class LifecycleStream:
    """Wraps a synchronous iterator to track start, items, end, and errors."""

    def __init__(
        self,
        it: Iterator[Any] | Generator[Any, Any, Any],
        on_start: Callable[[], None] | None = None,
        on_item: Callable[[Any], None] | None = None,
        on_end: Callable[[int], None] | None = None,
        on_error: Callable[[BaseException, int], None] | None = None,
    ) -> None:
        self._it = it
        self._on_start = on_start
        self._on_item = on_item
        self._on_end = on_end
        self._on_error = on_error
        self._started = False
        self._completed = False
        self._count = 0

    def __iter__(self) -> "LifecycleStream":
        return self

    def __next__(self) -> Any:
        if self._completed:
            raise StopIteration

        try:
            val = next(self._it)
            if not self._started:
                self._started = True
                if self._on_start:
                    self._on_start()
            self._count += 1
            if self._on_item:
                self._on_item(val)
            return val
        except StopIteration:
            self._completed = True
            if not self._started:
                self._started = True
                if self._on_start:
                    self._on_start()
            if self._on_end:
                self._on_end(self._count)
            raise
        except BaseException as exc:
            self._completed = True
            if not self._started:
                self._started = True
                if self._on_start:
                    self._on_start()
            if self._on_error:
                self._on_error(exc, self._count)
            raise
