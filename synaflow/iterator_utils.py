from typing import Any, Callable, Iterable


class InterleavedIterator:
    """
    An iterator that yields items from a source, while simultaneously
    executing a set of callbacks on each item.

    This is useful for lockstep processing where eager consumers must execute
    on items before lazy downstream consumers iterate over them.
    """

    def __init__(self, source: Iterable[Any], callbacks: list[Callable[[Any], None]]):
        self.source = iter(source)
        self.callbacks = callbacks

    def __next__(self):
        item = next(self.source)
        for callback in self.callbacks:
            callback(item)
        return item

    def __iter__(self):
        return self
