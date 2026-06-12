from collections.abc import Iterable, Iterator
from typing import Any, Callable, Protocol

from synaflow.core.types import MaterializeContext

SyncMaterializer = Callable[[Iterator[Any]], Iterable[Any]]


class SyncMaterializerFactory(Protocol):
    def __call__(self, ctx: MaterializeContext) -> SyncMaterializer:
        ...
