from collections.abc import AsyncIterable, AsyncIterator, Awaitable
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Protocol


@dataclass
class MaterializeContext:
    pipeline_name: str
    dataset_name: str
    item_type: Any


SyncMaterializer = Callable[[Iterator[Any]], Iterable[Any]]

AsyncMaterializer = Callable[
    [AsyncIterator[Any]],
    Awaitable[AsyncIterable[Any]] | AsyncIterable[Any] | Awaitable[Iterable[Any]],
]


class SyncMaterializerFactory(Protocol):
    def __call__(self, ctx: MaterializeContext) -> SyncMaterializer:
        ...


class AsyncMaterializerFactory(Protocol):
    def __call__(self, ctx: MaterializeContext) -> AsyncMaterializer:
        ...
