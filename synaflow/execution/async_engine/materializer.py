from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Iterable
from typing import Any, Callable, Protocol

from synaflow.core.types import MaterializeContext

AsyncMaterializer = Callable[
    [AsyncIterator[Any]], Awaitable[Iterable[Any]] | AsyncIterable[Any]
]


class AsyncMaterializerFactory(Protocol):
    def __call__(self, ctx: MaterializeContext) -> AsyncMaterializer:
        ...
