import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any

from synaflow.core.type_compatibility import is_iterable_type, is_scalar

from .constants import EOF_MARKER
from .iterator_utils import async_list, queue_to_async_gen
from .topology import AsyncTeeWrapper


class AsyncDependencyResolver:
    def __init__(self, context: dict[str, Any]):
        self.context = context

    async def resolve_node_arguments(
        self, consumer_name: str, node: dict
    ) -> dict[str, Any]:
        sig = inspect.signature(node["fn"])
        deps = node.get("deps", {})
        kwargs: dict[str, Any] = {}

        for param_name in sig.parameters:
            if param_name in self.context:
                value = self.context.get(param_name)

                if isinstance(value, AsyncTeeWrapper):
                    queue = value.queues[consumer_name]
                    consumer_type = deps.get(param_name)
                    value = await self._adapt_queue_to_type(queue, consumer_type)
                else:
                    consumer_type = deps.get(param_name)
                    if consumer_type:
                        value = self._adapt_scalar_to_type(value, consumer_type)

                kwargs[param_name] = value

        return kwargs

    async def _adapt_queue_to_type(
        self, queue: asyncio.Queue, consumer_type: Any
    ) -> Any:
        if self._is_lazy_async_iterator_type(consumer_type):
            return queue_to_async_gen(queue)

        origin = getattr(consumer_type, "__origin__", consumer_type)
        if origin in (list, set, tuple) or consumer_type in (list, set, tuple):
            items = await async_list(queue_to_async_gen(queue))
            if origin is set or consumer_type is set:
                return set(items)
            elif origin is tuple or consumer_type is tuple:
                return tuple(items)
            return items

        return queue_to_async_gen(queue)

    def _adapt_scalar_to_type(self, value: Any, consumer_type: Any) -> Any:
        origin = getattr(consumer_type, "__origin__", consumer_type)
        if (
            origin in (list, set, tuple)
            or consumer_type in (list, set, tuple)
            or self._is_lazy_async_iterator_type(consumer_type)
        ):
            if not isinstance(value, (list, set, tuple)):
                value = [value]

            if origin is set or consumer_type is set:
                value = set(value)
            elif origin is tuple or consumer_type is tuple:
                value = tuple(value)

            if self._is_lazy_async_iterator_type(consumer_type):

                async def as_gen():
                    for x in value:
                        yield x

                return as_gen()
        return value

    def _is_lazy_async_iterator_type(self, tp: Any) -> bool:
        if tp in (AsyncIterator, AsyncGenerator):
            return True
        origin = getattr(tp, "__origin__", tp)
        return origin in (AsyncIterator, AsyncGenerator)
