import asyncio
import inspect
from typing import Any, Callable

from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException, StepExecutionError
from synaflow.core.type_compatibility import is_iterable_type, is_scalar
from synaflow.core.types import OnError

from .constants import EOF_MARKER
from .dependencies import AsyncDependencyResolver
from .topology import AsyncStreamManager, AsyncTeeWrapper


class AsyncNodeRunner:
    def __init__(
        self,
        pipeline: PipelineDef,
        context: dict[str, Any],
        resolver: AsyncDependencyResolver,
        stream_manager: AsyncStreamManager,
    ):
        self.pipeline = pipeline
        self.dag = pipeline.dag
        self.context = context
        self.resolver = resolver
        self.stream_manager = stream_manager

    async def execute_node(self, name: str) -> None:
        node = self.dag.get(name)
        if not node or node.get("fn") is None:
            return

        fn = node["fn"]
        deps = node.get("deps", {})

        is_each = False
        first_dep_name = None
        if deps:
            first_dep_name = next(iter(deps))
            producer = self.dag.get(first_dep_name, {})
            producer_output = producer.get("output")
            if is_iterable_type(producer_output) and is_scalar(deps[first_dep_name]):
                is_each = True

        try:
            if is_each:
                await self._execute_each_node(name, fn, node, first_dep_name)
            else:
                await self._execute_standard_node(name, fn, node)
        except StepExecutionError:
            if node.get("on_error") == OnError.STOP:
                raise PipelineStopException()

    async def _execute_each_node(
        self, name: str, fn: Callable, node: dict, first_dep_name: str
    ) -> None:
        kwargs = await self.resolver.resolve_node_arguments(name, node)

        wrapper = self.context.get(first_dep_name)
        if isinstance(wrapper, AsyncTeeWrapper):
            queue = wrapper.queues[name]
        else:
            queue = asyncio.Queue()
            if isinstance(wrapper, (list, tuple, set)):
                for w in wrapper:
                    await queue.put(w)
            else:
                await queue.put(wrapper)
            await queue.put(EOF_MARKER)

        async def each_gen():
            while True:
                item = await queue.get()
                if isinstance(item, Exception):
                    raise item
                if item is EOF_MARKER:
                    break
                item_kwargs = dict(kwargs)
                item_kwargs[first_dep_name] = item
                try:
                    if inspect.iscoroutinefunction(fn):
                        yield await fn(**item_kwargs)
                    else:
                        yield fn(**item_kwargs)
                except Exception as e:
                    if node.get("on_error") == OnError.STOP:
                        raise PipelineStopException() from e
                    continue

        consumers = [
            c for c, cnode in self.dag.items() if name in cnode.get("deps", {})
        ]
        is_sink = name.startswith("_") or len(consumers) == 0

        gen = each_gen()
        if is_sink:
            async for _ in gen:
                pass
        else:
            needs_materialize = node.get("needs_materialize", False)
            self.stream_manager.store_output(name, gen, needs_materialize)

    async def _execute_standard_node(self, name: str, fn: Callable, node: dict) -> None:
        kwargs = await self.resolver.resolve_node_arguments(name, node)

        try:
            if inspect.iscoroutinefunction(fn):
                output = await fn(**kwargs)
            else:
                output = fn(**kwargs)
        except Exception as e:
            raise StepExecutionError(f"Error executing step '{name}'") from e

        try:
            if name and not name.startswith("_"):
                needs_materialize = node.get("needs_materialize", False)
                self.stream_manager.store_output(name, output, needs_materialize)
        except StepExecutionError:
            if node.get("on_error") == OnError.STOP:
                raise PipelineStopException()
