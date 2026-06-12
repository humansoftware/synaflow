import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from typing import Any, Callable, NamedTuple

from synaflow.core.pipeline import PipelineDef
from synaflow.core.type_compatibility import is_iterable_type, is_scalar
from synaflow.core.types import OnError
from synaflow.execution.sync_engine.executor import PipelineStopException
from synaflow.execution.sync_engine.materializer import SyncMaterializerFactory

EOF_MARKER = object()


class AsyncTeeWrapper:
    def __init__(self, queues: dict[str, asyncio.Queue]):
        self.queues = queues


async def _queue_to_async_gen(queue: asyncio.Queue):
    while True:
        item = await queue.get()
        if isinstance(item, Exception):
            raise item
        if item is EOF_MARKER:
            break
        yield item


async def async_list(gen):
    return [x async for x in gen]


class AsyncPipelineExecutor:
    """Executes a compiled Directed Acyclic Graph (DAG) asynchronously."""

    def __init__(self, pipeline: PipelineDef):
        self.pipeline = pipeline
        self.dag = pipeline._dag
        self.context: dict[str, Any] = {}
        self.pump_tasks: list[asyncio.Task] = []

    async def _apply_materializer(self, name: str, iterator: Any) -> Any:
        node = self.dag.get(name)
        step_def = None
        if node and node.get("fn"):
            step_def = next((s for s in self.pipeline.steps if s.name == name), None)

        mat = getattr(step_def, "materializer", None) if step_def else None

        if mat is None:
            mat = self.pipeline.default_materializer_factory

        if mat is None:
            return await async_list(iterator)

        sig = inspect.signature(mat)
        if (
            len(sig.parameters) > 1
            or "ctx" in sig.parameters
            or "context" in sig.parameters
        ):
            ctx = MaterializeContext(
                pipeline_name=self.pipeline.name,
                dataset_name=name,
                item_type=node.get("output") if node else Any,
            )
            mat = mat(ctx)

        if inspect.iscoroutinefunction(mat):
            return await mat(iterator)
        return mat(iterator)

    async def execute(self, params: Any) -> None:
        self._initialize_context_with_params(params)

        try:
            levels = self.pipeline.get_execution_levels()
            for level in levels:
                tasks = [self._execute_node(name) for name in level]
                if tasks:
                    await asyncio.gather(*tasks)

            if self.pump_tasks:
                await asyncio.gather(*self.pump_tasks)
        except PipelineStopException:
            pass

    def _initialize_context_with_params(self, params: Any) -> None:
        for field, value in params._asdict().items():
            needs_materialize = self.dag.get(field, {}).get("needs_materialize", False)
            self._store_output(field, value, needs_materialize)

    def _store_output(
        self, name: str, value: Any, needs_materialize: bool = False
    ) -> None:
        consumers = [
            c for c, cnode in self.dag.items() if name in cnode.get("deps", {})
        ]

        if isinstance(value, (Iterator, Generator, AsyncIterator, AsyncGenerator)):
            if consumers:
                queues = {c: asyncio.Queue(maxsize=100) for c in consumers}
                self.context[name] = AsyncTeeWrapper(queues)
                node = self.dag.get(name, {})
                on_error = node.get("on_error")
                task = asyncio.create_task(
                    self._pump_iterator(
                        name, value, queues, needs_materialize, on_error
                    )
                )
                self.pump_tasks.append(task)
            else:
                self.context[name] = value
        else:
            self.context[name] = value

    async def _pump_iterator(
        self,
        name: str,
        iterator: Any,
        queues: dict[str, asyncio.Queue],
        needs_materialize: bool = False,
        on_error: Any = None,
    ) -> None:
        try:
            if needs_materialize:
                items = await self._apply_materializer(name, iterator)
                if isinstance(items, (AsyncIterator, AsyncGenerator)):
                    async for item in items:
                        for q in queues.values():
                            await q.put(item)
                else:
                    for item in items:
                        for q in queues.values():
                            await q.put(item)
            else:
                if isinstance(iterator, (AsyncIterator, AsyncGenerator)):
                    async for item in iterator:
                        for q in queues.values():
                            await q.put(item)
                else:
                    for item in iterator:
                        for q in queues.values():
                            await q.put(item)
        except Exception as e:
            if on_error == OnError.STOP:
                for q in queues.values():
                    await q.put(PipelineStopException())
                raise PipelineStopException() from e
            for q in queues.values():
                await q.put(e)
        finally:
            for q in queues.values():
                await q.put(EOF_MARKER)

    async def _execute_node(self, name: str) -> None:
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
        except Exception:
            if node.get("on_error") == OnError.STOP:
                raise PipelineStopException()

    async def _execute_each_node(
        self, name: str, fn: Callable, node: dict, first_dep_name: str
    ) -> None:
        kwargs = await self._resolve_node_arguments(name, node)

        wrapper = self.context.get(first_dep_name)
        if isinstance(wrapper, AsyncTeeWrapper):
            queue = wrapper.queues[name]
        else:
            # Create a dummy queue for iterable fallback
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
                except Exception:
                    if node.get("on_error") == OnError.STOP:
                        raise PipelineStopException()

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
            self._store_output(name, gen, needs_materialize)

    async def _execute_standard_node(self, name: str, fn: Callable, node: dict) -> None:
        kwargs = await self._resolve_node_arguments(name, node)

        try:
            if inspect.iscoroutinefunction(fn):
                output = await fn(**kwargs)
            else:
                output = fn(**kwargs)

            if name and not name.startswith("_"):
                needs_materialize = node.get("needs_materialize", False)
                self._store_output(name, output, needs_materialize)
        except Exception:
            if node.get("on_error") == OnError.STOP:
                raise PipelineStopException()

    async def _resolve_node_arguments(
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
            return _queue_to_async_gen(queue)

        origin = getattr(consumer_type, "__origin__", consumer_type)
        if origin in (list, set, tuple) or consumer_type in (list, set, tuple):
            items = await async_list(_queue_to_async_gen(queue))
            if origin is set or consumer_type is set:
                return set(items)
            elif origin is tuple or consumer_type is tuple:
                return tuple(items)
            return items

        return _queue_to_async_gen(queue)

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


async def async_run(pipeline: PipelineDef, params: Any) -> None:
    """Executes a pipeline definition asynchronously."""
    if getattr(pipeline, "requires_sync_runner", False):
        raise RuntimeError(
            "This pipeline contains synchronous streams (Iterator). It must be executed with run() or migrated to AsyncIterator."
        )

    executor = AsyncPipelineExecutor(pipeline)
    await executor.execute(params)
