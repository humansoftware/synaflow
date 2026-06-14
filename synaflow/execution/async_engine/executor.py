import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException, StepExecutionError
from synaflow.core.types import (
    ErrorInterceptorContext,
    ErrorMaterializeContext,
    MaterializeContext,
    OnError,
)


def _output_key(dag: Dag, producer: str, consumer: str) -> str:
    if len(dag.consumers_of(producer)) > 1:
        return f"{producer}__{consumer}"
    return producer


from .constants import EOF_MARKER
from .iterator_utils import async_list, queue_to_async_gen

# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


async def _apply_materializer(
    dag: Dag,
    step_name: str,
    value: Any,
    consumer_type: Any = None,
    inputs: dict[str, Any] | None = None,
) -> Any:
    node = dag[step_name]
    mat = node.get("materializer")
    if mat is None:
        if isinstance(value, (AsyncIterator, AsyncGenerator)):
            return await async_list(value)
        return value
    sig = inspect.signature(mat)
    if (
        len(sig.parameters) > 1
        or "ctx" in sig.parameters
        or "context" in sig.parameters
    ):
        mat = mat(
            MaterializeContext(
                pipeline_name=dag.name,
                dataset_name=step_name,
                item_type=node.get("output"),
                consumer_type=consumer_type,
            )
        )
    if inspect.iscoroutinefunction(mat):
        return await mat(value)
    if isinstance(value, (AsyncIterator, AsyncGenerator)):
        return await async_list(value)
    return mat(value)


def _handle_error(dag: Dag, step_name: str, exc: BaseException) -> None:
    factory = getattr(dag, "error_materializer_factory", None)
    if factory is None:
        return

    handler = factory(
        ErrorMaterializeContext(
            pipeline_name=dag.name,
            dataset_name=step_name,
            exception_type=type(exc),
        )
    )
    if callable(handler):
        handler(exc)


async def _trigger_interceptors(
    dag: Dag, step_name: str, exc: Exception, inputs: dict[str, Any] | None
) -> None:
    node = dag.steps.get(step_name)
    if not node:
        return

    ctx = ErrorInterceptorContext(
        pipeline_name=dag.name,
        step_name=step_name,
        inputs=inputs or {},
    )

    for interceptor in getattr(node, "error_interceptors", []):
        try:
            if inspect.iscoroutinefunction(interceptor):
                await interceptor(exc, ctx)
            else:
                interceptor(exc, ctx)
        except Exception as interceptor_exc:
            import logging

            logging.getLogger("synaflow").warning(
                f"Error in interceptor {interceptor.__name__ if hasattr(interceptor, '__name__') else interceptor} for step '{step_name}': {interceptor_exc}"
            )


async def _pump_iterator(
    name: str,
    iterator: Any,
    queues: dict[str, asyncio.Queue],
    on_error: Any,
    dag: Dag | None = None,
    materialize_before_enqueue: bool = False,
    consumer_type: Any = None,
    inputs: dict[str, Any] | None = None,
) -> None:
    try:
        safe = _safe_iterate(name, iterator)
        if materialize_before_enqueue:
            items = await _apply_materializer(
                dag, name, safe, consumer_type=consumer_type, inputs=inputs
            )
            async for item in _safe_iterate(name, items):
                for q in queues.values():
                    await q.put(item)
        else:
            async for item in safe:
                for q in queues.values():
                    await q.put(item)
    except StepExecutionError as e:
        exc = e.__cause__ or e
        await _trigger_interceptors(dag, name, exc, inputs)
        _handle_error(dag, name, exc)
        if on_error == OnError.STOP:
            for q in queues.values():
                await q.put(PipelineStopException(step_name=name))
            raise PipelineStopException(step_name=name) from e
    finally:
        for q in queues.values():
            await q.put(EOF_MARKER)


async def _pump_observer(name: str, queue: asyncio.Queue, observer: Any) -> None:
    items = []
    while True:
        item = await queue.get()
        if item is EOF_MARKER:
            break
        if isinstance(item, Exception):
            break
        items.append(item)
    observer(name, items)


async def _safe_iterate(name: str, iterable: Any):
    if isinstance(iterable, (AsyncIterator, AsyncGenerator)):
        while True:
            try:
                item = await anext(iterable)
                yield item
            except StopAsyncIteration:
                break
            except Exception as e:
                if isinstance(e, StepExecutionError):
                    raise e
                raise StepExecutionError(f"Error iterating step '{name}'") from e
    else:
        iterator = iter(iterable)
        while True:
            try:
                item = next(iterator)
                yield item
            except StopIteration:
                break
            except Exception as e:
                if isinstance(e, StepExecutionError):
                    raise e
                raise StepExecutionError(f"Error iterating step '{name}'") from e


async def _resolve_queue(
    dag: Dag, producer: str, queue: asyncio.Queue, consumer_type: Any
) -> Any:
    if consumer_type in (AsyncIterator, AsyncGenerator):
        return queue_to_async_gen(queue)
    origin = getattr(consumer_type, "__origin__", consumer_type)
    if origin in (AsyncIterator, AsyncGenerator):
        return queue_to_async_gen(queue)
    return await _apply_materializer(
        dag, producer, queue_to_async_gen(queue), consumer_type=consumer_type
    )


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class AsyncPipelineExecutor:
    def __init__(self, dag: Dag, *, step_output_observers: list = None):
        self.dag = dag
        self.outputs = {}
        self._pump_tasks: list[asyncio.Task] = []
        self._step_output_observers = step_output_observers or []
        self._step_inputs = {}

    async def execute(self, params: Any) -> None:
        for field, value in params._asdict().items():
            self.outputs[field] = value

        try:
            for level in self.dag.get_execution_levels():
                tasks = [self._run_step(name) for name in level]
                if tasks:
                    await asyncio.gather(*tasks)

            if self._pump_tasks:
                await asyncio.gather(*self._pump_tasks)
        except PipelineStopException:
            raise

    async def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        unrolled = self.dag.each_inputs(step_name)
        arguments = await self._build_arguments(step_name, node, unrolled)
        self._step_inputs[step_name] = arguments

        try:
            if unrolled:
                output = await self._unroll_step(step_name, node, arguments, unrolled)
            else:
                output = await self._call_fn(node.fn, arguments)

            if not step_name.startswith("_"):
                await self._publish_output(step_name, output, node)
        except PipelineStopException:
            raise
        except Exception as exc:
            await _trigger_interceptors(self.dag, step_name, exc, arguments)
            _handle_error(self.dag, step_name, exc)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc

    async def _call_fn(self, fn: Any, kwargs: dict) -> Any:
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        return fn(**kwargs)

    async def _unroll_step(self, step_name, node, base_args, unrolled):
        queues = {}
        for dep in unrolled:
            key = _output_key(self.dag, dep, step_name)
            value = self.outputs.get(key, self.outputs.get(dep))
            if isinstance(value, asyncio.Queue):
                queues[dep] = value
            else:
                q = asyncio.Queue()
                if isinstance(value, (list, tuple, set)):
                    for item in value:
                        await q.put(item)
                elif value is not None:
                    await q.put(value)
                await q.put(EOF_MARKER)
                queues[dep] = q
        completed = set()

        async def generate():
            while len(completed) < len(unrolled):
                item_args = dict(base_args)
                for dep in unrolled:
                    if dep in completed:
                        item_args[dep] = None
                        continue

                    item = await queues[dep].get()
                    if item is EOF_MARKER:
                        completed.add(dep)
                        item_args[dep] = None
                    elif isinstance(item, Exception):
                        raise item
                    else:
                        item_args[dep] = item
                if len(completed) == len(unrolled):
                    break
                try:
                    yield await self._call_fn(node.fn, item_args)
                except Exception as exc:
                    await _trigger_interceptors(self.dag, step_name, exc, item_args)
                    _handle_error(self.dag, step_name, exc)
                    if node.on_error == OnError.STOP:
                        raise PipelineStopException(
                            step_name=step_name, cause=exc
                        ) from exc

        if self._is_terminal(step_name):
            async for _ in generate():
                pass
            return None
        return generate()

    def _is_terminal(self, step_name):
        return step_name.startswith("_") or not self.dag.consumers_of(step_name)

    async def _build_arguments(self, consumer, node, unrolled):
        args = {}
        for dep_name in node.deps:
            key = _output_key(self.dag, dep_name, consumer)
            value = self.outputs.get(key, self.outputs.get(dep_name))
            if isinstance(value, asyncio.Queue) and dep_name not in unrolled:
                dep_type = node.deps.get(dep_name)
                value = await _resolve_queue(self.dag, dep_name, value, dep_type)
            args[dep_name] = value
        return args

    def _notify_observers(self, step_name, output):
        for observer in self._step_output_observers:
            observer(step_name, output)

    async def _publish_output(self, step_name, output, node):
        inputs = self._step_inputs.get(step_name)
        if isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator)):
            consumers = self.dag.consumers_of(step_name)
            if consumers:
                queues = {c: asyncio.Queue(maxsize=100) for c in consumers}
                for c, q in queues.items():
                    self.outputs[_output_key(self.dag, step_name, c)] = q
                if self._step_output_observers:
                    for observer in self._step_output_observers:
                        obs_queue = asyncio.Queue(maxsize=100)
                        queues[f"__obs"] = obs_queue
                        self._pump_tasks.append(
                            asyncio.create_task(
                                _pump_observer(step_name, obs_queue, observer)
                            )
                        )
                materialize_before_enqueue = len(consumers) == 1 and (
                    node.on_error == OnError.STOP or node.force_materialize
                )
                consumer_type = None
                if materialize_before_enqueue:
                    consumer_type = self.dag[consumers[0]].deps.get(step_name)
                task = asyncio.create_task(
                    _pump_iterator(
                        step_name,
                        output,
                        queues,
                        node.on_error,
                        dag=self.dag,
                        materialize_before_enqueue=materialize_before_enqueue,
                        consumer_type=consumer_type,
                        inputs=inputs,
                    )
                )
                self._pump_tasks.append(task)
                return
            else:
                self._notify_observers(step_name, output)
        elif self.dag.needs_materialize(step_name):
            output = await _apply_materializer(
                self.dag,
                step_name,
                output,
                consumer_type=node.get("output"),
                inputs=inputs,
            )
        self.outputs[step_name] = output
        self._notify_observers(step_name, output)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def async_run(pipeline: PipelineDef, params: Any) -> None:
    if getattr(pipeline, "requires_sync_runner", False):
        raise RuntimeError(
            "This pipeline contains synchronous streams (Iterator)."
            " It must be executed with run() or migrated to AsyncIterator."
        )
    await AsyncPipelineExecutor(pipeline.dag).execute(params)
