import asyncio
import inspect
import logging
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException, StepExecutionError
from synaflow.core.types import (
    ErrorMaterializeContext,
    MaterializeContext,
    OnError,
)
from synaflow.core.observers import (
    PipelineEvent,
    StepEvent,
    MaterializationEvent,
    PipelineStartedContext,
    PipelineCompletedContext,
    PipelineFailedContext,
    StepStartedContext,
    StepCompletedContext,
    StepFailedContext,
    MaterializationStartedContext,
    MaterializationCompletedContext,
    MaterializationFailedContext,
)

from .constants import EOF_MARKER
from .iterator_utils import queue_to_async_gen


async def _dispatch_observers(observers: list, event: Any, context: Any) -> None:
    log = logging.getLogger("synaflow")
    for obs in observers:
        if obs.event == event:
            try:
                res = obs.handler(context)
                if inspect.isawaitable(res):
                    await res
            except Exception as exc:
                log.warning(
                    "Observer failed for event %s: %s", event, exc, exc_info=True
                )


class StepState:
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.completed_all_inputs = False


async def _wrap_step_async_iterator(
    iterator: Any,
    step_state: StepState,
    step_name: str,
    node: Any,
    pipeline_name: str,
):
    try:
        if isinstance(iterator, (AsyncIterator, AsyncGenerator)):
            while True:
                try:
                    item = await anext(iterator)
                    yield item
                except StopAsyncIteration:
                    break
        else:
            # Synchronous iterator running inside async pipeline
            it = iter(iterator)
            while True:
                try:
                    item = next(it)
                    yield item
                except StopIteration:
                    break
        step_state.success_count = 1
        step_state.completed_all_inputs = True
        ctx = StepCompletedContext(
            pipeline_name=pipeline_name,
            event=StepEvent.COMPLETED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
            success_count=step_state.success_count,
            error_count=step_state.error_count,
            completed_all_inputs=step_state.completed_all_inputs,
        )
        await _dispatch_observers(node.observers, StepEvent.COMPLETED, ctx)
    except Exception as exc:
        cause = exc.__cause__ or exc if isinstance(exc, PipelineStopException) else exc
        ctx = StepFailedContext(
            pipeline_name=pipeline_name,
            event=StepEvent.FAILED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
            success_count=step_state.success_count,
            error_count=step_state.error_count,
            completed_all_inputs=step_state.completed_all_inputs,
            exception=cause,
        )
        await _dispatch_observers(node.observers, StepEvent.FAILED, ctx)
        raise


def _output_key(dag: Dag, producer: str, consumer: str) -> str:
    if len(dag.consumers_of(producer)) > 1:
        return f"{producer}__{consumer}"
    return producer


# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


async def _collect_async_iterator(dag: Dag, step_name: str, value: Any) -> list[Any]:
    items = []
    try:
        if isinstance(value, (AsyncIterator, AsyncGenerator)):
            while True:
                try:
                    item = await anext(value)
                    items.append(item)
                except StopAsyncIteration:
                    break
        else:
            iterator = iter(value)
            while True:
                try:
                    item = next(iterator)
                    items.append(item)
                except StopIteration:
                    break
    except Exception as exc:
        await _handle_error(dag, step_name, exc)
        if dag[step_name].on_error == OnError.STOP:
            raise PipelineStopException(step_name=step_name, cause=exc) from exc
    return items


async def _apply_materializer(
    dag: Dag, step_name: str, value: Any, consumer_type: Any = None
) -> Any:
    node = dag[step_name]
    mat = node.get("materializer")
    mat_name = getattr(mat, "__name__", str(mat)) if mat else None

    # Emit Materialization STARTED
    ctx_started = MaterializationStartedContext(
        pipeline_name=dag.name,
        event=MaterializationEvent.STARTED,
        step_name=step_name,
        dataset_name=step_name,
        consumer_type=consumer_type,
        materializer_name=mat_name,
    )
    await _dispatch_observers(node.observers, MaterializationEvent.STARTED, ctx_started)

    try:
        if mat is None:
            if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
                res = await _collect_async_iterator(dag, step_name, value)
            else:
                res = value
        else:
            concrete_mat = mat(
                MaterializeContext(
                    pipeline_name=dag.name,
                    dataset_name=step_name,
                    item_type=node.get("output"),
                    consumer_type=consumer_type,
                )
            )
            if inspect.iscoroutinefunction(concrete_mat):
                res = await concrete_mat(value)
            elif isinstance(
                value, (AsyncIterator, AsyncGenerator, Iterator, Generator)
            ):
                # If it's a standard collection, collect first
                if (
                    concrete_mat in (list, tuple, set, dict)
                    or getattr(concrete_mat, "__name__", "") == "_identity"
                ):
                    items = await _collect_async_iterator(dag, step_name, value)
                    res = items if concrete_mat is list else concrete_mat(items)
                    if inspect.iscoroutine(res):
                        res = await res
                else:
                    items = await _collect_async_iterator(dag, step_name, value)
                    res = concrete_mat(items)
                    if inspect.iscoroutine(res):
                        res = await res
            else:
                res = concrete_mat(value)
                if inspect.iscoroutine(res):
                    res = await res

        # Emit Materialization COMPLETED
        ctx_completed = MaterializationCompletedContext(
            pipeline_name=dag.name,
            event=MaterializationEvent.COMPLETED,
            step_name=step_name,
            dataset_name=step_name,
            consumer_type=consumer_type,
            materializer_name=mat_name,
        )
        await _dispatch_observers(
            node.observers, MaterializationEvent.COMPLETED, ctx_completed
        )
        return res
    except Exception as exc:
        # Emit Materialization FAILED
        ctx_failed = MaterializationFailedContext(
            pipeline_name=dag.name,
            event=MaterializationEvent.FAILED,
            step_name=step_name,
            dataset_name=step_name,
            consumer_type=consumer_type,
            materializer_name=mat_name,
            exception=exc,
        )
        await _dispatch_observers(
            node.observers, MaterializationEvent.FAILED, ctx_failed
        )
        raise


async def _handle_error(dag: Dag, step_name: str, exc: BaseException) -> None:
    node = dag.steps.get(step_name)
    if not node:
        return

    err_mat = getattr(node, "error_materializer", None)
    if err_mat is None:
        return

    if inspect.iscoroutinefunction(err_mat):
        handler = await err_mat(
            ErrorMaterializeContext(
                pipeline_name=dag.name,
                dataset_name=step_name,
                exception_type=type(exc),
            )
        )
    else:
        handler = err_mat(
            ErrorMaterializeContext(
                pipeline_name=dag.name,
                dataset_name=step_name,
                exception_type=type(exc),
            )
        )

    if handler is not None:
        if inspect.iscoroutinefunction(handler):
            await handler(exc)
        else:
            res = handler(exc)
            if inspect.iscoroutine(res):
                await res


async def _pump_iterator(
    name: str,
    iterator: Any,
    queues: dict[str, asyncio.Queue],
    on_error: Any,
    dag: Dag | None = None,
    materialize_before_enqueue: bool = False,
    consumer_type: Any = None,
) -> None:
    try:
        safe = _safe_iterate(name, iterator)
        if materialize_before_enqueue:
            items = await _apply_materializer(
                dag, name, safe, consumer_type=consumer_type
            )
            async for item in _safe_iterate(name, items):
                for q in queues.values():
                    await q.put(item)
        else:
            async for item in safe:
                for q in queues.values():
                    await q.put(item)
    except StepExecutionError as e:
        await _handle_error(dag, name, e.__cause__ or e)
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

    async def execute(self, params: Any) -> None:
        pipeline_observers = self.dag.observers

        # Emit Pipeline STARTED
        ctx_started = PipelineStartedContext(
            pipeline_name=self.dag.name, event=PipelineEvent.STARTED
        )
        await _dispatch_observers(
            pipeline_observers, PipelineEvent.STARTED, ctx_started
        )

        for field, value in params._asdict().items():
            self.outputs[field] = value

        try:
            for level in self.dag.get_execution_levels():
                tasks = [self._run_step(name) for name in level]
                if tasks:
                    await asyncio.gather(*tasks)

            if self._pump_tasks:
                await asyncio.gather(*self._pump_tasks)
        except PipelineStopException as exc:
            # Emit Pipeline FAILED
            ctx_failed = PipelineFailedContext(
                pipeline_name=self.dag.name,
                event=PipelineEvent.FAILED,
                step_name=exc.step_name,
                exception=exc.__cause__ or exc,
            )
            await _dispatch_observers(
                pipeline_observers, PipelineEvent.FAILED, ctx_failed
            )
            raise
        except Exception as exc:
            # Emit Pipeline FAILED for generic exception
            ctx_failed = PipelineFailedContext(
                pipeline_name=self.dag.name,
                event=PipelineEvent.FAILED,
                step_name=None,
                exception=exc,
            )
            await _dispatch_observers(
                pipeline_observers, PipelineEvent.FAILED, ctx_failed
            )
            raise
        else:
            # Emit Pipeline COMPLETED
            ctx_completed = PipelineCompletedContext(
                pipeline_name=self.dag.name, event=PipelineEvent.COMPLETED
            )
            await _dispatch_observers(
                pipeline_observers, PipelineEvent.COMPLETED, ctx_completed
            )

    async def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        unrolled = self.dag.each_inputs(step_name)
        step_state = StepState()

        # Emit Step STARTED
        ctx_started = StepStartedContext(
            pipeline_name=self.dag.name,
            event=StepEvent.STARTED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
        )
        await _dispatch_observers(node.observers, StepEvent.STARTED, ctx_started)

        arguments = await self._build_arguments(step_name, node, unrolled)

        try:
            if unrolled:
                output = await self._unroll_step(
                    step_name, node, arguments, unrolled, step_state
                )
            else:
                output = await self._call_fn(node.fn, arguments)
                if isinstance(
                    output, (AsyncIterator, AsyncGenerator, Iterator, Generator)
                ):
                    output = _wrap_step_async_iterator(
                        output, step_state, step_name, node, self.dag.name
                    )
                else:
                    step_state.success_count = 1
                    step_state.completed_all_inputs = True
                    # Emit Step COMPLETED
                    ctx_completed = StepCompletedContext(
                        pipeline_name=self.dag.name,
                        event=StepEvent.COMPLETED,
                        step_name=step_name,
                        mode=node.mode,
                        on_error=node.on_error,
                        success_count=step_state.success_count,
                        error_count=step_state.error_count,
                        completed_all_inputs=step_state.completed_all_inputs,
                    )
                    await _dispatch_observers(
                        node.observers, StepEvent.COMPLETED, ctx_completed
                    )

            if not step_name.startswith("_"):
                await self._publish_output(step_name, output, node)
        except PipelineStopException as exc:
            ctx_failed = StepFailedContext(
                pipeline_name=self.dag.name,
                event=StepEvent.FAILED,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=step_state.success_count,
                error_count=step_state.error_count,
                completed_all_inputs=step_state.completed_all_inputs,
                exception=exc.__cause__ or exc,
            )
            await _dispatch_observers(node.observers, StepEvent.FAILED, ctx_failed)
            raise
        except Exception as exc:
            ctx_failed = StepFailedContext(
                pipeline_name=self.dag.name,
                event=StepEvent.FAILED,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=step_state.success_count,
                error_count=step_state.error_count,
                completed_all_inputs=step_state.completed_all_inputs,
                exception=exc,
            )
            await _dispatch_observers(node.observers, StepEvent.FAILED, ctx_failed)
            await _handle_error(self.dag, step_name, exc)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc

    async def _call_fn(self, fn: Any, kwargs: dict) -> Any:
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        return fn(**kwargs)

    async def _unroll_step(self, step_name, node, base_args, unrolled, step_state):
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
                    res = await self._call_fn(node.fn, item_args)
                    step_state.success_count += 1
                    yield res
                except Exception as exc:
                    step_state.error_count += 1
                    await _handle_error(self.dag, step_name, exc)
                    if node.on_error == OnError.STOP:
                        raise PipelineStopException(
                            step_name=step_name, cause=exc
                        ) from exc

        if self._is_terminal(step_name):
            try:
                async for _ in generate():
                    pass
                step_state.completed_all_inputs = True
                from synaflow.core.observers import StepCompletedContext, StepEvent

                ctx_completed = StepCompletedContext(
                    pipeline_name=self.dag.name,
                    event=StepEvent.COMPLETED,
                    step_name=step_name,
                    mode=node.mode,
                    on_error=node.on_error,
                    success_count=step_state.success_count,
                    error_count=step_state.error_count,
                    completed_all_inputs=step_state.completed_all_inputs,
                )
                await _dispatch_observers(
                    node.observers, StepEvent.COMPLETED, ctx_completed
                )
            except Exception as exc:
                raise
            return None
        return _wrap_step_async_iterator(
            generate(), step_state, step_name, node, self.dag.name
        )

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
        if isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator)):
            consumers = self.dag.consumers_of(step_name)

            # 1. Step-level materialization required?
            if node.on_error == OnError.STOP or node.force_materialize:
                consumer_type = None
                if consumers:
                    consumer_type = self.dag[consumers[0]].deps.get(step_name)
                try:
                    items = await _apply_materializer(
                        self.dag, step_name, output, consumer_type=consumer_type
                    )
                    for c in consumers:
                        self.outputs[_output_key(self.dag, step_name, c)] = items
                    self._notify_observers(step_name, items)
                except PipelineStopException:
                    raise
                except Exception as exc:
                    await _handle_error(self.dag, step_name, exc)
                    if node.on_error == OnError.STOP:
                        raise PipelineStopException(
                            step_name=step_name, cause=exc
                        ) from exc
                return

            # 2. Single consumer requires materialized input?
            if len(consumers) == 1 and self.dag.needs_materialize(step_name):
                consumer_type = self.dag[consumers[0]].deps.get(step_name)
                try:
                    items = await _apply_materializer(
                        self.dag, step_name, output, consumer_type=consumer_type
                    )
                    self.outputs[_output_key(self.dag, step_name, consumers[0])] = items
                    self._notify_observers(step_name, items)
                except PipelineStopException:
                    raise
                except Exception as exc:
                    await _handle_error(self.dag, step_name, exc)
                    if node.on_error == OnError.STOP:
                        raise PipelineStopException(
                            step_name=step_name, cause=exc
                        ) from exc
                return

            # 3. Otherwise, keep it lazy / stream-based
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
                task = asyncio.create_task(
                    _pump_iterator(
                        step_name,
                        output,
                        queues,
                        node.on_error,
                        dag=self.dag,
                        materialize_before_enqueue=False,
                    )
                )
                self._pump_tasks.append(task)
                return
            else:
                if self.dag.needs_materialize(step_name):
                    output = await _apply_materializer(
                        self.dag, step_name, output, consumer_type=node.get("output")
                    )
                else:
                    self._notify_observers(step_name, output)
        elif self.dag.needs_materialize(step_name):
            output = await _apply_materializer(
                self.dag, step_name, output, consumer_type=node.get("output")
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
