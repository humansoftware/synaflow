import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException, StepExecutionError
from synaflow.core.observers import (
    MaterializationCompletedContext,
    MaterializationEvent,
    MaterializationFailedContext,
    MaterializationStartedContext,
    PipelineCompletedContext,
    PipelineEvent,
    PipelineFailedContext,
    PipelineStartedContext,
    StepCompletedContext,
    StepEvent,
    StepFailedContext,
    StepStartedContext,
    dispatch_observers_async,
)
from synaflow.core.types import (
    ErrorMaterializeContext,
    MaterializeContext,
    OnError,
    StepMode,
)


def _output_key(dag: Dag, producer: str, consumer: str) -> str:
    if len(dag.consumers_of(producer)) > 1:
        return f"{producer}__{consumer}"
    return producer


from .constants import EOF_MARKER
from .iterator_utils import queue_to_async_gen

# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


async def _collect_async_iterator(
    dag: Dag, step_name: str, value: Any
) -> tuple[list[Any], bool, BaseException | None]:
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
        return items, True, exc
    return items, False, None


async def _apply_materializer(
    dag: Dag, step_name: str, value: Any, consumer_type: Any = None
) -> tuple[Any, bool, BaseException | None]:
    node = dag[step_name]
    mat = node.get("materializer")
    if mat is None:
        if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
            items, had_error, exc = await _collect_async_iterator(dag, step_name, value)
            return items, had_error, exc
        return value, False, None
    concrete_mat = mat(
        MaterializeContext(
            pipeline_name=dag.name,
            dataset_name=step_name,
            item_type=node.get("output"),
            consumer_type=consumer_type,
        )
    )
    if inspect.iscoroutinefunction(concrete_mat):
        result = await concrete_mat(value)
        return result, False, None
    if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
        if (
            concrete_mat in (list, tuple, set, dict)
            or getattr(concrete_mat, "__name__", "") == "_identity"
        ):
            items, had_error, exc = await _collect_async_iterator(dag, step_name, value)
            res = items if concrete_mat is list else concrete_mat(items)
            if inspect.iscoroutine(res):
                return await res, had_error, exc
            return res, had_error, exc
        items, had_error, exc = await _collect_async_iterator(dag, step_name, value)
        res = concrete_mat(items)
        if inspect.iscoroutine(res):
            return await res, had_error, exc
        return res, had_error, exc
    res = concrete_mat(value)
    if inspect.iscoroutine(res):
        return await res, False, None
    return res, False, None


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
            items, _, _ = await _apply_materializer(
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
    result, _, _ = await _apply_materializer(
        dag, producer, queue_to_async_gen(queue), consumer_type=consumer_type
    )
    return result


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class AsyncPipelineExecutor:
    def __init__(self, dag: Dag, *, step_output_observers: list = None):
        self.dag = dag
        self.outputs = {}
        self._pump_tasks: list[asyncio.Task] = []
        self._step_output_observers = step_output_observers or []

    # ------------------------------------------------------------------
    # Lifecycle observer dispatch helpers (async)
    # ------------------------------------------------------------------

    async def _dispatch_pipeline_event(
        self,
        event: PipelineEvent,
        step_name: str | None = None,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self.dag.pipeline_observers
        if not registrations:
            return
        ctx: Any
        if event is PipelineEvent.STARTED:
            ctx = PipelineStartedContext(pipeline_name=self.dag.name, event=event)
        elif event is PipelineEvent.COMPLETED:
            ctx = PipelineCompletedContext(pipeline_name=self.dag.name, event=event)
        elif event is PipelineEvent.FAILED:
            ctx = PipelineFailedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                exception=exception,
            )
        else:
            return
        await dispatch_observers_async(registrations, ctx)

    async def _dispatch_step_event(
        self,
        node: Any,
        event: StepEvent,
        step_name: str,
        success_count: int = 0,
        error_count: int = 0,
        completed_all_inputs: bool = True,
        exception: BaseException | None = None,
    ) -> None:
        registrations = node.observers
        if not registrations:
            return
        ctx: Any
        if event is StepEvent.STARTED:
            ctx = StepStartedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
            )
        elif event is StepEvent.COMPLETED:
            ctx = StepCompletedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=success_count,
                error_count=error_count,
                completed_all_inputs=completed_all_inputs,
            )
        elif event is StepEvent.FAILED:
            ctx = StepFailedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=success_count,
                error_count=error_count,
                completed_all_inputs=completed_all_inputs,
                exception=exception,
            )
        else:
            return
        await dispatch_observers_async(registrations, ctx)

    async def _dispatch_materialization_event(
        self,
        step_name: str,
        node: Any,
        event: MaterializationEvent,
        consumer_type: Any = None,
        materializer_name: str | None = None,
        exception: BaseException | None = None,
    ) -> None:
        registrations = node.observers
        if not registrations:
            return
        ctx: Any
        if event is MaterializationEvent.STARTED:
            ctx = MaterializationStartedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                dataset_name=step_name,
                consumer_type=consumer_type,
                materializer_name=materializer_name,
            )
        elif event is MaterializationEvent.COMPLETED:
            ctx = MaterializationCompletedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                dataset_name=step_name,
                consumer_type=consumer_type,
                materializer_name=materializer_name,
            )
        elif event is MaterializationEvent.FAILED:
            ctx = MaterializationFailedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                dataset_name=step_name,
                consumer_type=consumer_type,
                materializer_name=materializer_name,
                exception=exception,
            )
        else:
            return
        await dispatch_observers_async(registrations, ctx)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(self, params: Any) -> None:
        for field, value in params._asdict().items():
            self.outputs[field] = value

        await self._dispatch_pipeline_event(PipelineEvent.STARTED)
        try:
            for level in self.dag.get_execution_levels():
                tasks = [self._run_step(name) for name in level]
                if tasks:
                    await asyncio.gather(*tasks)

            if self._pump_tasks:
                await asyncio.gather(*self._pump_tasks)
        except PipelineStopException as exc:
            await self._dispatch_pipeline_event(
                PipelineEvent.FAILED,
                step_name=exc.step_name,
                exception=exc.cause or exc,
            )
            raise
        except Exception as exc:
            await self._dispatch_pipeline_event(
                PipelineEvent.FAILED, step_name=None, exception=exc
            )
            raise
        else:
            await self._dispatch_pipeline_event(PipelineEvent.COMPLETED)

    async def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        unrolled = self.dag.each_inputs(step_name)
        arguments = await self._build_arguments(step_name, node, unrolled)
        is_each = bool(unrolled)

        await self._dispatch_step_event(node, StepEvent.STARTED, step_name)

        try:
            if unrolled:
                output = await self._unroll_step(step_name, node, arguments, unrolled)
            else:
                output = await self._call_fn(node.fn, arguments)

            if not is_each and not isinstance(
                output, (Iterator, Generator, AsyncIterator, AsyncGenerator)
            ):
                await self._dispatch_step_event(
                    node,
                    StepEvent.COMPLETED,
                    step_name,
                    success_count=1,
                    error_count=0,
                    completed_all_inputs=True,
                )

            if not step_name.startswith("_"):
                await self._publish_output(step_name, output, node)
        except PipelineStopException as exc:
            cause = exc.cause or exc
            if isinstance(cause, PipelineStopException):
                cause = cause.cause or cause
            await self._dispatch_step_event(
                node,
                StepEvent.FAILED,
                step_name,
                success_count=0,
                error_count=1,
                completed_all_inputs=False,
                exception=cause,
            )
            raise
        except Exception as exc:
            await _handle_error(self.dag, step_name, exc)
            await self._dispatch_step_event(
                node,
                StepEvent.FAILED,
                step_name,
                success_count=0,
                error_count=1,
                completed_all_inputs=False,
                exception=exc,
            )
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
                        param = node.dataset_param_names.get(dep, dep)
                        item_args[param] = None
                        continue

                    item = await queues[dep].get()
                    if item is EOF_MARKER:
                        completed.add(dep)
                        param = node.dataset_param_names.get(dep, dep)
                        item_args[param] = None
                    elif isinstance(item, Exception):
                        raise item
                    else:
                        param = node.dataset_param_names.get(dep, dep)
                        item_args[param] = item
                if len(completed) == len(unrolled):
                    break
                try:
                    yield await self._call_fn(node.fn, item_args)
                except Exception as exc:
                    await _handle_error(self.dag, step_name, exc)
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
            param = node.dataset_param_names.get(dep_name, dep_name)
            args[param] = value
        return args

    def _notify_observers(self, step_name, output):
        for observer in self._step_output_observers:
            observer(step_name, output)

    async def _materialize_with_events(
        self, step_name, output, node, consumer_type=None
    ):
        mat_name = node.materializer.__name__ if callable(node.materializer) else None
        await self._dispatch_materialization_event(
            step_name,
            node,
            MaterializationEvent.STARTED,
            consumer_type,
            mat_name,
        )
        try:
            result, had_error, exc = await _apply_materializer(
                self.dag, step_name, output, consumer_type=consumer_type
            )
            await self._dispatch_materialization_event(
                step_name,
                node,
                MaterializationEvent.COMPLETED,
                consumer_type,
                mat_name,
            )
            return result, had_error, exc
        except PipelineStopException:
            raise
        except Exception as exc:
            await self._dispatch_materialization_event(
                step_name,
                node,
                MaterializationEvent.FAILED,
                consumer_type,
                mat_name,
                exception=exc,
            )
            raise

    async def _emit_step_result(
        self, node, step_name, output, had_error, exception=None
    ):
        success = len(output) if hasattr(output, "__len__") else 1
        if had_error:
            await self._dispatch_step_event(
                node,
                StepEvent.FAILED,
                step_name,
                success_count=success,
                error_count=1,
                completed_all_inputs=False,
                exception=exception,
            )
        else:
            await self._dispatch_step_event(
                node,
                StepEvent.COMPLETED,
                step_name,
                success_count=success,
                error_count=0,
                completed_all_inputs=True,
            )

    async def _publish_output(self, step_name, output, node):
        deferred = node.mode == StepMode.EACH or (
            node.mode == StepMode.ALL
            and isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator))
        )

        if isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator)):
            consumers = self.dag.consumers_of(step_name)

            # 1. Step-level materialization required?
            if node.on_error == OnError.STOP or node.force_materialize:
                consumer_type = None
                if consumers:
                    consumer_type = self.dag[consumers[0]].deps.get(step_name)
                try:
                    items, had_error, exc = await self._materialize_with_events(
                        step_name, output, node, consumer_type=consumer_type
                    )
                    for c in consumers:
                        self.outputs[_output_key(self.dag, step_name, c)] = items
                    self._notify_observers(step_name, items)
                    if deferred:
                        await self._emit_step_result(
                            node, step_name, items, had_error, exc
                        )
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
                    items, had_error, exc = await self._materialize_with_events(
                        step_name, output, node, consumer_type=consumer_type
                    )
                    self.outputs[_output_key(self.dag, step_name, consumers[0])] = items
                    self._notify_observers(step_name, items)
                    if deferred:
                        await self._emit_step_result(
                            node, step_name, items, had_error, exc
                        )
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
                if deferred:
                    await self._dispatch_step_event(
                        node,
                        StepEvent.COMPLETED,
                        step_name,
                        success_count=0,
                        error_count=0,
                        completed_all_inputs=True,
                    )
                return
            else:
                if self.dag.needs_materialize(step_name):
                    output, had_error, exc = await self._materialize_with_events(
                        step_name, output, node, consumer_type=node.get("output")
                    )
                else:
                    self._notify_observers(step_name, output)
                    had_error = False
                    exc = None
                if deferred:
                    await self._emit_step_result(
                        node, step_name, output, had_error, exc
                    )
                return
        elif self.dag.needs_materialize(step_name):
            output, had_error, exc = await self._materialize_with_events(
                step_name, output, node, consumer_type=node.get("output")
            )
        else:
            had_error = False
            exc = None
        self.outputs[step_name] = output
        self._notify_observers(step_name, output)
        if deferred and not isinstance(
            output, (Iterator, Generator, AsyncIterator, AsyncGenerator)
        ):
            await self._emit_step_result(node, step_name, output, had_error, exc)


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
