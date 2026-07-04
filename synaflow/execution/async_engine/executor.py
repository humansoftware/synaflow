from __future__ import annotations
import asyncio
import inspect
import dataclasses
import uuid
from contextlib import AsyncExitStack
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import (
    PipelineStopException,
    StepExecutionError,
    ThresholdExceededException,
)
from .event_dispatch import AsyncEventDispatcher
from synaflow.core.types import (
    OnError,
    StepMode,
)
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.threshold import (
    check_threshold,
    wrap_threshold_raise_if_manual,
    compute_completed_all_inputs_for_all,
    has_threshold,
)

from .constants import EOF_MARKER
from .iterator_utils import AsyncQueueBranch, queue_to_async_gen


# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


async def _collect_async_iterator(
    dag: Dag,
    step_name: str,
    value: Any,
    events: "AsyncEventDispatcher",
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
        await events.handle_error(
            step_name,
            exc,
            success_count=len(items),
            error_count=1,
            completed_all_inputs=False,
        )
        if dag[step_name].on_error == OnError.STOP:
            raise PipelineStopException(step_name=step_name, cause=exc) from exc
        return items, True, exc
    return items, False, None


async def _apply_materializer(
    dag: Dag,
    step_name: str,
    value: Any,
    materializer: Any,
    events: "AsyncEventDispatcher",
    consumer_type: Any = None,
) -> tuple[Any, bool, BaseException | None]:
    if materializer is None:
        if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
            items, had_error, exc = await _collect_async_iterator(
                dag, step_name, value, events
            )
            return items, had_error, exc
        return value, False, None

    if inspect.iscoroutinefunction(materializer):
        result = await materializer(value)
        return result, False, None

    if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
        items, had_error, exc = await _collect_async_iterator(
            dag, step_name, value, events
        )
        res = materializer(items)
        if inspect.iscoroutine(res):
            return await res, had_error, exc
        return res, had_error, exc

    res = materializer(value)
    if inspect.iscoroutine(res):
        return await res, False, None
    return res, False, None


async def _pump_iterator(
    name: str,
    iterator: Any,
    queues: dict[str, Any],
    on_error: Any,
    dag: Dag | None = None,
    events: "AsyncEventDispatcher" | None = None,
) -> None:
    try:
        async for item in _safe_iterate(name, iterator):
            for q in queues.values():
                await q.put(item)
    except StepExecutionError as e:
        cause = e.__cause__ or e
        if dag is not None and events is not None:
            await events.handle_error(name, cause)
        if isinstance(cause, ThresholdExceededException):
            # Threshold violation from the producer: propagate regardless of on_error.
            for q in queues.values():
                await q.put(cause)
            raise PipelineStopException(step_name=name) from e
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


async def _list_to_async_gen(lst):
    for item in lst:
        yield item


async def _resolve_queue(
    queue: asyncio.Queue,
) -> Any:
    if isinstance(queue, AsyncQueueBranch):
        return queue
    return queue_to_async_gen(queue)


async def _wrap_started_stream(it: Any, fire_started: Any) -> Any:
    if isinstance(it, (AsyncIterator, AsyncGenerator)):
        try:
            async for item in it:
                await fire_started()
                yield item
        finally:
            await fire_started()
    else:
        iterator = iter(it)
        try:
            while True:
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                await fire_started()
                yield item
        finally:
            await fire_started()


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class AsyncPipelineExecutor:
    def __init__(
        self,
        dag: Dag,
        *,
        step_output_observers: list = None,
        overrides: ExecutionOverrides | None = None,
        resource_factories: dict[str, Any] | None = None,
    ):
        self.dag = dag
        self.outputs = {}
        self._pump_tasks: list[asyncio.Task] = []
        self._step_output_observers = step_output_observers or []
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})
        self.run_id = str(uuid.uuid4())
        self.events = AsyncEventDispatcher(self.dag, self.run_id, self._overrides)

    # ------------------------------------------------------------------
    # Lifecycle observer dispatch helpers (async)
    # ------------------------------------------------------------------

    def _resolve_materializer(self, step_name: str, node: Any) -> Any:
        if self._overrides is None:
            return node.materializer
        return self._overrides.materializers.resolve(step_name, node.materializer)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def _seed_runtime_inputs(self, params: Any) -> None:
        if dataclasses.is_dataclass(params):
            param_dict = {
                f.name: getattr(params, f.name) for f in dataclasses.fields(params)
            }
        else:
            param_dict = params._asdict()
        for field, value in param_dict.items():
            self.outputs[field] = value

    def _step_inputs_available(self, step_name: str) -> bool:
        node = self.dag[step_name]
        for dep_name in node.deps:
            if dep_name in self.dag.resources:
                continue
            key = self.dag.output_key(dep_name, step_name)
            if key not in self.outputs and dep_name not in self.outputs:
                return False
        return True

    async def _run_graph(self) -> None:
        running_tasks = set()
        finished_tasks = set()
        ready_tasks = set()
        fatal_error = None

        event = asyncio.Event()

        def check_new_ready_steps():
            for s in self.dag.steps:
                if (
                    s not in ready_tasks
                    and s not in running_tasks
                    and s not in finished_tasks
                ):
                    if self._step_inputs_available(s):
                        ready_tasks.add(s)

            while ready_tasks:
                s = ready_tasks.pop()
                running_tasks.add(s)
                task = asyncio.create_task(self._run_step(s))
                task.add_done_callback(lambda t, step_name=s: step_done(t, step_name))

        def step_done(task, step_name):
            nonlocal fatal_error
            running_tasks.remove(step_name)
            finished_tasks.add(step_name)
            try:
                task.result()
            except BaseException as exc:
                if fatal_error is None:
                    fatal_error = exc
                for t in self._pump_tasks:
                    t.cancel()

            if fatal_error is None:
                check_new_ready_steps()

            if not running_tasks:
                event.set()

        check_new_ready_steps()
        if running_tasks:
            await event.wait()

        if fatal_error is not None:
            raise fatal_error

    async def execute(self, params: Any) -> None:
        self._seed_runtime_inputs(params)

        await self.events.pipeline_started()
        try:
            await self._run_graph()

            if self._pump_tasks:
                await asyncio.gather(*self._pump_tasks)
        except PipelineStopException as exc:
            await self.events.pipeline_failed(
                step_name=exc.step_name,
                exception=exc.cause or exc,
            )
            raise
        except ThresholdExceededException as exc:
            await self.events.pipeline_failed(
                step_name=exc.step_name,
                exception=exc,
            )
            raise
        except Exception as exc:
            await self.events.pipeline_failed(step_name=None, exception=exc)
            raise
        else:
            await self.events.pipeline_completed()

    async def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        unrolled = self.dag.each_inputs(step_name)
        resource_stack = AsyncExitStack()
        arguments = await self._build_arguments(
            step_name, node, unrolled, resource_stack
        )
        await self.events.step_started(node, step_name)

        started = False

        async def fire_started():
            nonlocal started
            if not started:
                await self.events.step_started(node, step_name)
                started = True

        try:
            if (
                not unrolled
                and not inspect.isasyncgenfunction(node.fn)
                and not inspect.isgeneratorfunction(node.fn)
            ):
                await fire_started()
            output = await self._execute_step(step_name, node, arguments, unrolled)
            if self._is_stream_output(output):
                output = _wrap_started_stream(output, fire_started)
            output = self._attach_argument_cleanup(output, arguments)
            await self._emit_immediate_completion(step_name, node, output, unrolled)
            if not self.dag.is_hidden_step(step_name):
                await self._publish_output(step_name, output, node)
        except PipelineStopException as exc:
            await self._dispatch_step_failure(node, step_name, exc.cause or exc)
            raise
        except ThresholdExceededException as exc:
            if exc.step_name != step_name:
                # Upstream threshold propagating through this consumer:
                # the producer's generate() already dispatched FAILED.
                pass
            elif unrolled and has_threshold(node):
                # This step's generate() already dispatched FAILED (path A).
                pass
            elif not unrolled:
                # ALL-mode manual raise by this step (path B, escape hatch)
                completed_all_inputs = compute_completed_all_inputs_for_all(
                    node, arguments, exc
                )
                await self.events.handle_error(
                    step_name,
                    exc,
                    success_count=exc.success_count,
                    error_count=exc.error_count,
                    completed_all_inputs=completed_all_inputs,
                )
                await self._dispatch_step_failure(
                    node,
                    step_name,
                    exc,
                    success_count=exc.success_count,
                    error_count=exc.error_count,
                    completed_all_inputs=completed_all_inputs,
                )
            else:
                # EACH mode, no threshold configured (should not reach here
                # per build-time validation, but handle defensively)
                await self._dispatch_step_failure(
                    node,
                    step_name,
                    exc,
                    success_count=exc.success_count,
                    error_count=exc.error_count,
                    completed_all_inputs=True,
                )
            raise
        except Exception as exc:
            await self.events.handle_error(step_name, exc)
            await self._dispatch_step_failure(node, step_name, exc)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
        finally:
            if "output" not in locals() or not self._is_stream_output(output):
                await self._close_stream_arguments(arguments)
            await resource_stack.aclose()

    async def _execute_step(self, step_name, node, arguments, unrolled):
        if unrolled:
            return await self._unroll_step(step_name, node, arguments, unrolled)
        return await self._call_fn(node.fn, arguments)

    async def _emit_immediate_completion(self, step_name, node, output, unrolled):
        if unrolled or isinstance(
            output, (Iterator, Generator, AsyncIterator, AsyncGenerator)
        ):
            return
        success_count = 1
        if isinstance(output, (list, tuple, set)):
            success_count = len(output)
        await self.events.step_completed(
            node,
            step_name,
            success_count=success_count,
            error_count=0,
            completed_all_inputs=True,
        )

    async def _dispatch_step_failure(
        self,
        node,
        step_name,
        exception,
        success_count: int = 0,
        error_count: int = 1,
        completed_all_inputs: bool = False,
    ):
        cause = exception
        if isinstance(cause, PipelineStopException):
            cause = cause.cause or cause
        await self.events.step_failed(
            node,
            step_name,
            success_count=success_count,
            error_count=error_count,
            completed_all_inputs=completed_all_inputs,
            exception=cause,
        )

    async def _call_fn(self, fn: Any, kwargs: dict) -> Any:
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        return fn(**kwargs)

    async def _unroll_step(self, step_name, node, base_args, unrolled):
        queues = {}
        for dep in unrolled:
            key = self.dag.output_key(dep, step_name)
            value = self.outputs.get(key, self.outputs.get(dep))
            if isinstance(value, (asyncio.Queue, AsyncQueueBranch)):
                queues[dep] = value
            else:
                producer_node = self.dag.get(dep)
                # Non-queue inputs are already fully available in memory, so
                # max_in_flight does not apply here. Size the queue to avoid
                # deadlocking while preloading eager values for EACH-mode use.
                if isinstance(value, (list, tuple, set)):
                    q = asyncio.Queue(maxsize=max(1, len(value)) + 1)
                else:
                    maxsize = 2
                    if producer_node is not None:
                        maxsize = max(2, getattr(producer_node, "max_in_flight", 1) + 1)
                    q = asyncio.Queue(maxsize=maxsize)
                if isinstance(value, (list, tuple, set)):
                    for item in value:
                        await q.put(item)
                elif value is not None:
                    await q.put(value)
                await q.put(EOF_MARKER)
                queues[dep] = q
        completed = set()

        async def generate():
            invocation_count = 0
            error_count = 0
            # Reset runtime stats on the node so multiple executor runs
            # on the same pipeline don't leak counts across runs.
            node._runtime_error_count = 0
            node._runtime_invocation_count = 0
            try:
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
                    invocation_count += 1
                    try:
                        yield await self._call_fn(node.fn, item_args)
                    except PipelineStopException:
                        # Propagate STOP from upstream producer so the consumer
                        # also stops, even without forced materialization.
                        raise
                    except Exception as exc:
                        error_count += 1
                        await self.events.handle_error(
                            step_name,
                            wrap_threshold_raise_if_manual(exc, step_name),
                            success_count=invocation_count - error_count,
                            error_count=error_count,
                            completed_all_inputs=False,
                        )
                        if node.on_error == OnError.STOP:
                            raise PipelineStopException(
                                step_name=step_name, cause=exc
                            ) from exc
                # pos-loop, before generator ends
                if has_threshold(node):
                    try:
                        check_threshold(step_name, node, invocation_count, error_count)
                    except ThresholdExceededException as exc:
                        await self._dispatch_step_failure(
                            node,
                            step_name,
                            exc,
                            success_count=exc.success_count,
                            error_count=exc.error_count,
                            completed_all_inputs=True,
                        )
                        raise
                    success_count = invocation_count - error_count
                    await self.events.step_completed(
                        node,
                        step_name,
                        success_count=success_count,
                        error_count=error_count,
                        completed_all_inputs=True,
                    )
                else:
                    check_threshold(step_name, node, invocation_count, error_count)
            finally:
                node._runtime_error_count = error_count
                node._runtime_invocation_count = invocation_count

        if self.dag.is_terminal_step(step_name):
            async for _ in generate():
                pass
            return None
        return generate()

    async def _resolve_resource_argument(
        self, resource_name: str, resource_stack: AsyncExitStack
    ):
        provider = None
        if self._overrides is not None:
            provider = self._overrides.resources.resolve(resource_name)
        if provider is None:
            provider = self._resource_factories.get(resource_name)
        if provider is None:
            raise ValueError(
                f"Pipeline '{self.dag.name}' requires resource '{resource_name}' at runtime."
            )

        value = provider() if callable(provider) else provider
        if inspect.isawaitable(value):
            value = await value
        if hasattr(value, "__aenter__") and hasattr(value, "__aexit__"):
            return await resource_stack.enter_async_context(value)
        if hasattr(value, "__enter__") and hasattr(value, "__exit__"):
            return resource_stack.enter_context(value)
        return value

    async def _build_arguments(self, consumer, node, unrolled, resource_stack):
        args = {}
        for dep_name in node.deps:
            if dep_name in self.dag.resources:
                value = await self._resolve_resource_argument(dep_name, resource_stack)
            else:
                key = self.dag.output_key(dep_name, consumer)
                value = self.outputs.get(key, self.outputs.get(dep_name))
                if (
                    isinstance(value, (asyncio.Queue, AsyncQueueBranch))
                    and dep_name not in unrolled
                ):
                    value = await _resolve_queue(value)
                if isinstance(value, (list, tuple, set)) and dep_name not in unrolled:
                    dep_type = node.deps.get(dep_name)
                    origin = getattr(dep_type, "__origin__", dep_type)
                    if origin in (AsyncIterator, AsyncGenerator):
                        value = _list_to_async_gen(value)
            param = node.dataset_param_names.get(dep_name, dep_name)
            args[param] = value
        return args

    def _attach_argument_cleanup(self, output, arguments):
        if not isinstance(output, (AsyncIterator, AsyncGenerator)):
            return output

        async def wrapped():
            try:
                async for item in output:
                    yield item
            finally:
                await self._close_stream_arguments(arguments)

        return wrapped()

    async def _close_stream_arguments(self, arguments):
        for value in arguments.values():
            if isinstance(value, AsyncQueueBranch):
                value.close()
                continue
            if inspect.isasyncgen(value):
                try:
                    await value.aclose()
                except Exception:
                    pass

    def _notify_observers(self, step_name, output):
        for observer in self._step_output_observers:
            observer(step_name, output)

    async def _materialize_with_events(
        self, step_name, output, node, consumer_type=None
    ):
        materializer = self._resolve_materializer(step_name, node)
        mat_name = materializer.__name__ if callable(materializer) else None
        await self.events.materialization_started(
            step_name,
            node,
            consumer_type,
            mat_name,
        )
        try:
            result, had_error, exc = await _apply_materializer(
                self.dag,
                step_name,
                output,
                materializer,
                self.events,
                consumer_type=consumer_type,
            )
            await self.events.materialization_completed(
                step_name,
                node,
                consumer_type,
                mat_name,
            )
            return result, had_error, exc
        except PipelineStopException:
            raise
        except Exception as exc:
            await self.events.materialization_failed(
                step_name,
                node,
                consumer_type,
                mat_name,
                exception=exc,
            )
            raise

    async def _emit_step_result(
        self, node, step_name, output, had_error, exception=None
    ):
        if has_threshold(node):
            return  # already dispatched by generate()
        success = len(output) if hasattr(output, "__len__") else 1
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", success)
        if had_error:
            await self.events.step_failed(
                node,
                step_name,
                success_count=success,
                error_count=max(real_error_count, 1),
                completed_all_inputs=False,
                exception=exception,
            )
        else:
            await self.events.step_completed(
                node,
                step_name,
                success_count=real_invocation_count - real_error_count,
                error_count=real_error_count,
                completed_all_inputs=True,
            )

    async def _emit_deferred_completion(self, node, step_name):
        if has_threshold(node):
            return  # already dispatched by generate()
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", 0)
        await self.events.step_completed(
            node,
            step_name,
            success_count=real_invocation_count - real_error_count,
            error_count=real_error_count,
            completed_all_inputs=True,
        )

    def _wrap_deferred_output(self, step_name, output, node):
        if has_threshold(node):
            return output

        if isinstance(output, (AsyncIterator, AsyncGenerator)):

            async def wrapped_async():
                yielded_count = 0
                async for item in output:
                    yielded_count += 1
                    yield item

                if node.mode == StepMode.ALL:
                    node._runtime_invocation_count = yielded_count
                    node._runtime_error_count = 0
                await self._emit_deferred_completion(node, step_name)

            return wrapped_async()

        async def wrapped_sync():
            yielded_count = 0
            for item in output:
                yielded_count += 1
                yield item

            if node.mode == StepMode.ALL:
                node._runtime_invocation_count = yielded_count
                node._runtime_error_count = 0
            await self._emit_deferred_completion(node, step_name)

        return wrapped_sync()

    def _is_stream_output(self, output):
        return isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator))

    async def _publish_eager_materialized_stream(
        self,
        step_name,
        output,
        node,
        consumers,
        deferred,
    ):
        consumer_type = None
        if consumers:
            consumer_type = self.dag[consumers[0]].deps.get(step_name)
        items, had_error, exc = await self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        for consumer in consumers:
            self.outputs[self.dag.output_key(step_name, consumer)] = items
        self._notify_observers(step_name, items)
        if deferred:
            await self._emit_step_result(node, step_name, items, had_error, exc)

    async def _handle_stream_publish_error(self, step_name, node, exc):
        await self.events.handle_error(step_name, exc)
        if node.on_error == OnError.STOP:
            raise PipelineStopException(step_name=step_name, cause=exc) from exc

    def _register_observer_pumps(self, step_name, queues):
        if not self._step_output_observers:
            return
        for observer in self._step_output_observers:
            obs_queue = asyncio.Queue(maxsize=100)
            queues["__obs"] = obs_queue
            self._pump_tasks.append(
                asyncio.create_task(_pump_observer(step_name, obs_queue, observer))
            )

    async def _publish_stream_to_queues(
        self, step_name, output, node, consumers, deferred
    ):
        queue_maxsize = max(1, node.max_in_flight)
        queues = {
            consumer: AsyncQueueBranch(asyncio.Queue(maxsize=queue_maxsize))
            for consumer in consumers
        }
        for consumer, queue in queues.items():
            self.outputs[self.dag.output_key(step_name, consumer)] = queue
        self._register_observer_pumps(step_name, queues)
        task = asyncio.create_task(
            _pump_iterator(
                step_name,
                output,
                queues,
                node.on_error,
                dag=self.dag,
                events=self.events,
            )
        )
        self._pump_tasks.append(task)

    async def _publish_terminal_stream(self, step_name, output, node, deferred):
        if self.dag.needs_materialize(step_name):
            output, had_error, exc = await self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
        elif self._step_output_observers:
            output, had_error, exc = await _collect_async_iterator(
                self.dag, step_name, output, self.events
            )
        else:
            self._notify_observers(step_name, output)
            had_error = False
            exc = None
        if self._step_output_observers:
            self._notify_observers(step_name, output)
        if deferred:
            await self._emit_step_result(node, step_name, output, had_error, exc)

    async def _publish_scalar_output(self, step_name, output, node, deferred):
        if self.dag.needs_materialize(step_name):
            output, had_error, exc = await self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
        else:
            had_error = False
            exc = None
        self.outputs[step_name] = output
        self._notify_observers(step_name, output)
        if deferred:
            await self._emit_step_result(node, step_name, output, had_error, exc)

    async def _publish_output(self, step_name, output, node):
        deferred = node.mode == StepMode.EACH or (
            node.mode == StepMode.ALL and self._is_stream_output(output)
        )

        if not self._is_stream_output(output):
            await self._publish_scalar_output(step_name, output, node, deferred)
            return

        consumers = self.dag.consumers_of(step_name)

        if self.dag.needs_materialize(step_name):
            try:
                await self._publish_eager_materialized_stream(
                    step_name, output, node, consumers, deferred
                )
            except PipelineStopException:
                raise
            except Exception as exc:
                await self._handle_stream_publish_error(step_name, node, exc)
            return

        if deferred:
            output = self._wrap_deferred_output(step_name, output, node)

        if consumers:
            await self._publish_stream_to_queues(
                step_name, output, node, consumers, deferred
            )
            return

        await self._publish_terminal_stream(step_name, output, node, deferred)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def async_run(
    pipeline: PipelineDef,
    params: Any,
    overrides: ExecutionOverrides | None = None,
) -> None:
    if getattr(pipeline, "requires_sync_runner", False):
        raise RuntimeError(
            "This pipeline contains synchronous streams (Iterator)."
            " It must be executed with run() or migrated to AsyncIterator."
        )
    await AsyncPipelineExecutor(
        pipeline.dag,
        overrides=overrides,
        resource_factories=pipeline.resources,
    ).execute(params)
