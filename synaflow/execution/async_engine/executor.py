from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from contextlib import AsyncExitStack
from typing import Any, Callable

from synaflow.execution.async_engine.lifecycle_stream import AsyncLifecycleStream

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import (
    PipelineStopException,
    StepExecutionError,
    ThresholdExceededException,
)
from synaflow.core.types import OnError, StepMode
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.threshold import (
    check_threshold,
    compute_completed_all_inputs_for_all,
    has_threshold,
    wrap_threshold_raise_if_manual,
)
from synaflow.execution.state import ExecutionState

from .argument_builder import AsyncArgumentBuilder
from .constants import EOF_MARKER
from .event_dispatch import AsyncEventDispatcher
from .iterator_utils import AsyncQueueBranch, queue_to_async_gen
from synaflow.execution.stats import StepRunStats
from .step_lifecycle import AsyncStepLifecycle


# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


async def _pump_iterator(
    name: str,
    iterator: Any,
    queues: dict[str, Any],
    on_error: Any,
    events: AsyncEventDispatcher | None = None,
) -> None:
    try:
        async for item in _safe_iterate(name, iterator):
            for q in queues.values():
                await q.put(item)
    except StepExecutionError as e:
        cause = e.__cause__ or e
        if events is not None:
            await events.handle_error(name, cause)
        if isinstance(cause, ThresholdExceededException):
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


async def _resolve_queue(
    queue: asyncio.Queue,
) -> Any:
    if isinstance(queue, AsyncQueueBranch):
        return queue
    return queue_to_async_gen(queue)


async def _list_to_async_gen(items: list[Any]) -> AsyncGenerator[Any, None]:
    for item in items:
        yield item


def _wrap_started_stream(
    it: AsyncIterator[Any]
    | AsyncGenerator[Any, Any]
    | Iterator[Any]
    | Generator[Any, Any, Any],
    fire_started: Callable[[], Any],
) -> AsyncLifecycleStream:
    return AsyncLifecycleStream(it, on_start=fire_started)


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
        self._step_output_observers = step_output_observers or []
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})

        self.state = ExecutionState(self.dag)
        self.scope = AsyncArgumentBuilder(
            self.dag, self.state, self._overrides, self._resource_factories
        )
        self.run_id = str(uuid.uuid4())
        self.events = AsyncEventDispatcher(self.dag, self.run_id, self._overrides)
        self._pump_tasks: list[asyncio.Task] = []

    @property
    def outputs(self) -> dict[str, Any]:
        return self.state.raw_outputs()

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
                    if self.state.inputs_available(s):
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
                self.abort()

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
        self.scope.seed_runtime_inputs(params)

        await self.events.pipeline_started()
        try:
            await self._run_graph()

            await self.cleanup()
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
        arguments = await self.scope.build_arguments(
            step_name, node, unrolled, resource_stack
        )
        lifecycle = AsyncStepLifecycle(node, step_name, self.events, StepRunStats())

        try:
            if not unrolled and not inspect.isasyncgenfunction(node.fn):
                await lifecycle.start()
            output = await self._execute_step(
                step_name, node, arguments, unrolled, lifecycle
            )
            if self._is_stream_output(output):
                output = _wrap_started_stream(output, lifecycle.start)
            output = self.scope.attach_cleanup(output, arguments)
            await self._emit_immediate_completion(output, unrolled, lifecycle)
            if not self.dag.is_hidden_step(step_name):
                await self.publish(step_name, output, node)
        except PipelineStopException as exc:
            lifecycle.record_error(1)
            await lifecycle.finish(exception=exc, completed_all_inputs=False)
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
                lifecycle.set_counts(exc.success_count, exc.error_count)
                await lifecycle.finish(
                    exception=exc, completed_all_inputs=completed_all_inputs
                )
            else:
                # EACH mode, no threshold configured (should not reach here
                # per build-time validation, but handle defensively)
                lifecycle.set_counts(exc.success_count, exc.error_count)
                await lifecycle.finish(exception=exc, completed_all_inputs=True)
            raise
        except Exception as exc:
            await self.events.handle_error(step_name, exc)
            lifecycle.record_error(1)
            await lifecycle.finish(exception=exc, completed_all_inputs=False)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
        finally:
            if "output" not in locals() or not self._is_stream_output(output):
                await self.scope.close_stream_arguments(arguments)
            await resource_stack.aclose()

    async def _execute_step(self, step_name, node, arguments, unrolled, lifecycle):
        if unrolled:
            return await self._unroll_step(
                step_name, node, arguments, unrolled, lifecycle
            )
        return await self._call_fn(node.fn, arguments)

    async def _emit_immediate_completion(
        self, output, unrolled, lifecycle: AsyncStepLifecycle
    ):
        if unrolled or isinstance(
            output, (Iterator, Generator, AsyncIterator, AsyncGenerator)
        ):
            return
        success_count = 1
        if isinstance(output, (list, tuple, set)):
            success_count = len(output)
        lifecycle.record_success(success_count)
        await lifecycle.finish(completed_all_inputs=True)

    async def _call_fn(self, fn: Any, kwargs: dict) -> Any:
        if inspect.isasyncgenfunction(fn):
            return fn(**kwargs)
        return await fn(**kwargs)

    async def _unroll_step(self, step_name, node, base_args, unrolled, lifecycle):
        queues = {}
        for dep in unrolled:
            value = self.state.get_output(dep, step_name)
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
                        lifecycle.set_counts(exc.success_count, exc.error_count)
                        await lifecycle.finish(exception=exc, completed_all_inputs=True)
                        raise
                    success_count = invocation_count - error_count
                    lifecycle.set_counts(success_count, error_count)
                    await lifecycle.finish(completed_all_inputs=True)
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

    # ------------------------------------------------------------------
    # Dataflow routing & publishing (formerly StreamPublisher)
    # ------------------------------------------------------------------

    def abort(self) -> None:
        """Cancel all active pump tasks."""
        for t in self._pump_tasks:
            t.cancel()

    async def cleanup(self) -> None:
        """Await all pump tasks, suppressing exceptions."""
        if self._pump_tasks:
            try:
                await asyncio.gather(*self._pump_tasks, return_exceptions=True)
            except Exception:
                pass

    async def _collect_async_iterator(
        self,
        step_name: str,
        value: Any,
    ) -> tuple[list[Any], bool, BaseException | None]:
        items = []

        async def handle_error(exc: BaseException, count: int) -> None:
            await self.events.handle_error(
                step_name,
                exc,
                success_count=count,
                error_count=1,
                completed_all_inputs=False,
            )

        stream = AsyncLifecycleStream(
            value, on_item=items.append, on_error=handle_error
        )
        try:
            async for _ in stream:
                pass
            return items, False, None
        except PipelineStopException:
            raise
        except Exception as exc:
            if self.dag[step_name].on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
            return items, True, exc

    async def _apply_materializer(
        self,
        step_name: str,
        value: Any,
        materializer: Any,
        consumer_type: Any = None,
    ) -> tuple[Any, bool, BaseException | None]:
        if materializer is None:
            if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
                items, had_error, exc = await self._collect_async_iterator(
                    step_name, value
                )
                return items, had_error, exc
            return value, False, None

        # To preserve partial items in case the stream crashes during materialization,
        # we wrap the stream and record yielded items.
        history = []
        if self._is_stream_output(value):
            value = AsyncLifecycleStream(value, on_item=history.append)

        # Materializer is guaranteed to be async by validation.
        # It natively handles consuming the stream if needed.
        try:
            result = await materializer(value)
            return result, False, None
        except Exception as e:
            return history, True, e

    def _notify_observers(self, step_name: str, output: Any) -> None:
        for observer in self._step_output_observers:
            observer(step_name, output)

    async def _materialize_with_events(
        self, step_name: str, output: Any, node: Any, consumer_type: Any = None
    ) -> tuple[Any, bool, BaseException | None]:
        materializer = self.scope.resolve_materializer(step_name, node)
        mat_name = materializer.__name__ if callable(materializer) else None
        await self.events.materialization_started(
            step_name,
            node,
            consumer_type,
            mat_name,
        )
        try:
            result, had_error, exc = await self._apply_materializer(
                step_name,
                output,
                materializer,
                consumer_type=consumer_type,
            )
            if had_error:
                await self.events.materialization_failed(
                    step_name,
                    node,
                    consumer_type,
                    mat_name,
                    exception=exc,
                )
            else:
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
        self,
        node: Any,
        step_name: str,
        output: Any,
        had_error: bool,
        exception: BaseException | None = None,
    ) -> None:
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

    async def _emit_deferred_completion(self, node: Any, step_name: str) -> None:
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

    def _wrap_deferred_output(self, step_name: str, output: Any, node: Any) -> Any:
        if has_threshold(node):
            return output

        async def handle_end(count: int) -> None:
            if node.mode == StepMode.ALL:
                node._runtime_invocation_count = count
                node._runtime_error_count = 0
            await self._emit_deferred_completion(node, step_name)

        return AsyncLifecycleStream(output, on_end=handle_end)

    @staticmethod
    def _is_stream_output(output: Any) -> bool:
        return isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator))

    async def _publish_eager_materialized_stream(
        self,
        step_name: str,
        output: Any,
        node: Any,
        consumers: list[str],
        deferred: bool,
    ) -> None:
        consumer_type = None
        if consumers:
            consumer_type = self.dag[consumers[0]].deps.get(step_name)
        items, had_error, exc = await self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        if had_error:
            await self._handle_stream_publish_error(step_name, node, exc)
        for consumer in consumers:
            self.state.set_output(step_name, items, consumer)
        self._notify_observers(step_name, items)
        if deferred:
            await self._emit_step_result(node, step_name, items, had_error, exc)

    async def _handle_stream_publish_error(
        self, step_name: str, node: Any, exc: Exception
    ) -> None:
        await self.events.handle_error(step_name, exc)
        if node.on_error == OnError.STOP:
            raise PipelineStopException(step_name=step_name, cause=exc) from exc

    def _register_observer_pumps(self, step_name: str, queues: dict[str, Any]) -> None:
        if not self._step_output_observers:
            return
        for observer in self._step_output_observers:
            obs_queue = asyncio.Queue(maxsize=100)
            queues["__obs"] = obs_queue
            self._pump_tasks.append(
                asyncio.create_task(_pump_observer(step_name, obs_queue, observer))
            )

    async def _publish_stream_to_queues(
        self,
        step_name: str,
        output: Any,
        node: Any,
        consumers: list[str],
        deferred: bool,
    ) -> None:
        queue_maxsize = max(1, node.max_in_flight)
        queues = {
            consumer: AsyncQueueBranch(asyncio.Queue(maxsize=queue_maxsize))
            for consumer in consumers
        }
        for consumer, queue in queues.items():
            self.state.set_output(step_name, queue, consumer)
        self._register_observer_pumps(step_name, queues)
        task = asyncio.create_task(
            _pump_iterator(
                step_name,
                output,
                queues,
                node.on_error,
                self.events,
            )
        )
        self._pump_tasks.append(task)

    async def _publish_terminal_stream(
        self, step_name: str, output: Any, node: Any, deferred: bool
    ) -> None:
        if self.dag.needs_materialize(step_name):
            output, had_error, exc = await self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
            if had_error:
                await self._handle_stream_publish_error(step_name, node, exc)
        elif self._step_output_observers:
            output, had_error, exc = await self._collect_async_iterator(
                step_name, output
            )
        else:
            self._notify_observers(step_name, output)
            had_error = False
            exc = None
        if self._step_output_observers:
            self._notify_observers(step_name, output)
        if deferred:
            await self._emit_step_result(node, step_name, output, had_error, exc)

    async def _publish_scalar_output(
        self, step_name: str, output: Any, node: Any, deferred: bool
    ) -> None:
        if self.dag.needs_materialize(step_name):
            output, had_error, exc = await self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
            if had_error:
                await self._handle_stream_publish_error(step_name, node, exc)
        else:
            had_error = False
            exc = None
        self.state.set_output(step_name, output)
        self._notify_observers(step_name, output)
        if deferred:
            await self._emit_step_result(node, step_name, output, had_error, exc)

    async def publish(self, step_name: str, output: Any, node: Any) -> None:
        """Publish the output of a step."""
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
