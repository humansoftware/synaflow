import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from contextlib import AsyncExitStack
from typing import Any, Callable

from synaflow.core.types import OnError, StepMode
from synaflow.core.exceptions import PipelineStopException, ThresholdExceededException
from synaflow.core.dag import DagNode
from synaflow.execution.context_managers import (
    is_async_context_manager_instance,
    is_sync_context_manager_instance,
)
from synaflow.execution.state import ExecutionState
from synaflow.execution.async_engine.event_dispatch import AsyncEventDispatcher
from synaflow.execution.async_engine.step_lifecycle import AsyncStepLifecycle
from synaflow.execution.async_engine.lifecycle_stream import AsyncLifecycleStream
from synaflow.execution.stats import StepRunStats
from synaflow.execution.threshold import (
    check_threshold,
    wrap_threshold_raise_if_manual,
    compute_completed_all_inputs_for_all,
    has_threshold,
)
from synaflow.execution.async_engine.iterator_utils import AsyncQueueBranch
from synaflow.execution.async_engine.constants import EOF_MARKER


def _wrap_started_stream(
    it: AsyncIterator[Any]
    | AsyncGenerator[Any, Any]
    | Iterator[Any]
    | Generator[Any, Any, Any],
    fire_started: Callable[[], Any],
) -> AsyncLifecycleStream:
    return AsyncLifecycleStream(it, on_start=fire_started)


async def collect_async_iterator(
    step_name: str,
    value: Any,
    on_error_val: OnError,
    events: AsyncEventDispatcher,
) -> tuple[list[Any], bool, BaseException | None]:
    items = []

    async def handle_error(exc: BaseException, count: int) -> None:
        await events.handle_error(
            step_name,
            exc,
            success_count=count,
            error_count=1,
            completed_all_inputs=False,
        )

    stream = AsyncLifecycleStream(value, on_item=items.append, on_error=handle_error)
    try:
        async for _ in stream:
            pass
        return items, False, None
    except PipelineStopException:
        raise
    except Exception as exc:
        if on_error_val == OnError.STOP:
            raise PipelineStopException(step_name=step_name, cause=exc) from exc
        return items, True, exc


def wrap_deferred_output(
    step_name: str,
    output: Any,
    dag_node: DagNode,
    events: AsyncEventDispatcher,
    stats: StepRunStats,
) -> Any:
    if has_threshold(dag_node):
        return output

    async def handle_end(count: int) -> None:
        if dag_node.mode == StepMode.ALL:
            stats.set_counts(count, 0)

        if has_threshold(dag_node):
            return
        await events.step_completed(
            dag_node,
            step_name,
            success_count=stats.success_count,
            error_count=stats.error_count,
            completed_all_inputs=True,
        )

    return AsyncLifecycleStream(output, on_end=handle_end)


class AsyncStepRunner:
    def __init__(
        self,
        step_name: str,
        fn: Callable[..., Any],
        on_error: OnError,
        max_in_flight: int,
        dataset_param_names: dict[str, str],
        arguments: dict[str, Any],
        resource_stack: AsyncExitStack,
        is_each_mode: bool,
        should_drain: bool,
        publisher: Callable[[Any], Any],
        state: ExecutionState,
        events: AsyncEventDispatcher,
        stats: StepRunStats,
        dag_node: DagNode,
        each_mode_deps: list[str] | None = None,
        deferred_resources: dict[str, Any] | None = None,
        upstream_max_in_flight: dict[str, int] | None = None,
    ) -> None:
        self.step_name = step_name
        self.fn = fn
        self.on_error = on_error
        self.max_in_flight = max_in_flight
        self.dataset_param_names = dataset_param_names
        self.arguments = arguments
        self.resource_stack = resource_stack
        self.deferred_resources = deferred_resources or {}
        self.is_each_mode = is_each_mode
        self.should_drain = should_drain
        self.publisher = publisher
        self.state = state
        self.events = events
        self.stats = stats
        self.each_mode_deps = each_mode_deps
        self.dag_node = dag_node
        self.upstream_max_in_flight = upstream_max_in_flight or {}

    async def run(self) -> None:
        stats = self.stats
        step_name = self.step_name
        node = self.dag_node
        unrolled = self.each_mode_deps or []
        lifecycle = AsyncStepLifecycle(node, step_name, self.events, stats)

        try:
            if not unrolled and not inspect.isasyncgenfunction(self.fn):
                await lifecycle.start()
            output = await self._execute_step(unrolled, lifecycle)
            if self._is_stream_output(output):
                output = _wrap_started_stream(output, lifecycle.start)
            output = self._attach_cleanup(output, self.arguments)
            await self._emit_immediate_completion(output, unrolled, lifecycle)
            res = self.publisher(output)
            if inspect.iscoroutine(res) or (
                res is not None and inspect.isawaitable(res)
            ):
                await res
        except PipelineStopException as exc:
            lifecycle.record_error(1)
            await lifecycle.finish(exception=exc, completed_all_inputs=False)
            raise
        except ThresholdExceededException as exc:
            if exc.step_name != step_name:
                pass
            elif unrolled and has_threshold(node):
                pass
            elif not unrolled:
                completed_all_inputs = compute_completed_all_inputs_for_all(
                    node, self.arguments, exc
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
                lifecycle.set_counts(exc.success_count, exc.error_count)
                await lifecycle.finish(exception=exc, completed_all_inputs=True)
            raise
        except Exception as exc:
            await self.events.handle_error(step_name, exc)
            lifecycle.record_error(1)
            await lifecycle.finish(exception=exc, completed_all_inputs=False)
            if self.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
        finally:
            if "output" not in locals() or not self._is_stream_output(output):
                await self._close_managed_streams(self.arguments)
            await self.resource_stack.aclose()

    async def _execute_step(
        self, unrolled: list[str], lifecycle: AsyncStepLifecycle
    ) -> Any:
        if unrolled:
            return await self._unroll_step(unrolled, lifecycle)
        return await self._call_fn(self.fn, self.arguments)

    async def _call_fn(self, fn: Any, kwargs: dict) -> Any:
        if inspect.isasyncgenfunction(fn):
            return fn(**kwargs)
        return await fn(**kwargs)

    async def _unroll_step(
        self, unrolled: list[str], lifecycle: AsyncStepLifecycle
    ) -> Any:
        queues = {}
        for dep in unrolled:
            value = self.state.get_output(dep, self.step_name)
            if isinstance(value, (asyncio.Queue, AsyncQueueBranch)):
                queues[dep] = value
            else:
                # Non-queue inputs are already fully available in memory, so
                # max_in_flight does not apply here. Size the queue to avoid
                # deadlocking while preloading eager values for EACH-mode use.
                if isinstance(value, (list, tuple, set)):
                    q = asyncio.Queue(maxsize=max(1, len(value)) + 1)
                else:
                    upstream_max = self.upstream_max_in_flight.get(dep, 1)
                    maxsize = max(2, upstream_max + 1)
                    q = asyncio.Queue(maxsize=maxsize)
                if isinstance(value, (list, tuple, set)):
                    for item in value:
                        await q.put(item)
                elif value is not None:
                    await q.put(value)
                await q.put(EOF_MARKER)
                queues[dep] = q

        completed = set()

        async def generate() -> AsyncGenerator[Any, None]:
            invocation_count = 0
            error_count = 0
            # Reset runtime stats on the stats object
            self.stats.set_counts(0, 0)

            try:
                while len(completed) < len(unrolled):
                    item_args = dict(self.arguments)
                    for dep in unrolled:
                        if dep in completed:
                            param = self.dataset_param_names.get(dep, dep)
                            item_args[param] = None
                            continue

                        item = await queues[dep].get()
                        if item is EOF_MARKER:
                            completed.add(dep)
                            param = self.dataset_param_names.get(dep, dep)
                            item_args[param] = None
                        elif isinstance(item, Exception):
                            raise item
                        else:
                            param = self.dataset_param_names.get(dep, dep)
                            item_args[param] = item
                    if len(completed) == len(unrolled):
                        break
                    invocation_count += 1
                    has_error = False
                    exc_to_raise = None
                    result = None
                    async with AsyncExitStack() as item_stack:
                        for param, factory in self.deferred_resources.items():
                            val = factory()
                            if inspect.isawaitable(val):
                                val = await val
                            if is_async_context_manager_instance(val):
                                val = await item_stack.enter_async_context(val)
                            elif is_sync_context_manager_instance(val):
                                val = item_stack.enter_context(val)
                            item_args[param] = val
                        try:
                            result = await self._call_fn(self.fn, item_args)
                        except PipelineStopException as exc:
                            exc_to_raise = exc
                        except Exception as exc:
                            has_error = True
                            error_count += 1
                            await self.events.handle_error(
                                self.step_name,
                                wrap_threshold_raise_if_manual(exc, self.step_name),
                                success_count=invocation_count - error_count,
                                error_count=error_count,
                                completed_all_inputs=False,
                            )
                            if self.on_error == OnError.STOP:
                                exc_to_raise = PipelineStopException(
                                    step_name=self.step_name, cause=exc
                                )

                    if exc_to_raise is not None:
                        raise exc_to_raise
                    if not has_error:
                        yield result
                # pos-loop, before generator ends
                if has_threshold(self.dag_node):
                    try:
                        check_threshold(
                            self.step_name,
                            self.dag_node,
                            invocation_count,
                            error_count,
                        )
                    except ThresholdExceededException as exc:
                        lifecycle.set_counts(exc.success_count, exc.error_count)
                        await lifecycle.finish(exception=exc, completed_all_inputs=True)
                        raise
                    success_count = invocation_count - error_count
                    lifecycle.set_counts(success_count, error_count)
                    await lifecycle.finish(completed_all_inputs=True)
                else:
                    check_threshold(
                        self.step_name,
                        self.dag_node,
                        invocation_count,
                        error_count,
                    )
            finally:
                self.stats.set_counts(invocation_count - error_count, error_count)

        if self.should_drain:
            async for _ in generate():
                pass
            return None
        return generate()

    async def _emit_immediate_completion(
        self, output: Any, unrolled: list[str], lifecycle: AsyncStepLifecycle
    ) -> None:
        if unrolled or isinstance(
            output, (Iterator, Generator, AsyncIterator, AsyncGenerator)
        ):
            return
        success_count = 1
        if isinstance(output, (list, tuple, set)):
            success_count = len(output)
        lifecycle.record_success(success_count)
        await lifecycle.finish(completed_all_inputs=True)

    def _attach_cleanup(self, output: Any, arguments: dict[str, Any]) -> Any:
        if not isinstance(output, (AsyncIterator, AsyncGenerator)):
            return output

        async def wrapped() -> AsyncGenerator[Any, None]:
            try:
                async for item in output:
                    yield item
            finally:
                await self._close_managed_streams(arguments)

        return wrapped()

    async def _close_managed_streams(self, arguments: dict[str, Any]) -> None:
        for value in arguments.values():
            if isinstance(value, AsyncQueueBranch):
                value.close()
                continue
            if inspect.isasyncgen(value):
                try:
                    await value.aclose()
                except Exception:
                    pass

    @staticmethod
    def _is_stream_output(output: Any) -> bool:
        return isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator))
