import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from contextlib import AsyncExitStack
from typing import Any, Callable

from synaflow.core.types import OnError, StepMode
from synaflow.core.exceptions import PipelineStopException, ThresholdExceededException
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
from synaflow.core.dag import Dag


class AsyncStepConfig:
    def __init__(
        self,
        observers: list[Any],
        mode: Any,
        on_error: Any,
        max_in_flight: int,
        dataset_param_names: dict[str, str],
    ) -> None:
        self.observers = observers
        self.mode = mode
        self.on_error = on_error
        self.max_in_flight = max_in_flight
        self.dataset_param_names = dataset_param_names
        self.error_threshold_absolute: int | None = None
        self.error_threshold_pct: float | None = None
        self._dag_node: Any = None
        self._runtime_error_count: int = 0
        self._runtime_invocation_count: int = 0


def _wrap_started_stream(
    it: AsyncIterator[Any]
    | AsyncGenerator[Any, Any]
    | Iterator[Any]
    | Generator[Any, Any, Any],
    fire_started: Callable[[], Any],
) -> AsyncLifecycleStream:
    return AsyncLifecycleStream(it, on_start=fire_started)


async def _collect_async_iterator(
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


def _wrap_deferred_output(
    step_name: str,
    output: Any,
    node: Any,
    events: AsyncEventDispatcher,
) -> Any:
    if has_threshold(node):
        return output

    async def handle_end(count: int) -> None:
        if node.mode == StepMode.ALL:
            node._runtime_invocation_count = count
            node._runtime_error_count = 0

        if has_threshold(node):
            return
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", 0)
        await events.step_completed(
            node,
            step_name,
            success_count=real_invocation_count - real_error_count,
            error_count=real_error_count,
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
        each_mode_deps: list[str] | None = None,
        step_config: AsyncStepConfig | None = None,
        dag: Dag | None = None,
    ) -> None:
        self.step_name = step_name
        self.fn = fn
        self.on_error = on_error
        self.max_in_flight = max_in_flight
        self.dataset_param_names = dataset_param_names
        self.arguments = arguments
        self.resource_stack = resource_stack
        self.is_each_mode = is_each_mode
        self.should_drain = should_drain
        self.publisher = publisher
        self.state = state
        self.events = events
        self.stats = stats
        self.each_mode_deps = each_mode_deps
        self.dag = dag

        if step_config is None:
            step_config = AsyncStepConfig(
                observers=[],
                mode=StepMode.EACH if is_each_mode else StepMode.ALL,
                on_error=on_error,
                max_in_flight=max_in_flight,
                dataset_param_names=dataset_param_names,
            )
        self.step_config = step_config

    async def run(self) -> None:
        unrolled = []
        if self.is_each_mode:
            unrolled = (
                self.each_mode_deps
                if self.each_mode_deps is not None
                else list(self.dataset_param_names.keys())
            )

        lifecycle = AsyncStepLifecycle(
            self.step_config, self.step_name, self.events, self.stats
        )

        try:
            if not unrolled and not inspect.isasyncgenfunction(self.fn):
                await lifecycle.start()
            output = await self._execute_step(unrolled, lifecycle)
            if self._is_stream_output(output):
                output = _wrap_started_stream(output, lifecycle.start)
            output = self._attach_cleanup(output, self.arguments)
            await self._emit_immediate_completion(output, unrolled, lifecycle)

            res = self.publisher(output)
            if inspect.iscoroutine(res):
                await res
        except PipelineStopException as exc:
            lifecycle.record_error(1)
            await lifecycle.finish(exception=exc, completed_all_inputs=False)
            raise
        except ThresholdExceededException as exc:
            if exc.step_name != self.step_name:
                # Upstream threshold propagating through this consumer:
                # the producer's generate() already dispatched FAILED.
                pass
            elif unrolled and has_threshold(self.step_config):
                # This step's generate() already dispatched FAILED (path A).
                pass
            elif not unrolled:
                # ALL-mode manual raise by this step (path B, escape hatch)
                completed_all_inputs = compute_completed_all_inputs_for_all(
                    self.step_config, self.arguments, exc
                )
                await self.events.handle_error(
                    self.step_name,
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
            await self.events.handle_error(self.step_name, exc)
            lifecycle.record_error(1)
            await lifecycle.finish(exception=exc, completed_all_inputs=False)
            if self.on_error == OnError.STOP:
                raise PipelineStopException(
                    step_name=self.step_name, cause=exc
                ) from exc
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
                producer_node = self.dag.get(dep) if self.dag else None
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

        async def generate() -> AsyncGenerator[Any, None]:
            invocation_count = 0
            error_count = 0
            # Reset runtime stats on the node so multiple executor runs
            # on the same pipeline don't leak counts across runs.
            if self.step_config._dag_node is not None:
                self.step_config._dag_node._runtime_error_count = 0
                self.step_config._dag_node._runtime_invocation_count = 0
            else:
                self.step_config._runtime_error_count = 0
                self.step_config._runtime_invocation_count = 0

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
                    try:
                        yield await self._call_fn(self.fn, item_args)
                    except PipelineStopException:
                        # Propagate STOP from upstream producer so the consumer
                        # also stops, even without forced materialization.
                        raise
                    except Exception as exc:
                        error_count += 1
                        await self.events.handle_error(
                            self.step_name,
                            wrap_threshold_raise_if_manual(exc, self.step_name),
                            success_count=invocation_count - error_count,
                            error_count=error_count,
                            completed_all_inputs=False,
                        )
                        if self.on_error == OnError.STOP:
                            raise PipelineStopException(
                                step_name=self.step_name, cause=exc
                            ) from exc
                # pos-loop, before generator ends
                if has_threshold(self.step_config):
                    try:
                        check_threshold(
                            self.step_name,
                            self.step_config,
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
                        self.step_name, self.step_config, invocation_count, error_count
                    )
            finally:
                if self.step_config._dag_node is not None:
                    self.step_config._dag_node._runtime_error_count = error_count
                    self.step_config._dag_node._runtime_invocation_count = (
                        invocation_count
                    )
                else:
                    self.step_config._runtime_error_count = error_count
                    self.step_config._runtime_invocation_count = invocation_count

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
