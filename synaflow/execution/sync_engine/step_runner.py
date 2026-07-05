import inspect
from collections.abc import Callable, Iterator
from typing import Any
from contextlib import ExitStack

from synaflow.core.types import OnError, StepMode
from synaflow.core.exceptions import PipelineStopException, ThresholdExceededException
from synaflow.execution.state import ExecutionState
from synaflow.execution.sync_engine.event_dispatch import EventDispatcher
from synaflow.execution.sync_engine.step_lifecycle import StepLifecycle
from synaflow.execution.stats import StepRunStats
from synaflow.execution.threshold import (
    check_threshold,
    wrap_threshold_raise_if_manual,
    compute_completed_all_inputs_for_all,
    has_threshold,
)
from synaflow.execution.sync_engine.lifecycle_stream import LifecycleStream


class StepConfig:
    def __init__(
        self,
        observers: list[Any],
        mode: StepMode,
        on_error: OnError,
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


def _wrap_started_stream(
    it: Iterator[Any],
    fire_started: Callable[[], None],
) -> LifecycleStream:
    return LifecycleStream(it, on_start=fire_started)


def _collect_iterator(
    step_name: str,
    value: Iterator,
    on_error_val: OnError,
    events: EventDispatcher,
) -> tuple[list[Any], bool, BaseException | None]:
    items = []

    def handle_error(exc: BaseException, count: int) -> None:
        events.handle_error(
            step_name,
            exc,
            success_count=count,
            error_count=1,
            completed_all_inputs=False,
        )
        if on_error_val == OnError.STOP:
            raise PipelineStopException(step_name=step_name, cause=exc) from exc

    it = LifecycleStream(value, on_item=items.append, on_error=handle_error)
    try:
        for _ in it:
            pass
        return items, False, None
    except PipelineStopException:
        raise
    except Exception as exc:
        return items, True, exc


def _wrap_deferred_output(
    step_name: str,
    output: Any,
    node: Any,
    events: EventDispatcher,
) -> Any:
    if has_threshold(node):
        return output

    def handle_end(count: int) -> None:
        if node.mode == StepMode.ALL:
            node._runtime_invocation_count = count
            node._runtime_error_count = 0

        if has_threshold(node):
            return
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", 0)
        events.step_completed(
            node,
            step_name,
            success_count=real_invocation_count - real_error_count,
            error_count=real_error_count,
            completed_all_inputs=True,
        )

    return LifecycleStream(output, on_end=handle_end)


class StepRunner:
    def __init__(
        self,
        step_name: str,
        fn: Callable[..., Any],
        on_error: OnError,
        max_in_flight: int,
        dataset_param_names: dict[str, str],
        arguments: dict[str, Any],
        resource_stack: ExitStack,
        is_each_mode: bool,
        should_drain: bool,
        publisher: Callable[[Any], None],
        state: ExecutionState,
        events: EventDispatcher,
        stats: StepRunStats,
        each_mode_deps: list[str] | None = None,
        step_config: StepConfig | None = None,
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

        if step_config is None:
            step_config = StepConfig(
                observers=[],
                mode=StepMode.EACH if is_each_mode else StepMode.ALL,
                on_error=on_error,
                max_in_flight=max_in_flight,
                dataset_param_names=dataset_param_names,
            )
        self.step_config = step_config

    def run(self) -> None:
        unrolled = []
        if self.is_each_mode:
            unrolled = (
                self.each_mode_deps
                if self.each_mode_deps is not None
                else list(self.dataset_param_names.keys())
            )

        lifecycle = StepLifecycle(
            self.step_config, self.step_name, self.events, self.stats
        )

        try:
            if not unrolled and not inspect.isgeneratorfunction(self.fn):
                lifecycle.start()
            output = self._execute_step(unrolled, lifecycle)
            if isinstance(output, Iterator):
                output = _wrap_started_stream(output, lifecycle.start)
            output = self._attach_cleanup(output, self.arguments)
            self._emit_immediate_completion(output, unrolled, lifecycle)
            self.publisher(output)
        except PipelineStopException as exc:
            lifecycle.record_error(1)
            lifecycle.finish(exception=exc, completed_all_inputs=False)
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
                self.events.handle_error(
                    self.step_name,
                    exc,
                    success_count=exc.success_count,
                    error_count=exc.error_count,
                    completed_all_inputs=completed_all_inputs,
                )
                lifecycle.set_counts(exc.success_count, exc.error_count)
                lifecycle.finish(
                    exception=exc, completed_all_inputs=completed_all_inputs
                )
            else:
                # EACH mode, no threshold configured (should not reach here
                # per build-time validation, but handle defensively)
                lifecycle.set_counts(exc.success_count, exc.error_count)
                lifecycle.finish(exception=exc, completed_all_inputs=True)
            raise
        except Exception as exc:
            self.events.handle_error(self.step_name, exc)
            lifecycle.record_error(1)
            lifecycle.finish(exception=exc, completed_all_inputs=False)
            if self.on_error == OnError.STOP:
                raise PipelineStopException(
                    step_name=self.step_name, cause=exc
                ) from exc
        finally:
            if "output" not in locals() or not isinstance(output, Iterator):
                self._close_managed_streams(self.arguments)
            self.resource_stack.close()

    def _execute_step(self, unrolled: list[str], lifecycle: StepLifecycle) -> Any:
        if unrolled:
            return self._unroll_step(unrolled, lifecycle)
        return self.fn(**self.arguments)

    def _emit_immediate_completion(
        self, output: Any, unrolled: list[str], lifecycle: StepLifecycle
    ) -> None:
        if unrolled or isinstance(output, Iterator):
            return
        success_count = 1
        if isinstance(output, (list, tuple, set)):
            success_count = len(output)
        lifecycle.record_success(success_count)
        lifecycle.finish(completed_all_inputs=True)

    def _unroll_step(self, unrolled: list[str], lifecycle: StepLifecycle) -> Any:
        """Call fn once per item-tuple. Exhausted streams yield None."""
        iterators = {}
        for dep in unrolled:
            source = self.state.get_output(dep, self.step_name)
            iterators[dep] = iter(source if source is not None else [])

        on_err = self.on_error

        def generate():
            invocation_count = 0
            error_count = 0
            # Reset runtime stats on the node/config so multiple executor runs
            # on the same pipeline don't leak counts across runs.
            if self.step_config._dag_node is not None:
                self.step_config._dag_node._runtime_error_count = 0
                self.step_config._dag_node._runtime_invocation_count = 0
            else:
                self.step_config._runtime_error_count = 0
                self.step_config._runtime_invocation_count = 0

            try:
                while True:
                    item_args = dict(self.arguments)
                    exhausted = 0
                    for dep in unrolled:
                        try:
                            value = next(iterators[dep])
                        except StopIteration:
                            value = None
                            exhausted += 1
                        param = self.dataset_param_names.get(dep, dep)
                        item_args[param] = value
                    if exhausted == len(unrolled):
                        break

                    invocation_count += 1
                    try:
                        yield self.fn(**item_args)
                    except PipelineStopException:
                        # Propagate STOP from upstream producer so the consumer
                        # also stops, even without forced materialization.
                        raise
                    except Exception as exc:
                        error_count += 1
                        self.events.handle_error(
                            self.step_name,
                            wrap_threshold_raise_if_manual(exc, self.step_name),
                            success_count=invocation_count - error_count,
                            error_count=error_count,
                            completed_all_inputs=False,
                        )
                        if on_err == OnError.STOP:
                            raise PipelineStopException(
                                step_name=self.step_name, cause=exc
                            ) from exc
                # post-loop, before generator ends
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
                        lifecycle.finish(exception=exc, completed_all_inputs=True)
                        raise
                    success_count = invocation_count - error_count
                    lifecycle.set_counts(success_count, error_count)
                    lifecycle.finish(completed_all_inputs=True)
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
                self._close_managed_streams(iterators)

        if self.should_drain:
            for _ in generate():
                pass
            return None
        return generate()

    def _close_managed_streams(self, arguments: dict[str, Any]) -> None:
        from synaflow.execution.sync_handoff import SyncQueueIterator

        for value in arguments.values():
            if isinstance(value, SyncQueueIterator):
                try:
                    value.close()
                except Exception:
                    pass

    def _attach_cleanup(self, output: Any, arguments: dict[str, Any]) -> Any:
        if not isinstance(output, Iterator):
            return output

        def wrapped():
            try:
                yield from output
            finally:
                self._close_managed_streams(arguments)

        return wrapped()
