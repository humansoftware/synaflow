import inspect
from collections.abc import Callable, Generator, Iterator
from typing import Any
from contextlib import ExitStack

from synaflow.core.types import OnError, StepMode
from synaflow.core.exceptions import PipelineStopException, ThresholdExceededException
from synaflow.core.dag import DagNode
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
from synaflow.execution.runtime_contract_validation import (
    satisfies_sync_iterator_contract,
)
from synaflow.execution.sync_engine.lifecycle_stream import LifecycleStream


def _wrap_started_stream(
    it: Iterator[Any],
    fire_started: Callable[[], None],
) -> LifecycleStream:
    return LifecycleStream(it, on_start=fire_started)


def collect_iterator(
    step_name: str,
    value: Iterator[Any],
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


def wrap_deferred_output(
    step_name: str,
    output: Any,
    dag_node: DagNode,
    events: EventDispatcher,
    stats: StepRunStats,
) -> Any:
    if has_threshold(dag_node):
        return output

    def handle_end(count: int) -> None:
        if dag_node.mode == StepMode.ALL:
            stats.set_counts(count, 0)

        if has_threshold(dag_node):
            return
        events.step_completed(
            dag_node,
            step_name,
            success_count=stats.success_count,
            error_count=stats.error_count,
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
        dag_node: DagNode,
        each_mode_deps: list[str] | None = None,
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
        self.dag_node = dag_node

    def run(self) -> None:
        unrolled = []
        if self.is_each_mode:
            unrolled = (
                self.each_mode_deps
                if self.each_mode_deps is not None
                else list(self.dataset_param_names.keys())
            )

        lifecycle = StepLifecycle(
            self.dag_node, self.step_name, self.events, self.stats
        )
        output_contract = self.dag_node.output_contract
        expects_sync_stream = (
            output_contract is not None
            and output_contract.runtime_kind == "sync_stream"
        )

        try:
            if not unrolled and not inspect.isgeneratorfunction(self.fn):
                lifecycle.start()
            output = self._execute_step(unrolled, lifecycle)
            if expects_sync_stream and satisfies_sync_iterator_contract(output):
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
            elif unrolled and has_threshold(self.dag_node):
                # This step's generate() already dispatched FAILED (path A).
                pass
            elif not unrolled:
                # ALL-mode manual raise by this step (path B, escape hatch)
                completed_all_inputs = compute_completed_all_inputs_for_all(
                    self.dag_node, self.arguments, exc
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
            if "output" not in locals() or not expects_sync_stream:
                self._close_managed_streams(self.arguments)
            self.resource_stack.close()

    def _execute_step(self, unrolled: list[str], lifecycle: StepLifecycle) -> Any:
        if unrolled:
            return self._unroll_step(unrolled, lifecycle)
        return self.fn(**self.arguments)

    def _emit_immediate_completion(
        self, output: Any, unrolled: list[str], lifecycle: StepLifecycle
    ) -> None:
        dag_node = self.dag_node
        output_contract = dag_node.output_contract
        if unrolled or (
            output_contract is not None
            and output_contract.completion_policy == "on_exhaustion"
        ):
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

        def generate() -> Generator[Any, None, None]:
            invocation_count = 0
            error_count = 0
            # Reset runtime stats on the stats object
            self.stats.set_counts(0, 0)

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
                        lifecycle.finish(exception=exc, completed_all_inputs=True)
                        raise
                    success_count = invocation_count - error_count
                    lifecycle.set_counts(success_count, error_count)
                    lifecycle.finish(completed_all_inputs=True)
                else:
                    check_threshold(
                        self.step_name,
                        self.dag_node,
                        invocation_count,
                        error_count,
                    )
            finally:
                self.stats.set_counts(invocation_count - error_count, error_count)
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
        dag_node = self.dag_node
        output_contract = dag_node.output_contract
        if (
            output_contract is None
            or output_contract.runtime_kind != "sync_stream"
            or not satisfies_sync_iterator_contract(output)
        ):
            return output

        def wrapped() -> Iterator[Any]:
            try:
                yield from output
            finally:
                self._close_managed_streams(arguments)

        return wrapped()
