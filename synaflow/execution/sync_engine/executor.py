import inspect
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import (
    PipelineStopException,
    ThresholdExceededException,
)
from synaflow.execution.sync_engine.event_dispatch import EventDispatcher
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
from synaflow.execution.sync_handoff import SyncFanout
from synaflow.execution.bounded_iterator import BoundedIterator
from synaflow.execution.state import ExecutionState
from synaflow.execution.lifecycle_stream import LifecycleStream
from .argument_builder import ArgumentBuilder
from .step_lifecycle import StepLifecycle


# ---------------------------------------------------------------------------
# Runtime helpers (no flags needed on DagNode)
# ---------------------------------------------------------------------------


def _wrap_started_stream(it: Any, fire_started: Any) -> Any:
    return LifecycleStream(it, on_start=fire_started)


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class PipelineExecutor:
    def __init__(
        self,
        dag: Dag,
        *,
        step_output_observers: list = None,
        overrides: ExecutionOverrides | None = None,
        resource_factories: dict[str, Any] | None = None,
    ):
        self.dag = dag
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})
        self.run_id = str(uuid.uuid4())

        self.state = ExecutionState(self.dag)
        self.scope = ArgumentBuilder(
            self.dag, self.state, self._overrides, self._resource_factories
        )
        self.events = EventDispatcher(self.dag, self.run_id, self._overrides)
        self._step_output_observers = step_output_observers or []
        self._active_fanouts: list[SyncFanout] = []
        self._observer_threads: list[threading.Thread] = []

    @property
    def outputs(self) -> dict[str, Any]:
        return self.state.raw_outputs()

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, params: Any) -> None:
        self.scope.seed_runtime_inputs(params)

        self.events.pipeline_started()
        completed_cleanly = False
        try:
            self._run_graph()
            completed_cleanly = True
        except PipelineStopException as exc:
            self.events.pipeline_failed(
                step_name=exc.step_name,
                exception=exc.cause or exc,
            )
            raise
        except ThresholdExceededException as exc:
            self.events.pipeline_failed(
                step_name=exc.step_name,
                exception=exc,
            )
            raise
        except Exception as exc:
            self.events.pipeline_failed(step_name=None, exception=exc)
            raise
        finally:
            self.cleanup()
        if completed_cleanly:
            self.events.pipeline_completed()

    def _run_graph(self) -> None:
        cond = threading.Condition()
        running_tasks = set()
        finished_tasks = set()
        ready_tasks = set()
        fatal_error = None
        completed_cleanly = True

        with ThreadPoolExecutor(max_workers=max(1, len(self.dag.steps))) as pool:

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
                    future = pool.submit(self._run_step, s)
                    future.add_done_callback(
                        lambda fut, step_name=s: step_done(fut, step_name)
                    )

            def step_done(future, step_name):
                nonlocal fatal_error, completed_cleanly
                with cond:
                    running_tasks.remove(step_name)
                    finished_tasks.add(step_name)
                    try:
                        future.result()
                    except BaseException as exc:
                        if fatal_error is None:
                            fatal_error = exc
                        completed_cleanly = False
                        self.abort(exc)

                    if completed_cleanly:
                        check_new_ready_steps()
                    cond.notify_all()

            with cond:
                check_new_ready_steps()
                while running_tasks:
                    cond.wait()

        if fatal_error is not None:
            raise fatal_error

    def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        arguments, resource_stack = self.scope.build_arguments(step_name, node)
        unrolled = self.dag.each_inputs(step_name)

        lifecycle = StepLifecycle(node, step_name, self.events)

        try:
            if not unrolled and not inspect.isgeneratorfunction(node.fn):
                lifecycle.start()
            output = self._execute_step(step_name, node, arguments, unrolled, lifecycle)
            if isinstance(output, Iterator):
                output = _wrap_started_stream(output, lifecycle.start)
            output = self.scope.attach_cleanup(output, arguments)
            self._emit_immediate_completion(output, unrolled, lifecycle)
            if not self.dag.is_hidden_step(step_name):
                self.publish(step_name, output, node)
        except PipelineStopException as exc:
            lifecycle.record_error(1)
            lifecycle.finish(exception=exc, completed_all_inputs=False)
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
                self.events.handle_error(
                    step_name,
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
            self.events.handle_error(step_name, exc)
            lifecycle.record_error(1)
            lifecycle.finish(exception=exc, completed_all_inputs=False)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
        finally:
            if "output" not in locals() or not isinstance(output, Iterator):
                self.scope.close_managed_streams(arguments)
            resource_stack.close()

    def _execute_step(self, step_name, node, arguments, unrolled, lifecycle):
        if unrolled:
            return self._unroll_step(step_name, node, arguments, unrolled, lifecycle)
        return node.fn(**arguments)

    def _emit_immediate_completion(self, output, unrolled, lifecycle: StepLifecycle):
        if unrolled or isinstance(output, Iterator):
            return
        success_count = 1
        if isinstance(output, (list, tuple, set)):
            success_count = len(output)
        lifecycle.record_success(success_count)
        lifecycle.finish(completed_all_inputs=True)

    def _unroll_step(self, step_name, node, base_args, unrolled, lifecycle):
        """Call fn once per item-tuple. Exhausted streams yield None.
        If terminal (sink), consume eagerly without producing output."""
        iterators = {}
        for dep in unrolled:
            source = self.state.get_output(dep, step_name)
            iterators[dep] = iter(source if source is not None else [])

        on_err = node.on_error

        def generate():
            invocation_count = 0
            error_count = 0
            # Reset runtime stats on the node so multiple executor runs
            # on the same pipeline don't leak counts across runs.
            node._runtime_error_count = 0
            node._runtime_invocation_count = 0
            try:
                while True:
                    item_args = dict(base_args)
                    exhausted = 0
                    for dep in unrolled:
                        try:
                            value = next(iterators[dep])
                        except StopIteration:
                            value = None
                            exhausted += 1
                        param = node.dataset_param_names.get(dep, dep)
                        item_args[param] = value
                    if exhausted == len(unrolled):
                        break

                    invocation_count += 1
                    try:
                        yield node.fn(**item_args)
                    except PipelineStopException:
                        # Propagate STOP from upstream producer so the consumer
                        # also stops, even without forced materialization.
                        raise
                    except Exception as exc:
                        error_count += 1
                        self.events.handle_error(
                            step_name,
                            wrap_threshold_raise_if_manual(exc, step_name),
                            success_count=invocation_count - error_count,
                            error_count=error_count,
                            completed_all_inputs=False,
                        )
                        if on_err == OnError.STOP:
                            raise PipelineStopException(
                                step_name=step_name, cause=exc
                            ) from exc
                # pos-loop, before generator ends
                if has_threshold(node):
                    try:
                        check_threshold(step_name, node, invocation_count, error_count)
                    except ThresholdExceededException as exc:
                        lifecycle.set_counts(exc.success_count, exc.error_count)
                        lifecycle.finish(exception=exc, completed_all_inputs=True)
                        raise
                    success_count = invocation_count - error_count
                    lifecycle.set_counts(success_count, error_count)
                    lifecycle.finish(completed_all_inputs=True)
                else:
                    check_threshold(step_name, node, invocation_count, error_count)
            finally:
                node._runtime_error_count = error_count
                node._runtime_invocation_count = invocation_count
                self.scope.close_managed_streams(iterators)

        if self.dag.is_terminal_step(step_name):
            for _ in generate():
                pass
            return None
        return generate()

    # ------------------------------------------------------------------
    # Dataflow routing & publishing (formerly StreamPublisher)
    # ------------------------------------------------------------------

    def publish(self, step_name: str, output: Any, node: Any) -> None:
        deferred = node.mode == StepMode.EACH or (
            node.mode == StepMode.ALL and isinstance(output, Iterator)
        )

        if not isinstance(output, Iterator):
            output = self._notify_observers(step_name, output)
            self._publish_scalar_output(step_name, output, node, deferred)
            return

        consumers = self.dag.consumers_of(step_name)

        if self.dag.needs_materialize(step_name):
            self._materialize_stream_output(
                step_name, output, node, consumers, deferred
            )
            return

        if deferred:
            output = self._wrap_deferred_output(step_name, output, node)

        if len(consumers) == 1 and self._step_output_observers:
            self._publish_stream_to_single_consumer(
                step_name, output, node, consumers[0], deferred
            )
            return

        if len(consumers) > 1:
            self._publish_stream_to_multiple_consumers(
                step_name, output, node, consumers
            )
            return

        if len(consumers) == 0 and self._step_output_observers:
            fanout = SyncFanout(
                output,
                max_in_flight=max(1, node.max_in_flight),
                branches=self._observer_branch_names(),
            )
            self._active_fanouts.append(fanout)
            self._start_observer_threads(
                step_name, fanout, self._observer_branch_names()
            )
            fanout.start()
            return

        output = self._notify_observers(step_name, output)
        self.state.set_output(step_name, self._maybe_wrap_stream(output, node))

    def abort(self, exception: BaseException | None = None) -> None:
        for fanout in self._active_fanouts:
            fanout.abort(exception)

    def cleanup(self) -> None:
        for fanout in self._active_fanouts:
            fanout.join()
        self._active_fanouts.clear()
        for thread in self._observer_threads:
            thread.join()
        self._observer_threads.clear()

    def _maybe_wrap_stream(self, output: Any, node: Any) -> Any:
        if node.max_in_flight <= 1:
            return output
        if not isinstance(output, Iterator):
            return output
        return BoundedIterator(output, node.max_in_flight)

    def _collect_iterator(
        self,
        step_name: str,
        value: Iterator,
    ) -> tuple[list[Any], bool, BaseException | None]:
        items = []

        def handle_error(exc: BaseException, count: int) -> None:
            self.events.handle_error(
                step_name,
                exc,
                success_count=count,
                error_count=1,
                completed_all_inputs=False,
            )
            if self.dag[step_name].on_error == OnError.STOP:
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

    def _apply_materializer(
        self,
        step_name: str,
        value: Any,
        materializer: Any,
        consumer_type: Any = None,
    ) -> tuple[Any, bool, BaseException | None]:
        if materializer is None:
            if isinstance(value, Iterator):
                items, had_error, exc = self._collect_iterator(step_name, value)
                return items, had_error, exc
            return value, False, None

        if isinstance(value, Iterator):
            items, had_error, exc = self._collect_iterator(step_name, value)
            return materializer(items), had_error, exc

        return materializer(value), False, None

    def _notify_observers(self, step_name, output):
        if not self._step_output_observers:
            return output
        if isinstance(output, Iterator):
            pass
        else:
            for observer in self._step_output_observers:
                observer(step_name, output)
        return output

    def _observer_branch_names(self) -> list[str]:
        return [f"__obs{i}" for i, _observer in enumerate(self._step_output_observers)]

    def _collect_observer_items(self, branch) -> list[Any]:
        items = []
        try:
            for item in branch:
                items.append(item)
        except Exception:
            pass
        return items

    def _start_observer_threads(
        self,
        step_name: str,
        fanout: SyncFanout,
        observer_branch_names: list[str],
    ) -> None:
        for branch_name, observer in zip(
            observer_branch_names, self._step_output_observers
        ):
            iterator = fanout.lazy_iterator(branch_name)

            def run_observer(obs=observer, branch_iter=iterator):
                obs(step_name, self._collect_observer_items(branch_iter))

            thread = threading.Thread(target=run_observer, daemon=True)
            thread.start()
            self._observer_threads.append(thread)

    def _materialize_with_events(self, step_name, output, node, consumer_type=None):
        materializer = self.scope.resolve_materializer(step_name, node)
        mat_name = materializer.__name__ if callable(materializer) else None
        self.events.materialization_started(
            step_name,
            node,
            consumer_type,
            mat_name,
        )
        try:
            result, had_error, exc = self._apply_materializer(
                step_name,
                output,
                materializer,
                consumer_type=consumer_type,
            )
            self.events.materialization_completed(
                step_name,
                node,
                consumer_type,
                mat_name,
            )
            return result, had_error, exc
        except PipelineStopException:
            raise
        except Exception as exc:
            self.events.materialization_failed(
                step_name,
                node,
                consumer_type,
                mat_name,
                exception=exc,
            )
            raise

    def _materialize_stream_output(
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
        output, had_error, exc = self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        output = self._notify_observers(step_name, output)
        if deferred:
            self._emit_step_result(node, step_name, output, had_error, exc)
        for consumer in consumers:
            self.state.set_output(step_name, output, consumer)

    def _publish_stream_to_single_consumer(
        self,
        step_name,
        output,
        node,
        consumer,
        deferred,
    ):
        consumer_type = self.dag[consumer].deps.get(step_name)

        if self._step_output_observers and not self.dag.needs_materialize(step_name):
            observer_branches = self._observer_branch_names()
            fanout = SyncFanout(
                output,
                max_in_flight=max(1, node.max_in_flight),
                branches=[consumer, *observer_branches],
            )
            self._active_fanouts.append(fanout)
            self.state.set_output(step_name, fanout.lazy_iterator(consumer), consumer)
            self._start_observer_threads(step_name, fanout, observer_branches)
            fanout.start()
            return
        output, had_error, exc = self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        output = self._notify_observers(step_name, output)
        if deferred:
            self._emit_step_result(node, step_name, output, had_error, exc)
        output = self._maybe_wrap_stream(output, node)
        self.state.set_output(step_name, output, consumer)

    def _publish_stream_to_multiple_consumers(self, step_name, output, node, consumers):
        fanout = SyncFanout(
            output,
            max_in_flight=max(1, node.max_in_flight),
            branches=consumers + self._observer_branch_names(),
        )
        self._active_fanouts.append(fanout)
        for consumer in consumers:
            self.state.set_output(step_name, fanout.lazy_iterator(consumer), consumer)
        self._start_observer_threads(step_name, fanout, self._observer_branch_names())
        fanout.start()

    def _publish_scalar_output(self, step_name, output, node, deferred):
        if self.dag.needs_materialize(step_name):
            output, _, _ = self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
        self.state.set_output(step_name, output)
        if deferred:
            self._emit_step_result(
                node, step_name, output, had_error=False, exception=None
            )

    def _emit_step_result(self, node, step_name, output, had_error, exception=None):
        if has_threshold(node):
            return
        success = len(output) if hasattr(output, "__len__") else 1
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", success)
        if had_error:
            self.events.step_failed(
                node,
                step_name,
                success_count=success,
                error_count=max(real_error_count, 1),
                completed_all_inputs=False,
                exception=exception,
            )
        else:
            self.events.step_completed(
                node,
                step_name,
                success_count=real_invocation_count - real_error_count,
                error_count=real_error_count,
                completed_all_inputs=True,
            )

    def _emit_deferred_completion(self, node, step_name):
        if has_threshold(node):
            return
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", 0)
        self.events.step_completed(
            node,
            step_name,
            success_count=real_invocation_count - real_error_count,
            error_count=real_error_count,
            completed_all_inputs=True,
        )

    def _wrap_deferred_output(self, step_name: str, output: Any, node: Any) -> Any:
        if has_threshold(node):
            return output

        def handle_end(count: int) -> None:
            if node.mode == StepMode.ALL:
                node._runtime_invocation_count = count
                node._runtime_error_count = 0
            self._emit_deferred_completion(node, step_name)

        return LifecycleStream(output, on_end=handle_end)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run(
    pipeline: PipelineDef, params: Any, overrides: ExecutionOverrides | None = None
) -> None:
    if getattr(pipeline, "requires_async_runner", False):
        raise RuntimeError(
            "This pipeline contains async features (async def or AsyncIterator)"
            " and must be executed with async_run()."
        )
    PipelineExecutor(
        pipeline.dag,
        overrides=overrides,
        resource_factories=pipeline.resources,
    ).execute(params)
