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
    StepMode,
)
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.threshold import (
    has_threshold,
)
from synaflow.execution.sync_handoff import SyncFanout
from synaflow.execution.bounded_iterator import BoundedIterator
from synaflow.execution.state import ExecutionState
from .argument_builder import ArgumentBuilder
from synaflow.execution.stats import StepRunStats
from .step_runner import (
    StepRunner,
    StepConfig,
    collect_iterator,
    wrap_deferred_output,
)


# ---------------------------------------------------------------------------
# Runtime helpers (no flags needed on DagNode)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class PipelineExecutor:
    def __init__(
        self,
        dag: Dag,
        *,
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
        self._active_fanouts: list[SyncFanout] = []

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

        step_config = StepConfig(
            observers=node.observers,
            mode=node.mode,
            on_error=node.on_error,
            max_in_flight=node.max_in_flight,
            dataset_param_names=node.dataset_param_names,
        )
        step_config.error_threshold_absolute = getattr(
            node, "error_threshold_absolute", None
        )
        step_config.error_threshold_pct = getattr(node, "error_threshold_pct", None)
        step_config._dag_node = node

        stats = StepRunStats()

        runner = StepRunner(
            step_name=step_name,
            fn=node.fn,
            on_error=node.on_error,
            max_in_flight=node.max_in_flight,
            dataset_param_names=node.dataset_param_names,
            arguments=arguments,
            resource_stack=resource_stack,
            is_each_mode=(node.mode == StepMode.EACH),
            should_drain=self.dag.should_drain_deferred_step(step_name),
            publisher=lambda out: (
                self.publish(step_name, out, node, stats)
                if not self.dag.is_hidden_step(step_name)
                else None
            ),
            state=self.state,
            events=self.events,
            stats=stats,
            each_mode_deps=unrolled,
            step_config=step_config,
        )

        runner.run()

    # ------------------------------------------------------------------
    # Dataflow routing & publishing (formerly StreamPublisher)
    # ------------------------------------------------------------------

    def publish(
        self, step_name: str, output: Any, node: Any, stats: StepRunStats
    ) -> None:
        deferred = node.mode == StepMode.EACH or (
            node.mode == StepMode.ALL and isinstance(output, Iterator)
        )

        if not isinstance(output, Iterator):
            self._publish_scalar_output(step_name, output, node, stats, deferred)
            return

        consumers = self.dag.consumers_of(step_name)

        if self.dag.needs_materialize(step_name):
            self._materialize_stream_output(
                step_name, output, node, stats, consumers, deferred
            )
            return

        if deferred:
            output = wrap_deferred_output(step_name, output, node, self.events, stats)

        if len(consumers) > 1:
            self._publish_stream_to_multiple_consumers(
                step_name, output, node, consumers
            )
            return

        self.state.set_output(step_name, self._maybe_wrap_stream(output, node))

    def abort(self, exception: BaseException | None = None) -> None:
        for fanout in self._active_fanouts:
            fanout.abort(exception)

    def cleanup(self) -> None:
        for fanout in self._active_fanouts:
            fanout.join()
        self._active_fanouts.clear()

    def _maybe_wrap_stream(self, output: Any, node: Any) -> Any:
        if node.max_in_flight <= 1:
            return output
        if not isinstance(output, Iterator):
            return output
        return BoundedIterator(output, node.max_in_flight)

    def _apply_materializer(
        self,
        step_name: str,
        value: Any,
        materializer: Any,
        consumer_type: Any = None,
    ) -> tuple[Any, bool, BaseException | None]:
        if materializer is None:
            if isinstance(value, Iterator):
                items, had_error, exc = collect_iterator(
                    step_name, value, self.dag[step_name].on_error, self.events
                )
                return items, had_error, exc
            return value, False, None

        if isinstance(value, Iterator):
            items, had_error, exc = collect_iterator(
                step_name, value, self.dag[step_name].on_error, self.events
            )
            return materializer(items), had_error, exc

        return materializer(value), False, None

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
        step_name: str,
        output: Any,
        node: Any,
        stats: StepRunStats,
        consumers: list[str],
        deferred: bool,
    ) -> None:
        consumer_type = None
        if consumers:
            consumer_type = self.dag[consumers[0]].deps.get(step_name)
        output, had_error, exc = self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        if deferred:
            self._emit_step_result(node, step_name, output, stats, had_error, exc)
        for consumer in consumers:
            self.state.set_output(step_name, output, consumer)

    def _publish_stream_to_multiple_consumers(self, step_name, output, node, consumers):
        fanout = SyncFanout(
            output,
            max_in_flight=max(1, node.max_in_flight),
            branches=consumers,
        )
        self._active_fanouts.append(fanout)
        for consumer in consumers:
            self.state.set_output(step_name, fanout.lazy_iterator(consumer), consumer)

    def _publish_scalar_output(
        self,
        step_name: str,
        output: Any,
        node: Any,
        stats: StepRunStats,
        deferred: bool,
    ) -> None:
        if self.dag.needs_materialize(step_name):
            output, _, _ = self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
        self.state.set_output(step_name, output)
        if deferred:
            self._emit_step_result(
                node, step_name, output, stats, had_error=False, exception=None
            )

    def _emit_step_result(
        self,
        node: Any,
        step_name: str,
        output: Any,
        stats: StepRunStats,
        had_error: bool,
        exception: BaseException | None = None,
    ) -> None:
        if has_threshold(node):
            return
        success = len(output) if hasattr(output, "__len__") else 1
        real_error_count = stats.error_count
        real_invocation_count = (
            stats.invocation_count if stats.invocation_count > 0 else success
        )
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
