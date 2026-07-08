import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from synaflow.core.dag import Dag, DagNode
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
from synaflow.execution.runtime_contract_validation import (
    satisfies_sync_iterator_contract,
)
from synaflow.execution.state import ExecutionState
from .argument_builder import ArgumentBuilder
from synaflow.execution.stats import StepRunStats
from .step_runner import (
    StepRunner,
    StepRuntimeConfig,
    collect_iterator,
    wrap_deferred_output,
)


# ---------------------------------------------------------------------------
# Worker-thread lifecycle
# ---------------------------------------------------------------------------


_LOGGER = logging.getLogger("synaflow")


def wait_for_workers_after_shutdown(
    thread_name_prefix: str = "synaflow-worker",
    log_every_seconds: float = 60.0,
    poll_seconds: float = 0.5,
    *,
    _enumerate_threads: Callable[[], list[threading.Thread]] = threading.enumerate,
    _is_alive: Callable[[threading.Thread], bool] = threading.Thread.is_alive,
    _sleep: Callable[[float], None] = time.sleep,
    _monotonic: Callable[[], float] = time.monotonic,
    _log: Callable[..., Any] = _LOGGER.warning,
    _process_pid: int | None = None,
) -> int:
    """Block until no alive thread whose name starts with ``thread_name_prefix``.

    Used after a sync engine shuts down its pool with
    ``shutdown(wait=False, cancel_futures=True)``.  Polls every
    ``poll_seconds`` so a transient alive thread (mid-cleanup) clears
    within a couple of seconds; logs a warning every ``log_every_seconds``
    for threads that persist, with worker names and the process PID for
    diagnostics.

    If user code is blocked indefinitely, this function blocks
    indefinitely too — the contract is that the *user* is responsible
    for step progress.  The user-visible log line is the diagnostic.
    """
    if _process_pid is None:
        _process_pid = os.getpid()

    last_log_at: float | None = None
    polls = 0
    while True:
        polls += 1
        alive = [
            t
            for t in _enumerate_threads()
            if t.name.startswith(thread_name_prefix) and _is_alive(t)
        ]
        if not alive:
            return polls
        now = _monotonic()
        if last_log_at is None or now - last_log_at >= log_every_seconds:
            _log(
                "synaflow waiting for %d worker thread(s) (pid=%d): %s. "
                "These workers are blocked inside user code (step "
                "function); the process will not exit until they "
                "complete.  Check your step functions for blocking I/O, "
                "infinite loops, or deadlocks.",
                len(alive),
                _process_pid,
                [t.name for t in alive],
            )
            last_log_at = now
        _sleep(poll_seconds)


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
        worker_shutdown_poll_seconds: float = 0.5,
        worker_shutdown_log_every_seconds: float = 60.0,
    ):
        self.dag = dag
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})
        self._worker_shutdown_poll_seconds = worker_shutdown_poll_seconds
        self._worker_shutdown_log_every_seconds = worker_shutdown_log_every_seconds
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

        # Context-manager shutdown would call shutdown(wait=True), which would
        # block here when user code is stuck.  We pair shutdown(wait=False)
        # with the post-shutdown wait below, which logs stuck workers and
        # returns once they exit (or blocks indefinitely if user code does
        # not progress — that is the user's responsibility).
        pool = ThreadPoolExecutor(
            max_workers=max(1, len(self.dag.steps)),
            thread_name_prefix="synaflow-worker",
        )
        try:

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
                # Issue #103: when a step fails, exit the wait loop instead
                # of waiting forever for sibling steps that may be blocked
                # on I/O.  Cleanup() below handles the abandoned workers.
                while running_tasks and fatal_error is None:
                    cond.wait()
        finally:
            # wait=False so we don't block on workers that are stuck inside
            # user code.  The wait helper below blocks instead, logging
            # each iteration so the user can identify stuck steps.
            pool.shutdown(wait=False, cancel_futures=True)
            wait_for_workers_after_shutdown(
                poll_seconds=self._worker_shutdown_poll_seconds,
                log_every_seconds=self._worker_shutdown_log_every_seconds,
            )

        if fatal_error is not None:
            raise fatal_error

    def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        arguments, resource_stack = self.scope.build_arguments(step_name, node)
        unrolled = self.dag.each_inputs(step_name)

        # The runtime executor passes only what runtime actually needs:
        # the compiled ``DagNode``. Error thresholds, observers, mode,
        # etc. are read straight from ``dag_node`` by the threshold /
        # observer-resolution helpers — no redundant copies.
        step_runtime_config = StepRuntimeConfig(dag_node=node)

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
            should_drain=(
                node.output_contract is not None
                and node.output_contract.drain_policy != "none"
            ),
            publisher=lambda out: (
                self.publish(step_name, out, node, stats)
                if not self.dag.is_hidden_step(step_name)
                else None
            ),
            state=self.state,
            events=self.events,
            stats=stats,
            each_mode_deps=unrolled,
            step_runtime_config=step_runtime_config,
        )

        runner.run()

    # ------------------------------------------------------------------
    # Dataflow routing & publishing (formerly StreamPublisher)
    # ------------------------------------------------------------------

    def publish(
        self, step_name: str, output: Any, node: DagNode, stats: StepRunStats
    ) -> None:
        publish_plan = node.publish_plan
        output_contract = node.output_contract
        if publish_plan is None or output_contract is None:
            raise RuntimeError(
                f"Step '{step_name}' is missing a compiled execution plan."
            )

        deferred = output_contract.completion_policy == "on_exhaustion"
        consumers = self.dag.consumers_of(step_name)

        if publish_plan.strategy == "publish_value":
            self._validate_value_output_contract(step_name, output, output_contract)
            self._publish_scalar_output(step_name, output, node, stats, deferred)
            return

        self._validate_stream_output_contract(step_name, output, output_contract)

        if publish_plan.strategy == "publish_materialized":
            self._materialize_stream_output(
                step_name, output, node, stats, consumers, deferred
            )
            return

        if deferred:
            output = wrap_deferred_output(step_name, output, node, self.events, stats)

        if publish_plan.strategy == "publish_sync_fanout":
            self._publish_stream_to_multiple_consumers(
                step_name, output, node, consumers
            )
            return

        if publish_plan.strategy == "publish_stream":
            self.state.set_output(step_name, self._maybe_wrap_stream(output, node))
            return

        raise RuntimeError(
            f"Step '{step_name}' has unsupported sync publish strategy "
            f"'{publish_plan.strategy}'."
        )

    def abort(self, exception: BaseException | None = None) -> None:
        for fanout in self._active_fanouts:
            fanout.abort(exception)

    def cleanup(self) -> None:
        # A SyncFanout pump thread may be stuck in user code (e.g.
        # ``next(source)`` blocked on I/O).  We bound the wait per fanout
        # and log when a pump does not exit in time; the orphaned pump is
        # daemon so it does not block process exit.
        for fanout in self._active_fanouts:
            exited = fanout.join(timeout=1.0)
            if not exited:
                _LOGGER.warning(
                    "SyncFanout pump thread did not exit within 1.0s; "
                    "likely blocked in user code (next() on a stuck "
                    "iterator).  Pump is daemon and will not block "
                    "process exit."
                )
        self._active_fanouts.clear()

    def _validate_value_output_contract(
        self, step_name: str, output: Any, output_contract: Any
    ) -> None:
        if output_contract.runtime_kind != "value":
            return
        if satisfies_sync_iterator_contract(output):
            raise TypeError(
                f"Step '{step_name}' compiled as a value-producing step but returned "
                "a synchronous iterator at runtime."
            )

    def _validate_stream_output_contract(
        self, step_name: str, output: Any, output_contract: Any
    ) -> None:
        if output_contract.runtime_kind != "sync_stream":
            raise TypeError(
                f"Step '{step_name}' compiled with runtime kind "
                f"'{output_contract.runtime_kind}' but reached the sync stream "
                "publish path."
            )
        if not satisfies_sync_iterator_contract(output):
            raise TypeError(
                f"Step '{step_name}' compiled as a sync stream but returned "
                f"{type(output).__name__} at runtime."
            )

    def _maybe_wrap_stream(self, output: Any, node: DagNode) -> Any:
        if node.publish_plan is None or node.publish_plan.handoff != "bounded_iterator":
            return output
        if node.max_in_flight <= 1:
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
            if satisfies_sync_iterator_contract(value):
                items, had_error, exc = collect_iterator(
                    step_name, value, self.dag[step_name].on_error, self.events
                )
                return items, had_error, exc
            return value, False, None

        if satisfies_sync_iterator_contract(value):
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
        node: DagNode,
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
        node: DagNode,
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
        node: DagNode,
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
    pipeline: PipelineDef,
    params: Any,
    overrides: ExecutionOverrides | None = None,
    *,
    worker_shutdown_poll_seconds: float = 0.5,
    worker_shutdown_log_every_seconds: float = 60.0,
) -> None:
    if getattr(pipeline, "requires_async_runner", False):
        raise RuntimeError(
            "This pipeline contains async features (async def or AsyncIterator)"
            " and must be executed with async_run()."
        )
    PipelineExecutor(
        pipeline.dag,
        overrides=overrides,
        resource_factories=pipeline.dag.resource_factories,
        worker_shutdown_poll_seconds=worker_shutdown_poll_seconds,
        worker_shutdown_log_every_seconds=worker_shutdown_log_every_seconds,
    ).execute(params)
