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
)
from synaflow.execution.overrides import ExecutionOverrides
from .threshold import (
    check_threshold,
    wrap_threshold_raise_if_manual,
    compute_completed_all_inputs_for_all,
    has_threshold,
)
from .step_scope import StepScope


# ---------------------------------------------------------------------------
# Runtime helpers (no flags needed on DagNode)
# ---------------------------------------------------------------------------


from .stream_publisher import StreamPublisher


def _wrap_started_stream(it: Any, fire_started: Any) -> Any:
    try:
        for item in it:
            fire_started()
            yield item
    finally:
        fire_started()


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
        self.outputs = {}
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})
        self.run_id = str(uuid.uuid4())
        self.scope = StepScope(
            self.dag, self.outputs, self._overrides, self._resource_factories
        )
        self.events = EventDispatcher(self.dag, self.run_id, self._overrides)
        self.publisher = StreamPublisher(
            self.dag,
            self.outputs,
            self.events,
            step_output_observers or [],
            self.scope,
        )

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
            self.publisher.cleanup()
        if completed_cleanly:
            self.events.pipeline_completed()

    def _step_inputs_available(self, step_name: str) -> bool:
        node = self.dag[step_name]
        for dep_name in node.deps:
            if dep_name in self.dag.resources:
                continue
            key = self.dag.output_key(dep_name, step_name)
            if key not in self.outputs and dep_name not in self.outputs:
                return False
        return True

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
                        if self._step_inputs_available(s):
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
                        self.publisher.abort(exc)

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

        started = False

        def fire_started():
            nonlocal started
            if not started:
                self.events.step_started(node, step_name)
                started = True

        try:
            if not unrolled and not inspect.isgeneratorfunction(node.fn):
                fire_started()
            output = self._execute_step(step_name, node, arguments, unrolled)
            if isinstance(output, Iterator):
                output = _wrap_started_stream(output, fire_started)
            output = self.scope.attach_cleanup(output, arguments)
            self._emit_immediate_completion(step_name, node, output, unrolled)
            if not self.dag.is_hidden_step(step_name):
                self.publisher.publish(step_name, output, node)
        except PipelineStopException as exc:
            self._dispatch_step_failure(node, step_name, exc.cause or exc)
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
                self._dispatch_step_failure(
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
                self._dispatch_step_failure(
                    node,
                    step_name,
                    exc,
                    success_count=exc.success_count,
                    error_count=exc.error_count,
                    completed_all_inputs=True,
                )
            raise
        except Exception as exc:
            self.events.handle_error(step_name, exc)
            self._dispatch_step_failure(node, step_name, exc)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
        finally:
            if "output" not in locals() or not isinstance(output, Iterator):
                self.scope.close_managed_streams(arguments)
            resource_stack.close()

    def _execute_step(self, step_name, node, arguments, unrolled):
        if unrolled:
            return self._unroll_step(step_name, node, arguments, unrolled)
        return node.fn(**arguments)

    def _emit_immediate_completion(self, step_name, node, output, unrolled):
        if unrolled or isinstance(output, Iterator):
            return
        success_count = 1
        if isinstance(output, (list, tuple, set)):
            success_count = len(output)
        self.events.step_completed(
            node,
            step_name,
            success_count=success_count,
            error_count=0,
            completed_all_inputs=True,
        )

    def _dispatch_step_failure(
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
        self.events.step_failed(
            node,
            step_name,
            success_count=success_count,
            error_count=error_count,
            completed_all_inputs=completed_all_inputs,
            exception=cause,
        )

    def _unroll_step(self, step_name, node, base_args, unrolled):
        """Call fn once per item-tuple. Exhausted streams yield None.
        If terminal (sink), consume eagerly without producing output."""
        iterators = {}
        for dep in unrolled:
            key = self.dag.output_key(dep, step_name)
            source = self.outputs.get(key, self.outputs.get(dep))
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
                        self._dispatch_step_failure(
                            node,
                            step_name,
                            exc,
                            success_count=exc.success_count,
                            error_count=exc.error_count,
                            completed_all_inputs=True,
                        )
                        raise
                    success_count = invocation_count - error_count
                    self.events.step_completed(
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
                self.scope.close_managed_streams(iterators)

        if self.dag.is_terminal_step(step_name):
            for _ in generate():
                pass
            return None
        return generate()


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
