import dataclasses
import threading
import uuid
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.constants import PIPELINE_SCOPE
from synaflow.execution.bounded_iterator import BoundedIterator
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import (
    InvalidThresholdRaiseInEACHStep,
    PipelineStopException,
    ThresholdExceededException,
)
from synaflow.core.observers import (
    MaterializationCompletedContext,
    MaterializationEvent,
    MaterializationFailedContext,
    MaterializationStartedContext,
    PipelineCompletedContext,
    PipelineEvent,
    PipelineFailedContext,
    PipelineStartedContext,
    StepCompletedContext,
    StepEvent,
    StepFailedContext,
    StepStartedContext,
    dispatch_observers,
)
from synaflow.core.types import (
    ErrorContext,
    OnError,
    StepMode,
)
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.sync_handoff import (
    SyncFanout,
    SyncQueueIterator,
)


# ---------------------------------------------------------------------------
# Runtime helpers (no flags needed on DagNode)
# ---------------------------------------------------------------------------


def _maybe_wrap_stream(output, node):
    """Wrap a progressive stream output with bounded handoff if needed."""
    if node.max_in_flight <= 1:
        return output
    if not isinstance(output, Iterator):
        return output
    return BoundedIterator(output, node.max_in_flight)


def _collect_iterator(
    dag: Dag,
    step_name: str,
    value: Iterator,
    run_id: str,
) -> tuple[list[Any], bool, BaseException | None]:
    items = []
    while True:
        try:
            items.append(next(value))
        except StopIteration:
            return items, False, None
        except Exception as exc:
            _handle_error(
                dag,
                step_name,
                exc,
                run_id=run_id,
                success_count=len(items),
                error_count=1,
                completed_all_inputs=False,
            )
            if dag[step_name].on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
            return items, True, exc


def _apply_materializer(
    dag: Dag,
    step_name: str,
    value: Any,
    materializer: Any,
    run_id: str,
    consumer_type: Any = None,
) -> tuple[Any, bool, BaseException | None]:
    if materializer is None:
        if isinstance(value, Iterator):
            items, had_error, exc = _collect_iterator(dag, step_name, value, run_id)
            return items, had_error, exc
        return value, False, None

    if isinstance(value, Iterator):
        items, had_error, exc = _collect_iterator(dag, step_name, value, run_id)
        return materializer(items), had_error, exc

    return materializer(value), False, None


def _handle_error(
    dag: Dag,
    step_name: str,
    exc: BaseException,
    *,
    run_id: str,
    success_count: int = 0,
    error_count: int = 1,
    completed_all_inputs: bool | None = None,
) -> None:
    node = dag.steps.get(step_name)
    if not node:
        return

    err_mat = getattr(node, "error_materializer", None)
    if err_mat is None:
        return

    if not callable(err_mat):
        raise TypeError(f"Error materializer for step '{step_name}' is not callable.")

    error_ctx = ErrorContext(
        pipeline_name=dag.name,
        dataset_name=step_name,
        step_name=step_name,
        run_id=run_id,
        exception=exc,
        mode=getattr(node, "mode", None),
        on_error=getattr(node, "on_error", None),
        success_count=success_count,
        error_count=error_count,
        completed_all_inputs=completed_all_inputs,
    )
    err_mat(error_ctx)


def _wrap_threshold_raise_if_manual(
    exc: BaseException, step_name: str
) -> BaseException:
    """If the user manually raised ThresholdExceededException from inside fn(),
    wrap it so the error materializer logs a clear "you're misusing the API"
    message. The original is preserved as __cause__ for full traceback."""
    if isinstance(exc, ThresholdExceededException):
        return InvalidThresholdRaiseInEACHStep(step_name=step_name, original=exc)
    return exc


def _check_threshold(
    step_name: str,
    node: Any,
    invocation_count: int,
    error_count: int,
) -> None:
    """Called AFTER all inputs are consumed. Raises ThresholdExceededException
    if any threshold is violated. Not called mid-stream to avoid false positives."""
    if node.error_threshold_absolute is None and node.error_threshold_pct is None:
        return

    success_count = invocation_count - error_count

    if node.error_threshold_absolute is not None:
        if error_count >= node.error_threshold_absolute:
            raise ThresholdExceededException(
                step_name=step_name,
                error_count=error_count,
                success_count=success_count,
                threshold_absolute=node.error_threshold_absolute,
            )

    if node.error_threshold_pct is not None:
        if (
            invocation_count > 0
            and (error_count / invocation_count) >= node.error_threshold_pct
        ):
            raise ThresholdExceededException(
                step_name=step_name,
                error_count=error_count,
                success_count=success_count,
                threshold_pct=node.error_threshold_pct,
            )


def _compute_completed_all_inputs_for_all(
    node: Any, arguments: dict, exc: ThresholdExceededException
) -> bool:
    """For an ALL step that manually raised ThresholdExceededException,
    determine if the user processed all inputs before raising.

    Total processed = exc.error_count + exc.success_count (user-supplied).
    Total input = sum of len(arg) across deps, for sized collections only.
    Returns False if input size is unknown (non-sized iterators).
    """
    total_input = 0
    known = True
    for value in arguments.values():
        try:
            total_input += len(value)
        except TypeError:
            known = False
            break
    if not known:
        return False
    return (exc.error_count + exc.success_count) == total_input


def _has_threshold(node: Any) -> bool:
    return (
        node.error_threshold_absolute is not None
        or node.error_threshold_pct is not None
    )


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
        self._step_output_observers = step_output_observers or []
        self._active_fanouts: list[SyncFanout] = []
        self._observer_threads: list[threading.Thread] = []
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})
        self.run_id = str(uuid.uuid4())

    # ------------------------------------------------------------------
    # Lifecycle observer dispatch helpers
    # ------------------------------------------------------------------

    def _resolve_pipeline_observers(self) -> list:
        if self._overrides is None:
            return self.dag.pipeline_observers
        return self._overrides.observers.resolve(
            PIPELINE_SCOPE, self.dag.pipeline_observers
        )

    def _resolve_step_observers(self, node: Any, step_name: str) -> list:
        pipeline_observers = self._resolve_pipeline_observers()
        step_observers = [obs for obs in node.observers if obs.source == "step"]
        if self._overrides is not None:
            step_observers = self._overrides.observers.resolve(
                step_name, step_observers
            )
        return [*pipeline_observers, *step_observers]

    def _dispatch_pipeline_event(
        self,
        event: PipelineEvent,
        step_name: str | None = None,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self._resolve_pipeline_observers()
        if not registrations:
            return
        ctx: Any
        if event is PipelineEvent.STARTED:
            ctx = PipelineStartedContext(
                pipeline_name=self.dag.name, run_id=self.run_id, event=event
            )
        elif event is PipelineEvent.COMPLETED:
            ctx = PipelineCompletedContext(
                pipeline_name=self.dag.name, run_id=self.run_id, event=event
            )
        elif event is PipelineEvent.FAILED:
            ctx = PipelineFailedContext(
                pipeline_name=self.dag.name,
                run_id=self.run_id,
                event=event,
                step_name=step_name,
                exception=exception,
            )
        else:
            return
        dispatch_observers(registrations, ctx)

    def _dispatch_step_event(
        self,
        node: Any,
        event: StepEvent,
        step_name: str,
        success_count: int = 0,
        error_count: int = 0,
        completed_all_inputs: bool = True,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self._resolve_step_observers(node, step_name)
        if not registrations:
            return
        ctx: Any
        if event is StepEvent.STARTED:
            ctx = StepStartedContext(
                pipeline_name=self.dag.name,
                run_id=self.run_id,
                event=event,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
            )
        elif event is StepEvent.COMPLETED:
            ctx = StepCompletedContext(
                pipeline_name=self.dag.name,
                run_id=self.run_id,
                event=event,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=success_count,
                error_count=error_count,
                completed_all_inputs=completed_all_inputs,
            )
        elif event is StepEvent.FAILED:
            ctx = StepFailedContext(
                pipeline_name=self.dag.name,
                run_id=self.run_id,
                event=event,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=success_count,
                error_count=error_count,
                completed_all_inputs=completed_all_inputs,
                exception=exception,
            )
        else:
            return
        dispatch_observers(registrations, ctx)

    def _dispatch_materialization_event(
        self,
        step_name: str,
        node: Any,
        event: MaterializationEvent,
        consumer_type: Any = None,
        materializer_name: str | None = None,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self._resolve_step_observers(node, step_name)
        if not registrations:
            return
        ctx: Any
        if event is MaterializationEvent.STARTED:
            ctx = MaterializationStartedContext(
                pipeline_name=self.dag.name,
                run_id=self.run_id,
                event=event,
                step_name=step_name,
                dataset_name=step_name,
                consumer_type=consumer_type,
                materializer_name=materializer_name,
            )
        elif event is MaterializationEvent.COMPLETED:
            ctx = MaterializationCompletedContext(
                pipeline_name=self.dag.name,
                run_id=self.run_id,
                event=event,
                step_name=step_name,
                dataset_name=step_name,
                consumer_type=consumer_type,
                materializer_name=materializer_name,
            )
        elif event is MaterializationEvent.FAILED:
            ctx = MaterializationFailedContext(
                pipeline_name=self.dag.name,
                run_id=self.run_id,
                event=event,
                step_name=step_name,
                dataset_name=step_name,
                consumer_type=consumer_type,
                materializer_name=materializer_name,
                exception=exception,
            )
        else:
            return
        dispatch_observers(registrations, ctx)

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

    def _resolve_materializer(self, step_name: str, node: Any) -> Any:
        if self._overrides is None:
            return node.materializer
        return self._overrides.materializers.resolve(step_name, node.materializer)

    def execute(self, params: Any) -> None:
        self._seed_runtime_inputs(params)

        self._dispatch_pipeline_event(PipelineEvent.STARTED)
        completed_cleanly = False
        try:
            self._run_graph()
            completed_cleanly = True
        except PipelineStopException as exc:
            self._dispatch_pipeline_event(
                PipelineEvent.FAILED,
                step_name=exc.step_name,
                exception=exc.cause or exc,
            )
            raise
        except ThresholdExceededException as exc:
            self._dispatch_pipeline_event(
                PipelineEvent.FAILED,
                step_name=exc.step_name,
                exception=exc,
            )
            raise
        except Exception as exc:
            self._dispatch_pipeline_event(
                PipelineEvent.FAILED, step_name=None, exception=exc
            )
            raise
        finally:
            self._cleanup_fanouts()
        if completed_cleanly:
            self._dispatch_pipeline_event(PipelineEvent.COMPLETED)

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
                        self._abort_fanouts(exc)

                    if completed_cleanly:
                        check_new_ready_steps()
                    cond.notify_all()

            with cond:
                check_new_ready_steps()
                while running_tasks:
                    cond.wait()

        if fatal_error is not None:
            raise fatal_error

    def _abort_fanouts(self, exception: BaseException | None = None) -> None:
        for fanout in self._active_fanouts:
            fanout.abort(exception)

    def _cleanup_fanouts(self) -> None:
        for fanout in self._active_fanouts:
            fanout.join()
        self._active_fanouts.clear()
        for thread in self._observer_threads:
            thread.join()
        self._observer_threads.clear()

    def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        resource_stack = ExitStack()
        arguments = self._build_arguments(step_name, node, resource_stack)
        unrolled = self.dag.each_inputs(step_name)


        started = False
        def fire_started():
            nonlocal started
            if not started:
                self._dispatch_step_event(node, StepEvent.STARTED, step_name)
                started = True

        try:
            import inspect
            if not unrolled and not inspect.isgeneratorfunction(node.fn):
                fire_started()
            output = self._execute_step(step_name, node, arguments, unrolled)
            if isinstance(output, Iterator):
                def _wrap_started(it):
                    try:
                        for item in it:
                            fire_started()
                            yield item
                    finally:
                        fire_started()
                output = _wrap_started(output)
            output = self._attach_argument_cleanup(output, arguments)
            self._emit_immediate_completion(step_name, node, output, unrolled)
            if not self.dag.is_hidden_step(step_name):
                self._publish_output(step_name, output, node)
        except PipelineStopException as exc:
            self._dispatch_step_failure(node, step_name, exc.cause or exc)
            raise
        except ThresholdExceededException as exc:
            if exc.step_name != step_name:
                # Upstream threshold propagating through this consumer:
                # the producer's generate() already dispatched FAILED.
                pass
            elif unrolled and _has_threshold(node):
                # This step's generate() already dispatched FAILED (path A).
                pass
            elif not unrolled:
                # ALL-mode manual raise by this step (path B, escape hatch)
                completed_all_inputs = _compute_completed_all_inputs_for_all(
                    node, arguments, exc
                )
                _handle_error(
                    self.dag,
                    step_name,
                    exc,
                    run_id=self.run_id,
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
            _handle_error(self.dag, step_name, exc, run_id=self.run_id)
            self._dispatch_step_failure(node, step_name, exc)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
        finally:
            if "output" not in locals() or not isinstance(output, Iterator):
                self._close_managed_stream_arguments(arguments)
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
        self._dispatch_step_event(
            node,
            StepEvent.COMPLETED,
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
        self._dispatch_step_event(
            node,
            StepEvent.FAILED,
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
                        _handle_error(
                            self.dag,
                            step_name,
                            _wrap_threshold_raise_if_manual(exc, step_name),
                            run_id=self.run_id,
                            success_count=invocation_count - error_count,
                            error_count=error_count,
                            completed_all_inputs=False,
                        )
                        if on_err == OnError.STOP:
                            raise PipelineStopException(
                                step_name=step_name, cause=exc
                            ) from exc
                # pos-loop, before generator ends
                if _has_threshold(node):
                    try:
                        _check_threshold(step_name, node, invocation_count, error_count)
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
                    self._dispatch_step_event(
                        node,
                        StepEvent.COMPLETED,
                        step_name,
                        success_count=success_count,
                        error_count=error_count,
                        completed_all_inputs=True,
                    )
                else:
                    _check_threshold(step_name, node, invocation_count, error_count)
            finally:
                node._runtime_error_count = error_count
                node._runtime_invocation_count = invocation_count
                self._close_managed_stream_arguments(iterators)

        if self.dag.is_terminal_step(step_name):
            for _ in generate():
                pass
            return None
        return generate()

    def _resolve_resource_argument(self, resource_name: str, resource_stack: ExitStack):
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
        if hasattr(value, "__aenter__") and hasattr(value, "__aexit__"):
            raise TypeError(
                f"Pipeline '{self.dag.name}': resource '{resource_name}' produced an async context manager in sync run()."
            )
        if hasattr(value, "__enter__") and hasattr(value, "__exit__"):
            return resource_stack.enter_context(value)
        return value

    def _build_arguments(self, consumer, node, resource_stack: ExitStack):
        args = {}
        for dep_name in node.deps:
            if dep_name in self.dag.resources:
                value = self._resolve_resource_argument(dep_name, resource_stack)
            else:
                key = self.dag.output_key(dep_name, consumer)
                value = self.outputs.get(key, self.outputs.get(dep_name))
            param = node.dataset_param_names.get(dep_name, dep_name)
            args[param] = value
        return args

    def _attach_argument_cleanup(self, output, arguments):
        if not isinstance(output, Iterator):
            return output

        def wrapped():
            try:
                yield from output
            finally:
                self._close_managed_stream_arguments(arguments)

        return wrapped()

    def _close_managed_stream_arguments(self, arguments):
        for value in arguments.values():
            if isinstance(value, SyncQueueIterator):
                try:
                    value.close()
                except Exception:
                    pass

    def _notify_observers(self, step_name, output):
        if not self._step_output_observers:
            return output
        if isinstance(output, Iterator):
            # We can't safely tee the output here if it's purely lazy,
            # because an observer might be slow and cause memory to blow up.
            # However, if this stream is NOT consumed by multiple steps later,
            # we should use SyncFanout here. But if it IS consumed later,
            # the _publish_output methods handle observers by adding branches
            # to the SyncFanout!
            # So _notify_observers should ONLY notify for SCALAR values.
            # For iterators, observers are handled during fanout construction.
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
        materializer = self._resolve_materializer(step_name, node)
        mat_name = materializer.__name__ if callable(materializer) else None
        self._dispatch_materialization_event(
            step_name,
            node,
            MaterializationEvent.STARTED,
            consumer_type,
            mat_name,
        )
        try:
            result, had_error, exc = _apply_materializer(
                self.dag,
                step_name,
                output,
                materializer,
                self.run_id,
                consumer_type=consumer_type,
            )
            self._dispatch_materialization_event(
                step_name,
                node,
                MaterializationEvent.COMPLETED,
                consumer_type,
                mat_name,
            )
            return result, had_error, exc
        except PipelineStopException:
            raise
        except Exception as exc:
            self._dispatch_materialization_event(
                step_name,
                node,
                MaterializationEvent.FAILED,
                consumer_type,
                mat_name,
                exception=exc,
            )
            raise

    def _emit_step_result(self, node, step_name, output, had_error, exception=None):
        if _has_threshold(node):
            return  # already dispatched by generate()
        success = len(output) if hasattr(output, "__len__") else 1
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", success)
        if had_error:
            self._dispatch_step_event(
                node,
                StepEvent.FAILED,
                step_name,
                success_count=success,
                error_count=max(real_error_count, 1),
                completed_all_inputs=False,
                exception=exception,
            )
        else:
            self._dispatch_step_event(
                node,
                StepEvent.COMPLETED,
                step_name,
                success_count=real_invocation_count - real_error_count,
                error_count=real_error_count,
                completed_all_inputs=True,
            )

    def _emit_deferred_completion(self, node, step_name):
        if _has_threshold(node):
            return  # already dispatched by generate()
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", 0)
        self._dispatch_step_event(
            node,
            StepEvent.COMPLETED,
            step_name,
            success_count=real_invocation_count - real_error_count,
            error_count=real_error_count,
            completed_all_inputs=True,
        )

    def _wrap_deferred_output(self, step_name, output, node):
        if _has_threshold(node):
            return output

        def wrapped():
            yielded_count = 0
            for item in output:
                yielded_count += 1
                yield item

            if node.mode == StepMode.ALL:
                node._runtime_invocation_count = yielded_count
                node._runtime_error_count = 0
            self._emit_deferred_completion(node, step_name)

        return wrapped()

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
            self.outputs[self.dag.output_key(step_name, consumer)] = output

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
            self.outputs[self.dag.output_key(step_name, consumer)] = (
                fanout.lazy_iterator(consumer)
            )
            self._start_observer_threads(step_name, fanout, observer_branches)
            fanout.start()
            return
        output, had_error, exc = self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        output = self._notify_observers(step_name, output)
        if deferred:
            self._emit_step_result(node, step_name, output, had_error, exc)
        output = _maybe_wrap_stream(output, node)
        self.outputs[self.dag.output_key(step_name, consumer)] = output

    def _publish_stream_to_multiple_consumers(self, step_name, output, node, consumers):
        fanout = SyncFanout(
            output,
            max_in_flight=max(1, node.max_in_flight),
            branches=consumers + self._observer_branch_names(),
        )
        self._active_fanouts.append(fanout)
        for consumer in consumers:
            self.outputs[self.dag.output_key(step_name, consumer)] = (
                fanout.lazy_iterator(consumer)
            )
        self._start_observer_threads(step_name, fanout, self._observer_branch_names())
        fanout.start()

    def _publish_scalar_output(self, step_name, output, node, deferred):
        if self.dag.needs_materialize(step_name):
            output, _, _ = self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
        self.outputs[step_name] = output
        if deferred:
            self._emit_step_result(
                node, step_name, output, had_error=False, exception=None
            )

    def _publish_output(self, step_name, output, node):
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
        self.outputs[step_name] = _maybe_wrap_stream(output, node)


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
