import itertools
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.execution.bounded_iterator import BoundedIterator
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException
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
    ErrorMaterializeContext,
    MaterializeContext,
    OnError,
    StepMode,
)
from synaflow.execution.sync_handoff import (
    SyncFanout,
    SyncMaterializedValue,
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
    dag: Dag, step_name: str, value: Iterator
) -> tuple[list[Any], bool, BaseException | None]:
    items = []
    while True:
        try:
            items.append(next(value))
        except StopIteration:
            return items, False, None
        except Exception as exc:
            _handle_error(dag, step_name, exc)
            if dag[step_name].on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
            return items, True, exc


def _apply_materializer(
    dag: Dag, step_name: str, value: Any, consumer_type: Any = None
) -> tuple[Any, bool, BaseException | None]:
    node = dag[step_name]
    mat = node.get("materializer")
    if mat is None:
        if isinstance(value, Iterator):
            items, had_error, exc = _collect_iterator(dag, step_name, value)
            return items, had_error, exc
        return value, False, None
    concrete_mat = mat(
        MaterializeContext(
            pipeline_name=dag.name,
            dataset_name=step_name,
            item_type=node.output,
            consumer_type=consumer_type,
        )
    )
    if isinstance(value, Iterator) and concrete_mat in (list, tuple, set, dict):
        items, had_error, exc = _collect_iterator(dag, step_name, value)
        result = items if concrete_mat is list else concrete_mat(items)
        return result, had_error, exc
    if (
        isinstance(value, Iterator)
        and getattr(concrete_mat, "__name__", "") == "_identity"
    ):
        items, had_error, exc = _collect_iterator(dag, step_name, value)
        return items, had_error, exc
    return concrete_mat(value), False, None


def _handle_error(dag: Dag, step_name: str, exc: BaseException) -> None:
    node = dag.steps.get(step_name)
    if not node:
        return

    err_mat = getattr(node, "error_materializer", None)
    if err_mat is None:
        return

    handler = err_mat(
        ErrorMaterializeContext(
            pipeline_name=dag.name,
            dataset_name=step_name,
            exception_type=type(exc),
        )
    )

    if callable(handler):
        handler(exc)


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class PipelineExecutor:
    def __init__(self, dag: Dag, *, step_output_observers: list = None):
        self.dag = dag
        self.outputs = {}
        self._step_output_observers = step_output_observers or []
        self._active_fanouts: list[SyncFanout] = []

    # ------------------------------------------------------------------
    # Lifecycle observer dispatch helpers
    # ------------------------------------------------------------------

    def _dispatch_pipeline_event(
        self,
        event: PipelineEvent,
        step_name: str | None = None,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self.dag.pipeline_observers
        if not registrations:
            return
        ctx: Any
        if event is PipelineEvent.STARTED:
            ctx = PipelineStartedContext(pipeline_name=self.dag.name, event=event)
        elif event is PipelineEvent.COMPLETED:
            ctx = PipelineCompletedContext(pipeline_name=self.dag.name, event=event)
        elif event is PipelineEvent.FAILED:
            ctx = PipelineFailedContext(
                pipeline_name=self.dag.name,
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
        registrations = node.observers
        if not registrations:
            return
        ctx: Any
        if event is StepEvent.STARTED:
            ctx = StepStartedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
            )
        elif event is StepEvent.COMPLETED:
            ctx = StepCompletedContext(
                pipeline_name=self.dag.name,
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
        registrations = node.observers
        if not registrations:
            return
        ctx: Any
        if event is MaterializationEvent.STARTED:
            ctx = MaterializationStartedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                dataset_name=step_name,
                consumer_type=consumer_type,
                materializer_name=materializer_name,
            )
        elif event is MaterializationEvent.COMPLETED:
            ctx = MaterializationCompletedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                dataset_name=step_name,
                consumer_type=consumer_type,
                materializer_name=materializer_name,
            )
        elif event is MaterializationEvent.FAILED:
            ctx = MaterializationFailedContext(
                pipeline_name=self.dag.name,
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

    def execute(self, params: Any) -> None:
        for field, value in params._asdict().items():
            self.outputs[field] = value

        self._dispatch_pipeline_event(PipelineEvent.STARTED)
        try:
            for level in self.dag.get_execution_levels():
                self._run_level(level)
        except PipelineStopException as exc:
            self._dispatch_pipeline_event(
                PipelineEvent.FAILED,
                step_name=exc.step_name,
                exception=exc.cause or exc,
            )
            raise
        except Exception as exc:
            self._dispatch_pipeline_event(
                PipelineEvent.FAILED, step_name=None, exception=exc
            )
            raise
        else:
            self._dispatch_pipeline_event(PipelineEvent.COMPLETED)
        finally:
            self._cleanup_fanouts()

    def _run_level(self, level: list[str]) -> None:
        handoff_steps = self._handoff_steps_in_level(level)
        if not handoff_steps:
            for step_name in level:
                self._run_step(step_name)
            return

        submitted = {}
        with ThreadPoolExecutor(max_workers=len(handoff_steps)) as pool:
            for step_name in level:
                if step_name in handoff_steps:
                    submitted[step_name] = pool.submit(self._run_step, step_name)
                else:
                    self._run_step(step_name)
            for step_name in handoff_steps:
                try:
                    submitted[step_name].result()
                except BaseException as exc:
                    self._abort_fanouts(exc)
                    raise

    def _handoff_steps_in_level(self, level: list[str]) -> set[str]:
        handoff_steps = set()
        for step_name in level:
            node = self.dag[step_name]
            for dep_name in node.deps:
                key = self.dag.output_key(dep_name, step_name)
                value = self.outputs.get(key, self.outputs.get(dep_name))
                if isinstance(value, (SyncQueueIterator, SyncMaterializedValue)):
                    handoff_steps.add(step_name)
                    break
        return handoff_steps

    def _consumers_share_execution_level(self, consumers: list[str]) -> bool:
        level_index = {}
        for index, level in enumerate(self.dag.get_execution_levels()):
            for step_name in level:
                level_index[step_name] = index
        return len({level_index.get(consumer) for consumer in consumers}) <= 1

    def _abort_fanouts(self, exception: BaseException | None = None) -> None:
        for fanout in self._active_fanouts:
            fanout.abort(exception)

    def _cleanup_fanouts(self) -> None:
        for fanout in self._active_fanouts:
            fanout.abort()
        for fanout in self._active_fanouts:
            fanout.join()
        self._active_fanouts.clear()

    def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        arguments = self._build_arguments(step_name, node)
        unrolled = self.dag.each_inputs(step_name)
        self._dispatch_step_event(node, StepEvent.STARTED, step_name)

        try:
            output = self._execute_step(step_name, node, arguments, unrolled)
            output = self._attach_argument_cleanup(output, arguments)
            self._emit_immediate_completion(step_name, node, output, unrolled)
            if not self.dag.is_hidden_step(step_name):
                self._publish_output(step_name, output, node)
        except PipelineStopException as exc:
            self._dispatch_step_failure(node, step_name, exc.cause or exc)
            raise
        except Exception as exc:
            _handle_error(self.dag, step_name, exc)
            self._dispatch_step_failure(node, step_name, exc)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
        finally:
            if "output" not in locals() or not isinstance(output, Iterator):
                self._close_managed_stream_arguments(arguments)

    def _execute_step(self, step_name, node, arguments, unrolled):
        if unrolled:
            return self._unroll_step(step_name, node, arguments, unrolled)
        return node.fn(**arguments)

    def _emit_immediate_completion(self, step_name, node, output, unrolled):
        if unrolled or isinstance(output, Iterator):
            return
        self._dispatch_step_event(
            node,
            StepEvent.COMPLETED,
            step_name,
            success_count=1,
            error_count=0,
            completed_all_inputs=True,
        )

    def _dispatch_step_failure(self, node, step_name, exception):
        cause = exception
        if isinstance(cause, PipelineStopException):
            cause = cause.cause or cause
        self._dispatch_step_event(
            node,
            StepEvent.FAILED,
            step_name,
            success_count=0,
            error_count=1,
            completed_all_inputs=False,
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
                    try:
                        yield node.fn(**item_args)
                    except Exception as exc:
                        _handle_error(self.dag, step_name, exc)
                        if on_err == OnError.STOP:
                            raise PipelineStopException(
                                step_name=step_name, cause=exc
                            ) from exc
            finally:
                self._close_managed_stream_arguments(iterators)

        if self.dag.is_terminal_step(step_name):
            for _ in generate():
                pass
            return None
        return generate()

    def _build_arguments(self, consumer, node):
        args = {}
        for dep_name in node.deps:
            key = self.dag.output_key(dep_name, consumer)
            value = self.outputs.get(key, self.outputs.get(dep_name))
            if isinstance(value, SyncMaterializedValue):
                value = self._resolve_materialized_value(
                    dep_name, value, node, consumer
                )
            param = node.dataset_param_names.get(dep_name, dep_name)
            args[param] = value
        return args

    def _resolve_materialized_value(self, producer, holder, node, consumer):
        consumer_type = node.deps.get(producer)
        output, _, _ = self._materialize_with_events(
            producer,
            holder.result(),
            self.dag[producer],
            consumer_type=consumer_type,
        )
        return output

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
        for observer in self._step_output_observers:
            if isinstance(output, Iterator):
                observed, output = itertools.tee(output)
                observer(step_name, observed)
            else:
                observer(step_name, output)
        return output

    def _materialize_with_events(self, step_name, output, node, consumer_type=None):
        mat_name = node.materializer.__name__ if callable(node.materializer) else None
        self._dispatch_materialization_event(
            step_name,
            node,
            MaterializationEvent.STARTED,
            consumer_type,
            mat_name,
        )
        try:
            result, had_error, exc = _apply_materializer(
                self.dag, step_name, output, consumer_type=consumer_type
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
        success = len(output) if hasattr(output, "__len__") else 1
        if had_error:
            self._dispatch_step_event(
                node,
                StepEvent.FAILED,
                step_name,
                success_count=success,
                error_count=1,
                completed_all_inputs=False,
                exception=exception,
            )
        else:
            self._dispatch_step_event(
                node,
                StepEvent.COMPLETED,
                step_name,
                success_count=success,
                error_count=0,
                completed_all_inputs=True,
            )

    def _emit_deferred_completion(self, node, step_name):
        self._dispatch_step_event(
            node,
            StepEvent.COMPLETED,
            step_name,
            success_count=0,
            error_count=0,
            completed_all_inputs=True,
        )

    def _stream_requires_eager_materialization(self, node):
        return node.on_error == OnError.STOP or node.force_materialize

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
        output, had_error, exc = self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        if deferred:
            self._emit_step_result(node, step_name, output, had_error, exc)
        output = _maybe_wrap_stream(output, node)
        self.outputs[self.dag.output_key(step_name, consumer)] = output

    def _publish_stream_to_multiple_consumers(self, step_name, output, node, consumers):
        lazy_consumers = [
            consumer
            for consumer in consumers
            if step_name not in self.dag[consumer].materialized_deps
        ]
        eager_consumers = [
            consumer
            for consumer in consumers
            if step_name in self.dag[consumer].materialized_deps
        ]

        if not self._consumers_share_execution_level(consumers):
            output = _maybe_wrap_stream(output, node)
            branches = itertools.tee(output, len(consumers))
            for consumer, branch in zip(consumers, branches):
                consumer_node = self.dag[consumer]
                if step_name in consumer_node.materialized_deps:
                    branch, _, _ = self._materialize_with_events(
                        step_name,
                        branch,
                        node,
                        consumer_type=consumer_node.deps.get(step_name),
                    )
                self.outputs[self.dag.output_key(step_name, consumer)] = branch
            return

        if not lazy_consumers:
            branches = itertools.tee(output, len(consumers))
            for consumer, branch in zip(consumers, branches):
                consumer_node = self.dag[consumer]
                branch, _, _ = self._materialize_with_events(
                    step_name,
                    branch,
                    node,
                    consumer_type=consumer_node.deps.get(step_name),
                )
                self.outputs[self.dag.output_key(step_name, consumer)] = branch
            return

        fanout = SyncFanout(
            output,
            max_in_flight=max(1, node.max_in_flight),
            lazy_branches=lazy_consumers,
            eager_branches=eager_consumers,
        )
        self._active_fanouts.append(fanout)
        for consumer in lazy_consumers:
            self.outputs[self.dag.output_key(step_name, consumer)] = (
                fanout.lazy_iterator(consumer)
            )
        for consumer in eager_consumers:
            self.outputs[self.dag.output_key(step_name, consumer)] = fanout.eager_value(
                consumer
            )
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
        output = self._notify_observers(step_name, output)
        deferred = node.mode == StepMode.EACH or (
            node.mode == StepMode.ALL and isinstance(output, Iterator)
        )

        if not isinstance(output, Iterator):
            self._publish_scalar_output(step_name, output, node, deferred)
            return

        consumers = self.dag.consumers_of(step_name)

        if self._stream_requires_eager_materialization(node):
            self._materialize_stream_output(
                step_name, output, node, consumers, deferred
            )
            return

        if len(consumers) == 1 and self.dag.needs_materialize(step_name):
            self._publish_stream_to_single_consumer(
                step_name, output, node, consumers[0], deferred
            )
            return

        if len(consumers) > 1:
            self._publish_stream_to_multiple_consumers(
                step_name, output, node, consumers
            )
            if deferred:
                self._emit_deferred_completion(node, step_name)
            return

        if deferred:
            self._emit_deferred_completion(node, step_name)

        self.outputs[step_name] = _maybe_wrap_stream(output, node)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run(pipeline: PipelineDef, params: Any) -> None:
    if getattr(pipeline, "requires_async_runner", False):
        raise RuntimeError(
            "This pipeline contains async features (async def or AsyncIterator)"
            " and must be executed with async_run()."
        )
    PipelineExecutor(pipeline.dag).execute(params)
