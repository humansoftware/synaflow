import itertools
from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
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


def _output_key(dag: Dag, producer: str, consumer: str) -> str:
    if len(dag.consumers_of(producer)) > 1:
        return f"{producer}__{consumer}"
    return producer


# ---------------------------------------------------------------------------
# Runtime helpers (no flags needed on DagNode)
# ---------------------------------------------------------------------------


def _collect_iterator(dag: Dag, step_name: str, value: Iterator) -> list[Any]:
    items = []
    while True:
        try:
            items.append(next(value))
        except StopIteration:
            return items
        except Exception as exc:
            _handle_error(dag, step_name, exc)
            if dag[step_name].on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
            return items


def _apply_materializer(
    dag: Dag, step_name: str, value: Any, consumer_type: Any = None
) -> Any:
    node = dag[step_name]
    mat = node.get("materializer")
    if mat is None:
        return (
            _collect_iterator(dag, step_name, value)
            if isinstance(value, Iterator)
            else value
        )
    concrete_mat = mat(
        MaterializeContext(
            pipeline_name=dag.name,
            dataset_name=step_name,
            item_type=node.get("output"),
            consumer_type=consumer_type,
        )
    )
    if isinstance(value, Iterator) and concrete_mat in (list, tuple, set, dict):
        items = _collect_iterator(dag, step_name, value)
        return items if concrete_mat is list else concrete_mat(items)
    if (
        isinstance(value, Iterator)
        and getattr(concrete_mat, "__name__", "") == "_identity"
    ):
        return _collect_iterator(dag, step_name, value)
    return concrete_mat(value)


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

    # ------------------------------------------------------------------
    # Lifecycle observer dispatch helpers
    # ------------------------------------------------------------------

    def _dispatch_pipeline_event(self, event: PipelineEvent, **kw: Any) -> None:
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
                step_name=kw.get("step_name"),
                exception=kw.get("exception"),
            )
        else:
            return
        dispatch_observers(registrations, event, ctx)

    def _dispatch_step_event(
        self,
        node: Any,
        event: StepEvent,
        step_name: str,
        **kw: Any,
    ) -> None:
        registrations = getattr(node, "observers", None) or []
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
                success_count=kw.get("success_count", 0),
                error_count=kw.get("error_count", 0),
                completed_all_inputs=kw.get("completed_all_inputs", True),
            )
        elif event is StepEvent.FAILED:
            ctx = StepFailedContext(
                pipeline_name=self.dag.name,
                event=event,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=kw.get("success_count", 0),
                error_count=kw.get("error_count", 0),
                completed_all_inputs=kw.get("completed_all_inputs", False),
                exception=kw.get("exception"),
            )
        else:
            return
        dispatch_observers(registrations, event, ctx)

    def _dispatch_materialization_event(
        self,
        step_name: str,
        node: Any,
        event: MaterializationEvent,
        consumer_type: Any = None,
        materializer_name: str | None = None,
        **kw: Any,
    ) -> None:
        registrations = getattr(node, "observers", None) or []
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
                exception=kw.get("exception"),
            )
        else:
            return
        dispatch_observers(registrations, event, ctx)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, params: Any) -> None:
        for field, value in params._asdict().items():
            self.outputs[field] = value

        self._dispatch_pipeline_event(PipelineEvent.STARTED)
        try:
            for level in self.dag.get_execution_levels():
                for step_name in level:
                    self._run_step(step_name)
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

    def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        arguments = self._build_arguments(step_name, node)
        unrolled = self.dag.each_inputs(step_name)
        is_each = bool(unrolled)

        self._dispatch_step_event(node, StepEvent.STARTED, step_name)

        try:
            if unrolled:
                output = self._unroll_step(step_name, node, arguments, unrolled)
            else:
                output = node.fn(**arguments)

            if not is_each:
                self._dispatch_step_event(
                    node,
                    StepEvent.COMPLETED,
                    step_name,
                    success_count=1,
                    error_count=0,
                    completed_all_inputs=True,
                )

            if not step_name.startswith("_"):
                self._publish_output(step_name, output, node)
        except PipelineStopException as exc:
            cause = exc.cause or exc
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
            raise
        except Exception as exc:
            _handle_error(self.dag, step_name, exc)
            self._dispatch_step_event(
                node,
                StepEvent.FAILED,
                step_name,
                success_count=0,
                error_count=1,
                completed_all_inputs=False,
                exception=exc,
            )
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc

    def _unroll_step(self, step_name, node, base_args, unrolled):
        """Call fn once per item-tuple. Exhausted streams yield None.
        If terminal (sink), consume eagerly without producing output."""
        iterators = {}
        for dep in unrolled:
            key = _output_key(self.dag, dep, step_name)
            source = self.outputs.get(key, self.outputs.get(dep))
            iterators[dep] = iter(source if source is not None else [])

        on_err = node.on_error

        def generate():
            while True:
                item_args = dict(base_args)
                exhausted = 0
                for dep in unrolled:
                    try:
                        item_args[dep] = next(iterators[dep])
                    except StopIteration:
                        item_args[dep] = None
                        exhausted += 1
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

        if self._is_terminal(step_name):
            for _ in generate():
                pass
            return None
        return generate()

    def _is_terminal(self, step_name):
        return step_name.startswith("_") or not self.dag.consumers_of(step_name)

    def _build_arguments(self, consumer, node):
        args = {}
        for dep_name in node.deps:
            key = _output_key(self.dag, dep_name, consumer)
            args[dep_name] = self.outputs.get(key, self.outputs.get(dep_name))
        return args

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
            result = _apply_materializer(
                self.dag, step_name, output, consumer_type=consumer_type
            )
            self._dispatch_materialization_event(
                step_name,
                node,
                MaterializationEvent.COMPLETED,
                consumer_type,
                mat_name,
            )
            return result
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

    def _publish_output(self, step_name, output, node):
        output = self._notify_observers(step_name, output)
        is_each = node.mode == StepMode.EACH

        if isinstance(output, Iterator):
            consumers = self.dag.consumers_of(step_name)

            # 1. Step-level materialization required?
            if node.on_error == OnError.STOP or node.force_materialize:
                consumer_type = None
                if consumers:
                    consumer_type = self.dag[consumers[0]].deps.get(step_name)
                output = self._materialize_with_events(
                    step_name, output, node, consumer_type=consumer_type
                )
                if is_each:
                    success = len(output) if hasattr(output, "__len__") else 0
                    self._dispatch_step_event(
                        node,
                        StepEvent.COMPLETED,
                        step_name,
                        success_count=success,
                        error_count=0,
                        completed_all_inputs=True,
                    )
                for c in consumers:
                    self.outputs[_output_key(self.dag, step_name, c)] = output
                return

            # 2. Single consumer requires materialized input?
            if len(consumers) == 1 and self.dag.needs_materialize(step_name):
                consumer_type = self.dag[consumers[0]].deps.get(step_name)
                output = self._materialize_with_events(
                    step_name, output, node, consumer_type=consumer_type
                )
                if is_each:
                    success = len(output) if hasattr(output, "__len__") else 0
                    self._dispatch_step_event(
                        node,
                        StepEvent.COMPLETED,
                        step_name,
                        success_count=success,
                        error_count=0,
                        completed_all_inputs=True,
                    )
                self.outputs[_output_key(self.dag, step_name, consumers[0])] = output
                return

            # 3. Otherwise, keep it lazy / stream-based
            if len(consumers) > 1:
                branches = itertools.tee(output, len(consumers))
                for consumer, branch in zip(consumers, branches):
                    consumer_node = self.dag[consumer]
                    if step_name in consumer_node.materialized_deps:
                        branch = self._materialize_with_events(
                            step_name,
                            branch,
                            node,
                            consumer_type=consumer_node.deps.get(step_name),
                        )
                    self.outputs[_output_key(self.dag, step_name, consumer)] = branch
                if is_each:
                    self._dispatch_step_event(
                        node,
                        StepEvent.COMPLETED,
                        step_name,
                        success_count=0,
                        error_count=0,
                        completed_all_inputs=True,
                    )
                return

            if is_each:
                self._dispatch_step_event(
                    node,
                    StepEvent.COMPLETED,
                    step_name,
                    success_count=0,
                    error_count=0,
                    completed_all_inputs=True,
                )

        elif self.dag.needs_materialize(step_name):
            output = self._materialize_with_events(
                step_name, output, node, consumer_type=node.get("output")
            )

        self.outputs[step_name] = output
        if is_each and not isinstance(output, Iterator):
            success = len(output) if hasattr(output, "__len__") else 1
            self._dispatch_step_event(
                node,
                StepEvent.COMPLETED,
                step_name,
                success_count=success,
                error_count=0,
                completed_all_inputs=True,
            )


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
