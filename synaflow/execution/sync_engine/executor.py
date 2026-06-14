import itertools
import logging
from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException
from synaflow.core.types import (
    ErrorMaterializeContext,
    MaterializeContext,
    OnError,
)
from synaflow.core.observers import (
    PipelineEvent,
    StepEvent,
    MaterializationEvent,
    PipelineStartedContext,
    PipelineCompletedContext,
    PipelineFailedContext,
    StepStartedContext,
    StepCompletedContext,
    StepFailedContext,
    MaterializationStartedContext,
    MaterializationCompletedContext,
    MaterializationFailedContext,
)


def _dispatch_observers(observers: list, context: Any) -> None:
    log = logging.getLogger("synaflow")
    for obs in observers:
        try:
            obs.handler(context)
        except Exception as exc:
            log.warning(
                "Observer failed for event %s: %s", context.event, exc, exc_info=True
            )


class StepState:
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.completed_all_inputs = False


def _wrap_step_iterator(
    iterator: Iterator,
    step_state: StepState,
    step_name: str,
    node: Any,
    pipeline_name: str,
):
    try:
        for item in iterator:
            yield item
        step_state.success_count = 1
        step_state.completed_all_inputs = True
        ctx = StepCompletedContext(
            pipeline_name=pipeline_name,
            event=StepEvent.COMPLETED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
            success_count=step_state.success_count,
            error_count=step_state.error_count,
            completed_all_inputs=step_state.completed_all_inputs,
        )
        _dispatch_observers(node.observers, ctx)
    except Exception as exc:
        cause = exc.__cause__ or exc if isinstance(exc, PipelineStopException) else exc
        ctx = StepFailedContext(
            pipeline_name=pipeline_name,
            event=StepEvent.FAILED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
            success_count=step_state.success_count,
            error_count=step_state.error_count,
            completed_all_inputs=step_state.completed_all_inputs,
            exception=cause,
        )
        _dispatch_observers(node.observers, ctx)
        raise


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
    mat_name = getattr(mat, "__name__", str(mat)) if mat else None

    # Emit Materialization STARTED
    ctx_started = MaterializationStartedContext(
        pipeline_name=dag.name,
        event=MaterializationEvent.STARTED,
        step_name=step_name,
        dataset_name=step_name,
        consumer_type=consumer_type,
        materializer_name=mat_name,
    )
    _dispatch_observers(node.observers, ctx_started)

    try:
        if mat is None:
            res = (
                _collect_iterator(dag, step_name, value)
                if isinstance(value, Iterator)
                else value
            )
        else:
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
                res = items if concrete_mat is list else concrete_mat(items)
            elif (
                isinstance(value, Iterator)
                and getattr(concrete_mat, "__name__", "") == "_identity"
            ):
                res = _collect_iterator(dag, step_name, value)
            else:
                res = concrete_mat(value)

        # Emit Materialization COMPLETED
        ctx_completed = MaterializationCompletedContext(
            pipeline_name=dag.name,
            event=MaterializationEvent.COMPLETED,
            step_name=step_name,
            dataset_name=step_name,
            consumer_type=consumer_type,
            materializer_name=mat_name,
        )
        _dispatch_observers(node.observers, ctx_completed)
        return res
    except Exception as exc:
        # Emit Materialization FAILED
        ctx_failed = MaterializationFailedContext(
            pipeline_name=dag.name,
            event=MaterializationEvent.FAILED,
            step_name=step_name,
            dataset_name=step_name,
            consumer_type=consumer_type,
            materializer_name=mat_name,
            exception=exc,
        )
        _dispatch_observers(node.observers, ctx_failed)
        raise


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

    def execute(self, params: Any) -> None:
        pipeline_observers = self.dag.observers

        # Emit Pipeline STARTED
        ctx_started = PipelineStartedContext(
            pipeline_name=self.dag.name, event=PipelineEvent.STARTED
        )
        _dispatch_observers(pipeline_observers, ctx_started)

        for field, value in params._asdict().items():
            self.outputs[field] = value

        try:
            for level in self.dag.get_execution_levels():
                for step_name in level:
                    self._run_step(step_name)
        except PipelineStopException as exc:
            # Emit Pipeline FAILED
            ctx_failed = PipelineFailedContext(
                pipeline_name=self.dag.name,
                event=PipelineEvent.FAILED,
                step_name=exc.step_name,
                exception=exc.__cause__ or exc,
            )
            _dispatch_observers(pipeline_observers, ctx_failed)
            raise
        except Exception as exc:
            # Emit Pipeline FAILED for generic exception
            ctx_failed = PipelineFailedContext(
                pipeline_name=self.dag.name,
                event=PipelineEvent.FAILED,
                step_name=None,
                exception=exc,
            )
            _dispatch_observers(pipeline_observers, ctx_failed)
            raise
        else:
            # Emit Pipeline COMPLETED
            ctx_completed = PipelineCompletedContext(
                pipeline_name=self.dag.name, event=PipelineEvent.COMPLETED
            )
            _dispatch_observers(pipeline_observers, ctx_completed)

    def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        arguments = self._build_arguments(step_name, node)
        unrolled = self.dag.each_inputs(step_name)
        step_state = StepState()

        # Emit Step STARTED
        ctx_started = StepStartedContext(
            pipeline_name=self.dag.name,
            event=StepEvent.STARTED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
        )
        _dispatch_observers(node.observers, ctx_started)

        try:
            if unrolled:
                output = self._unroll_step(
                    step_name, node, arguments, unrolled, step_state
                )
            else:
                output = node.fn(**arguments)
                # If all-mode returns an iterator, we wrap it
                if isinstance(output, Iterator):
                    output = _wrap_step_iterator(
                        output, step_state, step_name, node, self.dag.name
                    )
                else:
                    step_state.success_count = 1
                    step_state.completed_all_inputs = True
                    # Emit Step COMPLETED
                    ctx_completed = StepCompletedContext(
                        pipeline_name=self.dag.name,
                        event=StepEvent.COMPLETED,
                        step_name=step_name,
                        mode=node.mode,
                        on_error=node.on_error,
                        success_count=step_state.success_count,
                        error_count=step_state.error_count,
                        completed_all_inputs=step_state.completed_all_inputs,
                    )
                    _dispatch_observers(node.observers, ctx_completed)

            if not step_name.startswith("_"):
                self._publish_output(step_name, output, node)
        except PipelineStopException as exc:
            ctx_failed = StepFailedContext(
                pipeline_name=self.dag.name,
                event=StepEvent.FAILED,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=step_state.success_count,
                error_count=step_state.error_count,
                completed_all_inputs=step_state.completed_all_inputs,
                exception=exc.__cause__ or exc,
            )
            _dispatch_observers(node.observers, ctx_failed)
            raise
        except Exception as exc:
            ctx_failed = StepFailedContext(
                pipeline_name=self.dag.name,
                event=StepEvent.FAILED,
                step_name=step_name,
                mode=node.mode,
                on_error=node.on_error,
                success_count=step_state.success_count,
                error_count=step_state.error_count,
                completed_all_inputs=step_state.completed_all_inputs,
                exception=exc,
            )
            _dispatch_observers(node.observers, ctx_failed)
            _handle_error(self.dag, step_name, exc)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc

    def _unroll_step(self, step_name, node, base_args, unrolled, step_state):
        """Call fn once per item-tuple. Exhausted streams yield None.

        If terminal (sink), consume eagerly without producing output.
        """
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
                    res = node.fn(**item_args)
                    step_state.success_count += 1
                    yield res
                except Exception as exc:
                    step_state.error_count += 1
                    _handle_error(self.dag, step_name, exc)
                    if on_err == OnError.STOP:
                        raise PipelineStopException(
                            step_name=step_name, cause=exc
                        ) from exc

        if self._is_terminal(step_name):
            try:
                for _ in generate():
                    pass
                step_state.completed_all_inputs = True
                ctx_completed = StepCompletedContext(
                    pipeline_name=self.dag.name,
                    event=StepEvent.COMPLETED,
                    step_name=step_name,
                    mode=node.mode,
                    on_error=node.on_error,
                    success_count=step_state.success_count,
                    error_count=step_state.error_count,
                    completed_all_inputs=step_state.completed_all_inputs,
                )
                _dispatch_observers(node.observers, ctx_completed)
            except Exception as exc:
                raise
            return None
        return _wrap_step_iterator(
            generate(), step_state, step_name, node, self.dag.name
        )

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

    def _publish_output(self, step_name, output, node):
        output = self._notify_observers(step_name, output)
        if isinstance(output, Iterator):
            consumers = self.dag.consumers_of(step_name)

            # 1. Step-level materialization required?
            if node.on_error == OnError.STOP or node.force_materialize:
                consumer_type = None
                if consumers:
                    consumer_type = self.dag[consumers[0]].deps.get(step_name)
                output = _apply_materializer(
                    self.dag, step_name, output, consumer_type=consumer_type
                )
                for c in consumers:
                    self.outputs[_output_key(self.dag, step_name, c)] = output
                return

            # 2. Single consumer requires materialized input?
            if len(consumers) == 1 and self.dag.needs_materialize(step_name):
                consumer_type = self.dag[consumers[0]].deps.get(step_name)
                output = _apply_materializer(
                    self.dag, step_name, output, consumer_type=consumer_type
                )
                self.outputs[_output_key(self.dag, step_name, consumers[0])] = output
                return

            # 3. Otherwise, keep it lazy / stream-based
            if len(consumers) > 1:
                branches = itertools.tee(output, len(consumers))
                for consumer, branch in zip(consumers, branches):
                    consumer_node = self.dag[consumer]
                    if step_name in consumer_node.materialized_deps:
                        branch = _apply_materializer(
                            self.dag,
                            step_name,
                            branch,
                            consumer_type=consumer_node.deps.get(step_name),
                        )
                    self.outputs[_output_key(self.dag, step_name, consumer)] = branch
                return
        elif self.dag.needs_materialize(step_name):
            output = _apply_materializer(
                self.dag, step_name, output, consumer_type=node.get("output")
            )
        self.outputs[step_name] = output


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
