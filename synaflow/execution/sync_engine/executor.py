import inspect
import itertools
from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException
from synaflow.core.types import (
    ErrorInterceptorContext,
    ErrorMaterializeContext,
    MaterializeContext,
    OnError,
)


def _output_key(dag: Dag, producer: str, consumer: str) -> str:
    if len(dag.consumers_of(producer)) > 1:
        return f"{producer}__{consumer}"
    return producer


# ---------------------------------------------------------------------------
# Runtime helpers (no flags needed on DagNode)
# ---------------------------------------------------------------------------


def _collect_iterator(
    dag: Dag, step_name: str, value: Iterator, inputs: dict[str, Any] | None = None
) -> list[Any]:
    items = []
    while True:
        try:
            items.append(next(value))
        except StopIteration:
            return items
        except Exception as exc:
            _trigger_interceptors(dag, step_name, exc, inputs)
            _handle_error(dag, step_name, exc)
            if dag[step_name].on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
            return items


def _apply_materializer(
    dag: Dag,
    step_name: str,
    value: Any,
    consumer_type: Any = None,
    inputs: dict[str, Any] | None = None,
) -> Any:
    node = dag[step_name]
    mat = node.get("materializer")
    if mat is None:
        return (
            _collect_iterator(dag, step_name, value, inputs=inputs)
            if isinstance(value, Iterator)
            else value
        )
    sig = inspect.signature(mat)
    if (
        len(sig.parameters) > 1
        or "ctx" in sig.parameters
        or "context" in sig.parameters
    ):
        mat = mat(
            MaterializeContext(
                pipeline_name=dag.name,
                dataset_name=step_name,
                item_type=node.get("output"),
                consumer_type=consumer_type,
            )
        )
    if isinstance(value, Iterator) and mat in (list, tuple, set, dict):
        items = _collect_iterator(dag, step_name, value, inputs=inputs)
        return items if mat is list else mat(items)
    if isinstance(value, Iterator) and getattr(mat, "__name__", "") == "_identity":
        return _collect_iterator(dag, step_name, value, inputs=inputs)
    return mat(value)


def _handle_error(dag: Dag, step_name: str, exc: BaseException) -> None:
    factory = getattr(dag, "error_materializer_factory", None)
    if factory is None:
        return

    handler = factory(
        ErrorMaterializeContext(
            pipeline_name=dag.name,
            dataset_name=step_name,
            exception_type=type(exc),
        )
    )
    if callable(handler):
        handler(exc)


def _trigger_interceptors(
    dag: Dag, step_name: str, exc: Exception, inputs: dict[str, Any] | None
) -> None:
    node = dag.steps.get(step_name)
    if not node:
        return

    ctx = ErrorInterceptorContext(
        pipeline_name=dag.name,
        step_name=step_name,
        inputs=inputs or {},
    )

    for interceptor in getattr(node, "error_interceptors", []):
        try:
            interceptor(exc, ctx)
        except Exception as interceptor_exc:
            import logging

            logging.getLogger("synaflow").warning(
                f"Error in interceptor {interceptor.__name__ if hasattr(interceptor, '__name__') else interceptor} for step '{step_name}': {interceptor_exc}"
            )


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class PipelineExecutor:
    def __init__(self, dag: Dag, *, step_output_observers: list = None):
        self.dag = dag
        self.outputs = {}
        self._step_output_observers = step_output_observers or []
        self._step_inputs = {}

    def execute(self, params: Any) -> None:
        for field, value in params._asdict().items():
            self.outputs[field] = value

        for level in self.dag.get_execution_levels():
            for step_name in level:
                self._run_step(step_name)

    def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        arguments = self._build_arguments(step_name, node)
        self._step_inputs[step_name] = arguments
        unrolled = self.dag.each_inputs(step_name)

        try:
            if unrolled:
                output = self._unroll_step(step_name, node, arguments, unrolled)
            else:
                output = node.fn(**arguments)

            if not step_name.startswith("_"):
                self._publish_output(step_name, output, node)
        except PipelineStopException:
            raise
        except Exception as exc:
            _trigger_interceptors(self.dag, step_name, exc, arguments)
            _handle_error(self.dag, step_name, exc)
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
                    _trigger_interceptors(self.dag, step_name, exc, item_args)
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

    def _publish_output(self, step_name, output, node):
        output = self._notify_observers(step_name, output)
        inputs = self._step_inputs.get(step_name)
        if isinstance(output, Iterator):
            consumers = self.dag.consumers_of(step_name)
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
                            inputs=inputs,
                        )
                    self.outputs[_output_key(self.dag, step_name, consumer)] = branch
                return
            if self.dag.needs_materialize(step_name):
                consumer_type = None
                if consumers:
                    consumer_type = self.dag[consumers[0]].deps.get(step_name)
                output = _apply_materializer(
                    self.dag,
                    step_name,
                    output,
                    consumer_type=consumer_type,
                    inputs=inputs,
                )
        elif self.dag.needs_materialize(step_name):
            output = _apply_materializer(
                self.dag,
                step_name,
                output,
                consumer_type=node.get("output"),
                inputs=inputs,
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
