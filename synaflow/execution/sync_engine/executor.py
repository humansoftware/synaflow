import inspect
import itertools
from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException
from synaflow.core.types import MaterializeContext, OnError


def _output_key(dag: Dag, producer: str, consumer: str) -> str:
    if len(dag.consumers_of(producer)) > 1:
        return f"{producer}__{consumer}"
    return producer


# ---------------------------------------------------------------------------
# Runtime helpers (no flags needed on DagNode)
# ---------------------------------------------------------------------------


def _apply_materializer(dag: Dag, step_name: str, iterator: Iterator) -> Any:
    node = dag[step_name]
    mat = node.get("materializer")
    if mat is None:
        return list(iterator)
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
            )
        )
    return mat(iterator)


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class PipelineExecutor:
    def __init__(self, dag: Dag, *, step_output_observers: list = None):
        self.dag = dag
        self.outputs = {}
        self._step_output_observers = step_output_observers or []

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
        if isinstance(output, Iterator) and node.needs_materialize:
            output = _apply_materializer(self.dag, step_name, output)
        elif isinstance(output, Iterator):
            consumers = self.dag.consumers_of(step_name)
            if len(consumers) > 1:
                branches = itertools.tee(output, len(consumers))
                for consumer, branch in zip(consumers, branches):
                    self.outputs[_output_key(self.dag, step_name, consumer)] = branch
                return
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
