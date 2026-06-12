import inspect
import itertools
from collections.abc import Iterator
from typing import Any, Iterable

from synaflow.core.pipeline import PipelineDef
from synaflow.core.types import MaterializeContext


class TeeWrapper:
    def __init__(self, tees: dict[str, Iterator]):
        self.tees = tees


class SyncStreamManager:
    def __init__(self, pipeline: PipelineDef, context: dict[str, Any]):
        self.pipeline = pipeline
        self.dag = pipeline._dag
        self.context = context

    def apply_materializer(self, name: str, iterator: Iterator) -> Iterable:
        node = self.dag.get(name)
        step_def = None
        if node and node.get("fn"):
            step_def = next((s for s in self.pipeline.steps if s.name == name), None)

        mat = getattr(step_def, "materializer", None) if step_def else None

        if mat is None:
            mat = self.pipeline.default_materializer_factory

        if mat is None:
            return list(iterator)

        sig = inspect.signature(mat)
        if (
            len(sig.parameters) > 1
            or "ctx" in sig.parameters
            or "context" in sig.parameters
        ):
            ctx = MaterializeContext(
                pipeline_name=self.pipeline.name,
                dataset_name=name,
                item_type=node.get("output") if node else Any,
            )
            mat = mat(ctx)

        return mat(iterator)

    def tee_iterator_for_consumers(
        self, producer_name: str, iterator_value: Iterator
    ) -> Any:
        consumers = [
            consumer_name
            for consumer_name, node in self.dag.items()
            if producer_name in node.get("deps", {})
        ]
        if len(consumers) > 1:
            tees = itertools.tee(iterator_value, len(consumers))
            return TeeWrapper(dict(zip(consumers, tees)))
        return iterator_value
