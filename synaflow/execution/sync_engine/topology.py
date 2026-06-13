import inspect
import itertools
from collections.abc import Iterator
from typing import Any, Iterable

from synaflow.core.definition import PipelineDef
from synaflow.core.types import MaterializeContext


class TeeWrapper:
    def __init__(self, tees: dict[str, Iterator]):
        self.tees = tees


class SyncStreamManager:
    def __init__(self, pipeline: PipelineDef):
        self.pipeline = pipeline
        self.dag = pipeline.dag

    def apply_materializer(self, name: str, iterator: Iterator) -> Iterable:
        node = self.dag.get(name, {})
        mat = node.get("materializer")

        if mat is None:
            return list(iterator)

        sig = inspect.signature(mat)
        if (
            len(sig.parameters) > 1
            or "ctx" in sig.parameters
            or "context" in sig.parameters
        ):
            ctx = MaterializeContext(
                pipeline_name=self.dag.name,
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
