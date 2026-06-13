import inspect
import itertools
from collections.abc import Generator, Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.types import MaterializeContext


class TeeWrapper:
    def __init__(self, tees: dict[str, Iterator]):
        self.tees = tees


def handle_step_output(dag: Dag, step_name: str, output: Any) -> Any:
    """Route a step's output: tee for multiple consumers, materialize, or pass-through."""
    if not isinstance(output, Iterator):
        return output
    node = dag[step_name]
    if node.get("needs_materialize"):
        return apply_materializer(dag, step_name, output)
    consumers = dag.consumers_of(step_name)
    if len(consumers) > 1:
        tees = itertools.tee(output, len(consumers))
        return TeeWrapper(dict(zip(consumers, tees)))
    return output


def apply_materializer(dag: Dag, step_name: str, iterator: Iterator) -> Any:
    """Call the pre-computed materializer on the iterator."""
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
        ctx = MaterializeContext(
            pipeline_name=dag.name,
            dataset_name=step_name,
            item_type=node.get("output"),
        )
        mat = mat(ctx)
    return mat(iterator)


def resolve_dependency(value: Any, consumer_name: str) -> Any:
    """Unwrap tee'd stream to get this consumer's copy."""
    if isinstance(value, TeeWrapper):
        return value.tees[consumer_name]
    return value
