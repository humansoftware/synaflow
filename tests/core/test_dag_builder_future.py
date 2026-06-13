"""
Tests for expected DagBuilder behavior not yet implemented.
xfail = expected to fail until implemented.
"""
from collections.abc import Iterator
from typing import NamedTuple

import pytest

from synaflow import pipeline, step
from synaflow.core.types import OnError

from .conftest import EmptyParams, IntParam, build_minimal_dag

# ---------------------------------------------------------------------------
# Iterator[tuple[K,V]] -> dict[K,V]
# ---------------------------------------------------------------------------


class KVParam(NamedTuple):
    pass


@pytest.mark.xfail(
    reason="type system does not yet accept Iterator[tuple[K,V]] -> dict[K,V]"
)
def test_given_iterator_of_pairs_when_consumer_wants_dict_then_dag_builds():
    def producer() -> Iterator[tuple[str, int]]:
        yield ("a", 1)

    def consumer(producer: dict[str, int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=producer, consumer_fn=consumer, params=KVParam)
    assert p._dag.nodes["consumer"].materialized_deps == ["producer"]


# ---------------------------------------------------------------------------
# dict.items() -> Iterator[tuple[K,V]] (dict exports pairs naturally)
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="dict get_inner_type returns keys, not items. Needs special handling for .items() semantics"
)
def test_given_dict_producer_when_consumer_wants_iterator_of_items_then_no_materialized_deps():
    class DictParam(NamedTuple):
        data: dict[str, int] = {"a": 1}

    def producer(data: dict[str, int]) -> dict[str, int]:
        return data

    def consumer(producer: Iterator[tuple[str, int]]) -> list[tuple[str, int]]:
        return list(producer)

    p = build_minimal_dag(producer_fn=producer, consumer_fn=consumer, params=DictParam)
    assert p._dag.nodes["consumer"].materialized_deps == []


# ---------------------------------------------------------------------------
# Custom type without materializer should raise
# ---------------------------------------------------------------------------


@pytest.mark.xfail(reason="custom type materializer validation not yet implemented")
def test_given_custom_output_type_without_materializer_when_dag_built_then_raises():
    class CustomType:
        pass

    def producer() -> Iterator[CustomType]:
        yield CustomType()

    def consumer(producer: list[CustomType]) -> int:
        return len(producer)

    with pytest.raises(ValueError, match="materializer"):
        build_minimal_dag(producer_fn=producer, consumer_fn=consumer, params=KVParam)
