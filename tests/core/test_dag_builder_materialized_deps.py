from collections.abc import Iterator
from typing import NamedTuple

from synaflow import pipeline, step
from synaflow.core.types import OnError

from ._dag_builder_data import COMPATIBILITY_TABLE_ON_ERROR_STOP
from .conftest import EmptyParams, build_minimal_dag


def test_given_consumer_wants_list_when_dag_built_then_materialized_deps_set():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.steps["consumer"].materialized_deps == ["producer"]


def test_given_consumer_wants_set_when_dag_built_then_materialized_deps_set():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: set[int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.steps["consumer"].materialized_deps == ["producer"]


def test_given_consumer_wants_tuple_when_dag_built_then_materialized_deps_set():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: tuple[int, ...]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.steps["consumer"].materialized_deps == ["producer"]


def test_given_consumer_wants_iterator_when_dag_built_then_no_materialized_deps():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: Iterator[int]) -> list[int]:
        return list(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.steps["consumer"].materialized_deps == []


def test_given_producer_on_error_stop_when_dag_built_then_consumer_materialized_deps_set():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: Iterator[int]) -> list[int]:
        return list(producer)

    p = build_minimal_dag(
        producer_fn=gen,
        consumer_fn=consumer,
        producer_on_error=OnError.STOP,
    )
    assert p.dag.steps["consumer"].materialized_deps == ["producer"]


def test_given_consumer_wants_iterator_with_two_consumers_when_dag_built_then_only_materialized_consumer_has_deps():
    def gen() -> Iterator[int]:
        yield 1

    def consumer_a(gen: int) -> None:
        pass

    def consumer_b(gen: list[int]) -> int:
        return len(gen)

    class P(NamedTuple):
        pass

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("gen", fn=gen),
            step("a", fn=consumer_a),
            step("b", fn=consumer_b),
        ],
    )
    assert p.dag.steps["a"].materialized_deps == []
    assert p.dag.steps["b"].materialized_deps == ["gen"]


def test_given_force_materialize_when_dag_built_then_all_deps_materialized():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(gen: Iterator[int]) -> list[int]:
        return list(gen)

    p = pipeline(
        name="test",
        params=EmptyParams,
        steps=[
            step("gen", fn=gen),
            step("consumer", fn=consumer, force_materialize=True),
        ],
    )
    assert p.dag.steps["consumer"].materialized_deps == ["gen"]
