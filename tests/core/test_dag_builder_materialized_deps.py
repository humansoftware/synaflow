from collections.abc import Iterator
from typing import NamedTuple

from synaflow import pipeline, step
from synaflow.core.types import OnError

from .conftest import EmptyParams, build_minimal_dag


def test_given_consumer_wants_list_when_dag_built_then_producer_needs_materialization():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.needs_materialize("producer") is True


def test_given_consumer_wants_set_when_dag_built_then_producer_needs_materialization():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: set[int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.needs_materialize("producer") is True


def test_given_consumer_wants_tuple_when_dag_built_then_producer_needs_materialization():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: tuple[int, ...]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.needs_materialize("producer") is True


def test_given_consumer_wants_iterator_when_dag_built_then_producer_stays_lazy():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: Iterator[int]) -> list[int]:
        return list(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.needs_materialize("producer") is False


def test_given_producer_on_error_stop_when_dag_built_then_producer_needs_materialization():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: Iterator[int]) -> list[int]:
        return list(producer)

    p = build_minimal_dag(
        producer_fn=gen,
        consumer_fn=consumer,
        producer_on_error=OnError.STOP,
    )
    assert p.dag.needs_materialize("producer") is True


def test_given_mixed_consumers_when_dag_built_then_producer_materializes_for_all_consumers():
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
    assert p.dag.needs_materialize("gen") is True
    assert p.dag.steps["a"]._materialized_deps == ["gen"]
    assert p.dag.steps["b"]._materialized_deps == ["gen"]


def test_given_force_materialize_when_dag_built_then_upstream_producer_materializes():
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
    assert p.dag.needs_materialize("gen") is True


def test_given_dag_serialized_when_exported_then_private_materialized_deps_are_hidden():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    exported = p.dag.to_dict()

    assert "materialized_deps" not in exported["steps"]["producer"]
    assert "materialized_deps" not in exported["steps"]["consumer"]


def test_given_materialized_producer_when_exported_then_debug_reasons_are_serialized():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    exported = p.dag.to_dict()

    assert exported["steps"]["producer"]["needs_materialize_reasons"] == [
        "consumer_requires_materialized_type"
    ]
    assert "needs_materialize_reasons" not in exported["steps"]["consumer"]
