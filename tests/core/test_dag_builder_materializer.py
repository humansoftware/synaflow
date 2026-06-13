from collections.abc import Iterator
from typing import NamedTuple

import pytest

from synaflow import pipeline, step
from synaflow.core.types import MaterializeContext

from .conftest import EmptyParams, build_minimal_dag


def test_given_step_level_materializer_when_dag_built_then_step_materializer_wins():
    def my_mat(iterator):
        return list(iterator)

    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(
        producer_fn=gen,
        consumer_fn=consumer,
        producer_materializer=my_mat,
    )
    assert p.dag.steps["producer"].materializer is my_mat


def test_given_pipeline_level_factory_when_dag_built_then_factory_stored():
    def my_factory(ctx: MaterializeContext):
        return list

    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(
        producer_fn=gen,
        consumer_fn=consumer,
        pipeline_materializer=my_factory,
    )
    assert p.dag.steps["producer"].materializer is my_factory


def test_given_no_custom_materializer_when_dag_built_then_default_factory_used():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    from synaflow.core.dag_builder import default_materializer_factory as _def

    assert p.dag.steps["producer"].materializer is _def


def test_given_default_factory_when_consumer_type_is_scalar_then_returns_identity():
    from synaflow.core.dag_builder import default_materializer_factory as _def
    from synaflow.core.types import MaterializeContext

    ctx = MaterializeContext(
        pipeline_name="test",
        dataset_name="step1",
        item_type=str,
        consumer_type=str,
    )
    mat = _def(ctx)
    assert mat(42) == 42


def test_given_default_factory_when_consumer_type_is_none_then_returns_list():
    from synaflow.core.dag_builder import default_materializer_factory as _def
    from synaflow.core.types import MaterializeContext

    ctx = MaterializeContext(
        pipeline_name="test",
        dataset_name="step1",
        item_type=int,
        consumer_type=None,
    )
    mat = _def(ctx)
    assert mat is list


def test_given_scalar_producer_when_dag_built_then_materializer_is_default_factory():
    class P(NamedTuple):
        x: int = 1

    def producer(x: int) -> int:
        return x * 2

    def consumer(producer: int) -> None:
        pass

    p = build_minimal_dag(
        producer_fn=producer,
        consumer_fn=consumer,
        params=P,
    )
    from synaflow.core.dag_builder import default_materializer_factory as _def

    assert p.dag.steps["producer"].materializer is _def


def test_given_default_error_factory_when_called_then_returns_callable():
    from synaflow.core.dag_builder import default_error_materializer_factory as _def
    from synaflow.core.types import ErrorMaterializeContext

    ctx = ErrorMaterializeContext(
        pipeline_name="test",
        dataset_name="step1",
        exception_type=ValueError,
    )
    handler = _def(ctx)
    assert callable(handler)


def test_given_pipeline_created_without_error_factory_then_uses_default():
    from synaflow.core.dag_builder import default_error_materializer_factory as _def

    class P(NamedTuple):
        x: int = 1

    def producer(x: int) -> int:
        return x

    def consumer(producer: int) -> None:
        pass

    p = build_minimal_dag(producer_fn=producer, consumer_fn=consumer, params=P)
    assert p.default_error_materializer_factory is _def
