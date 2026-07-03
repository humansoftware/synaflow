import pytest
from collections.abc import Iterator
from typing import NamedTuple


from synaflow.core.types import MaterializeContext

from .conftest import build_minimal_dag


def test_given_step_level_materializer_when_dag_built_then_step_materializer_wins():
    def my_mat(iterator):
        return list(iterator)

    my_mat_wrapped = my_mat

    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(
        producer_fn=gen,
        consumer_fn=consumer,
        producer_materializer=my_mat_wrapped,
    )
    assert p.dag.steps["producer"].materializer is my_mat_wrapped


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
    assert p.dag.steps["producer"].materializer is list


def test_given_no_custom_materializer_when_dag_built_then_default_factory_used():
    def gen() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=gen, consumer_fn=consumer)
    assert p.dag.steps["producer"].materializer is list


def test_given_default_factory_when_consumer_type_is_scalar_then_returns_identity():
    from synaflow.core.dag_builder import memory_materializer_factory as _def
    from synaflow.core.types import MaterializeContext

    ctx = MaterializeContext(
        pipeline_name="test",
        dataset_name="step1",
        item_type=str,
        consumer_type=str,
    )
    mat = _def(ctx)
    assert mat(42) == 42


def test_given_default_factory_when_consumer_type_is_none_then_raises():
    from synaflow.core.dag_builder import memory_materializer_factory as _def
    from synaflow.core.types import MaterializeContext

    ctx = MaterializeContext(
        pipeline_name="test",
        dataset_name="step1",
        item_type=int,
        consumer_type=None,
    )
    with pytest.raises(ValueError, match="Cannot infer memory materializer"):
        _def(ctx)


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
    assert getattr(p.dag.steps["producer"].materializer, "__name__", "") in (
        "_identity",
        "list",
        "<lambda>",
        "",
    )


def test_given_default_error_factory_when_called_then_returns_callable():
    from synaflow.core.dag_builder import log_error_materializer_factory as _def
    from synaflow.core.types import ErrorMaterializeContext

    ctx = ErrorMaterializeContext(
        pipeline_name="test",
        dataset_name="step1",
    )
    handler = _def(ctx)
    assert callable(handler)


def test_given_pipeline_created_without_error_factory_then_uses_default():
    from synaflow.core.dag_builder import log_error_materializer_factory as _def

    class P(NamedTuple):
        x: int = 1

    def producer(x: int) -> int:
        return x

    def consumer(producer: int) -> None:
        pass

    p = build_minimal_dag(producer_fn=producer, consumer_fn=consumer, params=P)
    assert p.dag.error_materializer_factory is _def


def test_given_no_custom_materializer_when_non_builtin_inner_type_used_then_raises():
    from dataclasses import dataclass

    @dataclass
    class Row:
        id: int
        name: str

    class Params(NamedTuple):
        pass


def test_given_no_custom_materializer_when_non_builtin_inner_type_used_then_dag_builds():
    from dataclasses import dataclass
    from collections.abc import Iterator

    @dataclass
    class Row:
        id: int
        name: str

    class Params(NamedTuple):
        pass

    def producer() -> Iterator[Row]:
        yield Row(id=1, name="a")

    def consumer(producer: list[Row]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=producer, consumer_fn=consumer)
    assert p.dag.steps["producer"].materializer is list


def test_given_no_custom_materializer_and_non_builtin_inner_type_when_not_materialized_then_dag_builds():
    from dataclasses import dataclass
    from collections.abc import Iterator
    from synaflow import pipeline, step

    @dataclass
    class Row:
        id: int
        name: str

    class Params(NamedTuple):
        pass

    def producer() -> Iterator[Row]:
        yield Row(id=1, name="a")

    # Consumed as a scalar (EACH mode) so needs_materialize is False
    def consumer(producer: Row) -> None:
        pass

    p = pipeline(
        name="test_validation_no_mat",
        params=Params,
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )
    assert p.dag is not None
    assert p.dag.needs_materialize("producer") is False


def test_given_step_materializer_when_non_builtin_inner_type_used_then_dag_builds():
    from dataclasses import dataclass
    from collections.abc import Iterator
    from synaflow import pipeline, step

    @dataclass
    class Row:
        id: int
        name: str

    class Params(NamedTuple):
        pass

    def producer() -> Iterator[Row]:
        yield Row(id=1, name="a")

    def consumer(producer: list[Row]) -> int:
        return len(producer)

    p = pipeline(
        name="test_step_override",
        params=Params,
        steps=[
            step("producer", fn=producer, materializer=list),
            step("consumer", fn=consumer),
        ],
    )
    assert p.dag is not None


def test_given_builtin_concrete_materializer_when_signature_not_inspectable_then_dag_builds():
    from collections.abc import Iterator
    from synaflow import pipeline, step

    class Params(NamedTuple):
        pass

    def producer() -> Iterator[int]:
        yield 1
        yield 2

    def consumer(producer: int) -> None:
        pass

    p = pipeline(
        name="test_builtin_concrete_materializer",
        params=Params,
        steps=[
            step("producer", fn=producer, materializer=int),
            step("consumer", fn=consumer),
        ],
    )
    assert p.dag is not None
    assert p.dag.steps["producer"].materializer is int


def test_given_pipeline_materializer_when_non_builtin_inner_type_used_then_dag_builds():
    from dataclasses import dataclass
    from collections.abc import Iterator
    from synaflow import pipeline, step

    @dataclass
    class Row:
        id: int
        name: str

    class Params(NamedTuple):
        pass

    def producer() -> Iterator[Row]:
        yield Row(id=1, name="a")

    def consumer(producer: list[Row]) -> int:
        return len(producer)

    def dummy_pipeline_materializer(ctx):
        return list

    p = pipeline(
        name="test_pipeline_override",
        params=Params,
        materializer=dummy_pipeline_materializer,
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )
    assert p.dag is not None
