import inspect
from typing import Generator, Iterator, List, NamedTuple
from unittest.mock import MagicMock, call

import pytest

from synaflow import pipeline, step
from synaflow.core.types import OnError


def mock_step(**params: type) -> MagicMock:
    mock = MagicMock()
    if params:
        annotations = {name: tp for name, tp in params.items()}
        mock.__annotations__ = annotations
        mock.__globals__ = {}
        mock.__signature__ = inspect.Signature(
            [
                inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=tp)
                for name, tp in annotations.items()
            ]
        )
    else:
        mock.__signature__ = inspect.Signature([])
    return mock


def test_given_generator_output_and_two_each_consumers_when_run_then_materialized_once(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int):
        call_order.append(("a", items))

    def b(items: int):
        call_order.append(("b", items))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


def test_given_generator_and_scalar_and_iterator_consumers_when_run_then_no_materialization(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int):
        call_order.append(("a", items))

    def b(items: Iterator[int]):
        for x in items:
            call_order.append(("b", x))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


def test_given_generator_and_two_iterator_consumers_when_run_then_no_materialization(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: Iterator[int]):
        for x in items:
            call_order.append(("a", x))

    def b(items: Iterator[int]):
        for x in items:
            call_order.append(("b", x))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


def test_given_generator_and_union_scalar_and_union_iterator_consumers_when_run_then_no_materialization(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int | str):
        call_order.append(("a", items))

    def b(items: Iterator[int | str]):
        for x in items:
            call_order.append(("b", x))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


def test_given_generator_of_union_and_union_scalar_consumers_when_run_then_no_materialization(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int | str, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int | str | None):
        call_order.append(("a", items))

    def b(items: int | str | bool):
        call_order.append(("b", items))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


def test_given_generator_and_list_consumer_when_run_then_materialized_once(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int):
        call_order.append(("a", items))

    def b(items: list[int]):
        call_order.append(("b", items))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 1
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [[0, 1, 2]]


def test_given_generator_and_each_transformer_and_iterator_consumer_when_run_then_no_materialization(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[str, None, None]:
        for i in range(count):
            yield f"item_{i}"

    call_order = []

    def a(items: str) -> str:
        call_order.append(("a", items))
        return items.upper()

    def b(a: Iterator[str]):
        for x in a:
            call_order.append(("b", x))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [
        "item_0",
        "item_1",
        "item_2",
    ]
    assert [val for key, val in call_order if key == "b"] == [
        "ITEM_0",
        "ITEM_1",
        "ITEM_2",
    ]


def test_given_generator_and_eager_each_and_eager_iterator_consumers_when_run_then_lockstep_order(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int):
        call_order.append(("a", items))

    def b(items: Iterator[int]):
        for x in items:
            call_order.append(("b", x))

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


def test_given_generator_and_set_consumer_when_run_then_materialized_once(run_pipeline):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int):
        call_order.append(("a", items))

    def b(items: set[int]):
        call_order.append(("b", items))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 1
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [{0, 1, 2}]


def test_given_two_generators_when_consumed_by_single_step_then_no_materialization(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen1(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def gen2(count: int) -> Generator[int, None, None]:
        for i in range(count):
            yield i + 10

    call_order = []

    def c(gen1: Iterator[int], gen2: Iterator[int]):
        for x in gen1:
            call_order.append(("c1", x))
        for y in gen2:
            call_order.append(("c2", y))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("gen1", fn=gen1),
            step("gen2", fn=gen2),
            step("c", fn=c),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "c1"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "c2"] == [10, 11, 12]


def test_given_chain_and_bypass_dependencies_when_run_then_no_materialization(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int) -> int:
        call_order.append(("a", items))
        return items * 2

    def b(a: Iterator[int], items: Iterator[int]):
        for x in a:
            call_order.append(("b_a", x))
        for y in items:
            call_order.append(("b_items", y))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b_a"] == [0, 2, 4]
    assert [val for key, val in call_order if key == "b_items"] == [0, 1, 2]


def test_given_generator_and_tuple_consumer_when_run_then_materialized_once(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def a(items: int):
        call_order.append(("a", items))

    def b(items: tuple[int, ...]):
        call_order.append(("b", items))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 1
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [(0, 1, 2)]


def test_given_scalar_producer_and_list_and_iterator_consumers_when_run_then_wrapped_as_single_element_collections(
    run_pipeline,
):
    class P(NamedTuple):
        val: int = 42

    call_order = []

    def s1(val: int) -> int:
        call_order.append(("s1", val))
        return val

    def s2(s1: list[int]):
        call_order.append(("s2", s1))

    def s3(s1: Iterator[int]):
        call_order.append(("s3", list(s1)))

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", fn=s1),
            step("s2", fn=s2),
            step("s3", fn=s3),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert [val for key, val in call_order if key == "s1"] == [42]
    assert [val for key, val in call_order if key == "s2"] == [[42]]
    assert [val for key, val in call_order if key == "s3"] == [[42]]


def test_given_collection_producer_and_scalar_transformer_and_iterator_consumer_when_run_then_lazy_stream_no_materialization(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    call_order = []

    def s2(items: int) -> int:
        call_order.append(("s2", items))
        return items + 10

    def s3(s2: Iterator[int]):
        for val in s2:
            call_order.append(("s3", val))

    materialized = []

    def spy_materialize(g):
        materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test",
        params=P,
        default_materializer_factory=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("s2", fn=s2),
            step("s3", fn=s3),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "s2"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "s3"] == [10, 11, 12]
