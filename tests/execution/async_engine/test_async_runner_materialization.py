import inspect
from typing import AsyncGenerator, AsyncIterator, Generator, Iterator, List, NamedTuple
from unittest.mock import AsyncMock as MagicMock
from unittest.mock import call

import pytest

from synaflow import async_run, pipeline, step
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


async def test_given_generator_output_and_two_each_consumers_when_run_then_materialized_once():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int):
        call_order.append(("a", items))

    async def b(items: int):
        call_order.append(("b", items))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


async def test_given_generator_and_scalar_and_iterator_consumers_when_run_then_no_materialization():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int):
        call_order.append(("a", items))

    async def b(items: AsyncIterator[int]):
        async for x in items:
            call_order.append(("b", x))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


async def test_given_generator_and_two_iterator_consumers_when_run_then_no_materialization():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: AsyncIterator[int]):
        async for x in items:
            call_order.append(("a", x))

    async def b(items: AsyncIterator[int]):
        async for x in items:
            call_order.append(("b", x))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


async def test_given_generator_and_union_scalar_and_union_iterator_consumers_when_run_then_no_materialization():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int | str):
        call_order.append(("a", items))

    async def b(items: AsyncIterator[int | str]):
        async for x in items:
            call_order.append(("b", x))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


async def test_given_generator_of_union_and_union_scalar_consumers_when_run_then_no_materialization():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int | str, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int | str | None):
        call_order.append(("a", items))

    async def b(items: int | str | bool):
        call_order.append(("b", items))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


async def test_given_generator_and_list_consumer_when_run_then_materialized_once():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int):
        call_order.append(("a", items))

    async def b(items: list[int]):
        call_order.append(("b", items))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 1
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [[0, 1, 2]]


async def test_given_generator_and_each_transformer_and_iterator_consumer_when_run_then_no_materialization():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[str, None]:
        for i in range(count):
            yield f"item_{i}"

    call_order = []

    async def a(items: str) -> str:
        call_order.append(("a", items))
        return items.upper()

    async def b(a: AsyncIterator[str]):
        async for x in a:
            call_order.append(("b", x))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
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


async def test_given_generator_and_eager_each_and_eager_iterator_consumers_when_run_then_lockstep_order():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int):
        call_order.append(("a", items))

    async def b(items: AsyncIterator[int]):
        async for x in items:
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

    await async_run(my_pipeline, params=P())
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [0, 1, 2]


async def test_given_generator_and_set_consumer_when_run_then_materialized_once():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int):
        call_order.append(("a", items))

    async def b(items: set[int]):
        call_order.append(("b", items))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 1
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [{0, 1, 2}]


async def test_given_two_generators_when_consumed_by_single_step_then_no_materialization():
    class P(NamedTuple):
        count: int = 3

    async def gen1(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    async def gen2(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i + 10

    call_order = []

    async def c(gen1: AsyncIterator[int], gen2: AsyncIterator[int]):
        async for x in gen1:
            call_order.append(("c1", x))
        async for y in gen2:
            call_order.append(("c2", y))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "c1"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "c2"] == [10, 11, 12]


async def test_given_chain_and_bypass_dependencies_when_run_then_no_materialization():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int) -> int:
        call_order.append(("a", items))
        return items * 2

    async def b(a: AsyncIterator[int], items: AsyncIterator[int]):
        async for x in a:
            call_order.append(("b_a", x))
        async for y in items:
            call_order.append(("b_items", y))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b_a"] == [0, 2, 4]
    assert [val for key, val in call_order if key == "b_items"] == [0, 1, 2]


async def test_given_generator_and_tuple_consumer_when_run_then_materialized_once():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def a(items: int):
        call_order.append(("a", items))

    async def b(items: tuple[int, ...]):
        call_order.append(("b", items))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 1
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b"] == [(0, 1, 2)]


async def test_given_scalar_producer_and_list_and_iterator_consumers_when_run_then_wrapped_as_single_element_collections():
    class P(NamedTuple):
        val: int = 42

    call_order = []

    async def s1(val: int) -> int:
        call_order.append(("s1", val))
        return val

    async def s2(s1: list[int]):
        call_order.append(("s2", s1))

    async def s3(s1: AsyncIterator[int]):
        call_order.append(("s3", [x async for x in s1]))

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", fn=s1),
            step("s2", fn=s2),
            step("s3", fn=s3),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert [val for key, val in call_order if key == "s1"] == [42]
    assert [val for key, val in call_order if key == "s2"] == [[42]]
    assert [val for key, val in call_order if key == "s3"] == [[42]]


async def test_given_collection_producer_and_scalar_transformer_and_iterator_consumer_when_run_then_lazy_stream_no_materialization():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for _i in range(count):
            yield _i

    call_order = []

    async def s2(items: int) -> int:
        call_order.append(("s2", items))
        return items + 10

    async def s3(s2: AsyncIterator[int]):
        async for val in s2:
            call_order.append(("s3", val))

    materialized = []

    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]

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

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "s2"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "s3"] == [10, 11, 12]


async def test_given_step_materializer_when_run_then_overrides_pipeline_factory():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def consumer(items: list[int]):
        pass

    pipeline_materialized = []

    async def pipeline_mat(g):
        pipeline_materialized.append("called")
        return [x async for x in g]

    step_materialized = []

    async def step_mat(g):
        step_materialized.append("called")
        return [x async for x in g]

    my_pipeline = pipeline(
        name="test_override",
        params=P,
        default_materializer_factory=pipeline_mat,
        steps=[
            step("items", fn=gen, materializer=step_mat),
            step("consumer", fn=consumer),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert len(step_materialized) == 1
    assert len(pipeline_materialized) == 0


async def test_given_factory_with_context_when_run_then_context_is_injected():
    from synaflow.core.types import MaterializeContext

    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def consumer(items: list[int]):
        pass

    captured_context = []

    def factory_with_ctx(ctx: MaterializeContext):
        captured_context.append(ctx)

        async def mat(g):
            return [x async for x in g]

        return mat

    my_pipeline = pipeline(
        name="test_context",
        params=P,
        default_materializer_factory=factory_with_ctx,
        steps=[
            step("items", fn=gen),
            step("consumer", fn=consumer),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert len(captured_context) >= 1
    assert captured_context[-1].pipeline_name == "test_context"
    assert any(c.dataset_name == "items" for c in captured_context)
