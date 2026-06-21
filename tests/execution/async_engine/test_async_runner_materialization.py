import inspect
from typing import AsyncGenerator, AsyncIterator, NamedTuple
from unittest.mock import AsyncMock as MagicMock


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

    def spy_materialize(ctx):
        async def concrete(g):
            materialized.append("called")
            return [x async for x in g]

        return concrete

    my_pipeline = pipeline(
        name="test",
        params=P,
        materializer=spy_materialize,
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

    def spy_materialize(ctx):
        async def concrete(g):
            materialized.append("called")
            return [x async for x in g]

        return concrete

    my_pipeline = pipeline(
        name="test",
        params=P,
        materializer=spy_materialize,
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

    def spy_materialize(ctx):
        async def concrete(g):
            materialized.append("called")
            return [x async for x in g]

        return concrete

    my_pipeline = pipeline(
        name="test",
        params=P,
        materializer=spy_materialize,
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


async def test_given_two_generators_when_consumed_by_single_step_then_automatic_materialization():
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

    def spy_materialize(ctx):
        async def concrete(g):
            materialized.append("called")
            return [x async for x in g]

        return concrete

    my_pipeline = pipeline(
        name="test",
        params=P,
        materializer=spy_materialize,
        steps=[
            step("gen1", fn=gen1),
            step("gen2", fn=gen2),
            step("c", fn=c),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert len(materialized) == 2
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

    def spy_materialize(ctx):
        async def concrete(g):
            materialized.append("called")
            return [x async for x in g]

        return concrete

    my_pipeline = pipeline(
        name="test",
        params=P,
        materializer=spy_materialize,
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

    def spy_materialize(ctx):
        async def concrete(g):
            materialized.append("called")
            return [x async for x in g]

        return concrete

    my_pipeline = pipeline(
        name="test",
        params=P,
        materializer=spy_materialize,
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

    def pipeline_mat(ctx):
        async def concrete(g):
            pipeline_materialized.append("called")
            return [x async for x in g]

        return concrete

    step_materialized = []

    def step_mat(ctx):
        async def concrete(g):
            step_materialized.append("called")
            return [x async for x in g]

        return concrete

    my_pipeline = pipeline(
        name="test_override",
        params=P,
        materializer=pipeline_mat,
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
        materializer=factory_with_ctx,
        steps=[
            step("items", fn=gen),
            step("consumer", fn=consumer),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert len(captured_context) >= 1
    assert captured_context[-1].pipeline_name == "test_context"
    assert any(c.dataset_name == "items" for c in captured_context)
    assert any(c.consumer_type == list[int] for c in captured_context)


async def test_given_mixed_fanout_when_materializer_factory_receives_context_then_consumer_type_is_materialized_consumer_type():
    from synaflow.core.types import MaterializeContext

    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    captured_context = []

    def factory_with_ctx(ctx: MaterializeContext):
        captured_context.append(ctx)

        async def mat(g):
            return [x async for x in g]

        return mat

    async def lazy(items: AsyncIterator[int]):
        return [x async for x in items]

    async def eager(items: list[int]):
        return items

    my_pipeline = pipeline(
        name="test_mixed_context",
        params=P,
        materializer=factory_with_ctx,
        steps=[
            step("items", fn=gen),
            step("lazy", fn=lazy),
            step("eager", fn=eager),
        ],
    )

    await async_run(my_pipeline, params=P())

    runtime_contexts = [c for c in captured_context if c.dataset_name == "items"]
    assert runtime_contexts
    assert all(c.consumer_type == list[int] for c in runtime_contexts)


async def test_given_two_unrolled_streams_with_different_lengths_when_run_then_missing_side_is_padded_with_none():
    class P(NamedTuple):
        pass

    async def left() -> AsyncGenerator[int, None]:
        yield 1
        yield 2

    async def right() -> AsyncGenerator[int, None]:
        yield 10

    async def pair(left: int, right: int) -> tuple[int | None, int | None]:
        return (left, right)

    seen = []

    async def sink(pair: list[tuple[int | None, int | None]]):
        seen.extend(pair)

    my_pipeline = pipeline(
        name="test_unroll_padding",
        params=P,
        steps=[
            step("left", fn=left),
            step("right", fn=right),
            step("pair", fn=pair),
            step("sink", fn=sink),
        ],
    )

    await async_run(my_pipeline, params=P())

    assert seen == [(1, 10), (2, None)]


async def test_given_two_unrolled_streams_with_one_empty_when_run_then_non_empty_side_still_emits_with_none_padding():
    class P(NamedTuple):
        pass

    async def left() -> AsyncGenerator[int, None]:
        if False:
            yield 1

    async def right() -> AsyncGenerator[int, None]:
        yield 10
        yield 20

    async def pair(left: int, right: int) -> tuple[int | None, int | None]:
        return (left, right)

    seen = []

    async def sink(pair: list[tuple[int | None, int | None]]):
        seen.extend(pair)

    my_pipeline = pipeline(
        name="test_unroll_empty",
        params=P,
        steps=[
            step("left", fn=left),
            step("right", fn=right),
            step("pair", fn=pair),
            step("sink", fn=sink),
        ],
    )

    await async_run(my_pipeline, params=P())

    assert seen == [(None, 10), (None, 20)]


async def test_given_generator_and_iterator_and_list_consumers_when_run_then_iterator_consumer_receives_stream_and_list_consumer_receives_materialized_collection():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    observations = {}
    materialized = []

    def spy_materialize(ctx):
        async def concrete(g):
            materialized.append("called")
            return [x async for x in g]

        return concrete

    async def lazy(items: AsyncIterator[int]):
        observations["lazy_is_list"] = isinstance(items, list)
        values = []
        async for value in items:
            values.append(value)
        observations["lazy_values"] = values

    async def eager(items: list[int]):
        observations["eager_is_list"] = isinstance(items, list)
        observations["eager_values"] = items

    my_pipeline = pipeline(
        name="test_mixed_fanout",
        params=P,
        materializer=spy_materialize,
        steps=[
            step("items", fn=gen),
            step("lazy", fn=lazy),
            step("eager", fn=eager),
        ],
    )

    await async_run(my_pipeline, params=P())

    assert materialized == ["called"]
    assert observations["lazy_is_list"] is False
    assert observations["lazy_values"] == [0, 1, 2]
    assert observations["eager_is_list"] is True
    assert observations["eager_values"] == [0, 1, 2]


async def test_given_scalar_output_with_on_error_stop_when_run_then_scalar_materializer_is_invoked():
    class P(NamedTuple):
        x: int = 3

    materialized = []

    async def scalar_materializer(value):
        materialized.append(value)
        return value

    async def produce(x: int) -> int:
        return x * 2

    async def consume(produce: int):
        pass

    my_pipeline = pipeline(
        name="test_scalar_stop",
        params=P,
        steps=[
            step(
                "produce",
                fn=produce,
                on_error=OnError.STOP,
                materializer=scalar_materializer,
            ),
            step("consume", fn=consume),
        ],
    )

    await async_run(my_pipeline, params=P())

    assert materialized == [6]


async def test_given_step_non_builtin_type_and_iterator_consumer_when_run_then_executes_successfully():
    from dataclasses import dataclass
    from collections.abc import AsyncGenerator, AsyncIterator
    from synaflow import async_run

    @dataclass
    class Row:
        id: int
        name: str

    class P(NamedTuple):
        pass

    async def producer() -> AsyncGenerator[Row, None]:
        yield Row(id=1, name="alice")
        yield Row(id=2, name="bob")

    seen = []

    async def consumer(producer: AsyncIterator[Row]):
        async for item in producer:
            seen.append(item)

    my_pipeline = pipeline(
        name="test_custom_type_iterator_no_mat",
        params=P,
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert seen == [Row(id=1, name="alice"), Row(id=2, name="bob")]


async def test_given_no_custom_materializer_and_non_builtin_type_when_not_materialized_then_executes_successfully():
    from dataclasses import dataclass
    from collections.abc import AsyncGenerator
    from synaflow import async_run

    @dataclass
    class Row:
        id: int
        name: str

    class P(NamedTuple):
        pass

    async def producer() -> AsyncGenerator[Row, None]:
        yield Row(id=1, name="alice")
        yield Row(id=2, name="bob")

    seen = []

    # Consumed as a scalar (EACH mode) so needs_materialize is False
    async def consumer(producer: Row):
        seen.append(producer)

    my_pipeline = pipeline(
        name="test_no_materializer_custom_type_not_materialized",
        params=P,
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert seen == [Row(id=1, name="alice"), Row(id=2, name="bob")]


async def test_given_diamond_topology_with_multiple_lazy_streams_when_run_then_no_deadlock():
    class P(NamedTuple):
        pass

    async def source() -> AsyncIterator[int]:
        for i in range(10):
            yield i

    async def a(source: AsyncIterator[int]) -> AsyncIterator[int]:
        async for x in source:
            yield x

    async def b(source: AsyncIterator[int]) -> AsyncIterator[int]:
        async for x in source:
            yield x

    audit_seen = []

    async def audit(source: AsyncIterator[int]) -> None:
        async for x in source:
            audit_seen.append(x)

    call_order = []

    async def finalize(a: AsyncIterator[int], b: AsyncIterator[int]) -> None:
        async for x in a:
            call_order.append(("a", x))
        async for y in b:
            call_order.append(("b", y))

    my_pipeline = pipeline(
        name="test_diamond_deadlock",
        params=P,
        steps=[
            step("source", fn=source),
            step("a", fn=a),
            step("b", fn=b),
            step("audit", fn=audit),
            step("finalize", fn=finalize),
        ],
    )

    # Prove compile-time precision: source is materialized for a and b, but NOT for audit (remains lazy)
    assert "source" in my_pipeline.dag.steps["a"].materialized_deps
    assert "source" in my_pipeline.dag.steps["b"].materialized_deps
    assert "source" not in my_pipeline.dag.steps["audit"].materialized_deps
    assert my_pipeline.dag.steps["finalize"].materialized_deps == ["a", "b"]

    await async_run(my_pipeline, params=P())
    assert len(call_order) == 20
    assert len(audit_seen) == 10
