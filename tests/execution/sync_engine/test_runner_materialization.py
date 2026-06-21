import inspect
from typing import Generator, Iterator, NamedTuple
from unittest.mock import MagicMock


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

    def spy_materialize(ctx):
        def concrete(g):
            materialized.append("called")
            return list(g)

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

    def spy_materialize(ctx):
        def concrete(g):
            materialized.append("called")
            return list(g)

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

    def spy_materialize(ctx):
        def concrete(g):
            materialized.append("called")
            return list(g)

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


def test_given_two_generators_when_consumed_by_single_step_then_automatic_materialization(
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

    def spy_materialize(ctx):
        def concrete(g):
            materialized.append("called")
            return list(g)

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

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 2
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

    def spy_materialize(ctx):
        def concrete(g):
            materialized.append("called")
            return list(g)

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

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "a"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "b_a"] == [0, 2, 4]
    assert [val for key, val in call_order if key == "b_items"] == [0, 1, 2]


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

    def spy_materialize(ctx):
        def concrete(g):
            materialized.append("called")
            return list(g)

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

    run_pipeline(my_pipeline, params=P())
    assert len(materialized) == 0
    assert [val for key, val in call_order if key == "s2"] == [0, 1, 2]
    assert [val for key, val in call_order if key == "s3"] == [10, 11, 12]


def test_given_step_materializer_when_run_then_overrides_pipeline_factory(run_pipeline):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def consumer(items: list[int]):
        pass

    pipeline_materialized = []

    def pipeline_mat(g):
        pipeline_materialized.append("called")
        return list(g)

    step_materialized = []

    def step_mat(g):
        step_materialized.append("called")
        return list(g)

    my_pipeline = pipeline(
        name="test_override",
        params=P,
        materializer=pipeline_mat,
        steps=[
            step("items", fn=gen, materializer=step_mat),
            step("consumer", fn=consumer),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert len(step_materialized) == 1
    assert len(pipeline_materialized) == 0


def test_given_generator_and_iterator_and_list_consumers_when_run_then_iterator_consumer_receives_stream_and_list_consumer_receives_materialized_collection(
    run_pipeline,
):
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    observations = {}
    materialized = []

    def spy_materialize(ctx):
        def concrete(g):
            materialized.append("called")
            return list(g)

        return concrete

    def lazy(items: Iterator[int]):
        observations["lazy_is_list"] = isinstance(items, list)
        observations["lazy_values"] = list(items)

    def eager(items: list[int]):
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

    run_pipeline(my_pipeline, params=P())

    assert materialized == ["called"]
    assert observations["lazy_is_list"] is True
    assert observations["lazy_values"] == [0, 1, 2]
    assert observations["eager_is_list"] is True
    assert observations["eager_values"] == [0, 1, 2]


def test_given_scalar_output_with_on_error_stop_when_run_then_scalar_materializer_is_invoked(
    run_pipeline,
):
    class P(NamedTuple):
        x: int = 3

    materialized = []

    def scalar_materializer(value):
        materialized.append(value)
        return value

    def produce(x: int) -> int:
        return x * 2

    def consume(produce: int):
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

    run_pipeline(my_pipeline, params=P())

    assert materialized == [6]


def test_given_factory_with_context_when_run_then_context_is_injected(run_pipeline):
    from synaflow.core.types import MaterializeContext

    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def consumer(items: list[int]):
        pass

    captured_context = []

    def factory_with_ctx(ctx: MaterializeContext):
        captured_context.append(ctx)

        def mat(g):
            return list(g)

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

    run_pipeline(my_pipeline, params=P())
    assert len(captured_context) >= 1
    assert captured_context[-1].pipeline_name == "test_context"
    assert any(c.dataset_name == "items" for c in captured_context)
    assert any(c.consumer_type == list[int] for c in captured_context)


def test_given_mixed_fanout_when_materializer_factory_receives_context_then_consumer_type_is_materialized_consumer_type(
    run_pipeline,
):
    from synaflow.core.types import MaterializeContext

    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    captured_context = []

    def factory_with_ctx(ctx: MaterializeContext):
        captured_context.append(ctx)
        return list

    def lazy(items: Iterator[int]):
        return list(items)

    def eager(items: list[int]):
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

    run_pipeline(my_pipeline, params=P())

    runtime_contexts = [c for c in captured_context if c.dataset_name == "items"]
    assert runtime_contexts
    assert all(c.consumer_type == list[int] for c in runtime_contexts)


def test_given_two_unrolled_streams_with_different_lengths_when_run_then_missing_side_is_padded_with_none(
    run_pipeline,
):
    class P(NamedTuple):
        pass

    def left() -> Generator[int, None, None]:
        yield 1
        yield 2

    def right() -> Generator[int, None, None]:
        yield 10

    def pair(left: int, right: int) -> tuple[int | None, int | None]:
        return (left, right)

    seen = []

    def sink(pair: list[tuple[int | None, int | None]]):
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

    run_pipeline(my_pipeline, params=P())

    assert seen == [(1, 10), (2, None)]


def test_given_two_unrolled_streams_with_one_empty_when_run_then_non_empty_side_still_emits_with_none_padding(
    run_pipeline,
):
    class P(NamedTuple):
        pass

    def left() -> Generator[int, None, None]:
        if False:
            yield 1

    def right() -> Generator[int, None, None]:
        yield 10
        yield 20

    def pair(left: int, right: int) -> tuple[int | None, int | None]:
        return (left, right)

    seen = []

    def sink(pair: list[tuple[int | None, int | None]]):
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

    run_pipeline(my_pipeline, params=P())

    assert seen == [(None, 10), (None, 20)]


def test_given_step_non_builtin_type_and_iterator_consumer_when_run_then_executes_successfully(
    run_pipeline,
):
    from dataclasses import dataclass

    @dataclass
    class Row:
        id: int
        name: str

    class P(NamedTuple):
        pass

    def producer() -> Iterator[Row]:
        yield Row(id=1, name="alice")
        yield Row(id=2, name="bob")

    seen = []

    def consumer(producer: Iterator[Row]):
        seen.extend(list(producer))

    my_pipeline = pipeline(
        name="test_custom_type_iterator_no_mat",
        params=P,
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert seen == [Row(id=1, name="alice"), Row(id=2, name="bob")]


def test_given_no_custom_materializer_and_non_builtin_type_when_not_materialized_then_executes_successfully(
    run_pipeline,
):
    from dataclasses import dataclass

    @dataclass
    class Row:
        id: int
        name: str

    class P(NamedTuple):
        pass

    def producer() -> Iterator[Row]:
        yield Row(id=1, name="alice")
        yield Row(id=2, name="bob")

    seen = []

    # Consumed as a scalar (EACH mode) so needs_materialize is False
    def consumer(producer: Row):
        seen.append(producer)

    my_pipeline = pipeline(
        name="test_no_materializer_custom_type_not_materialized",
        params=P,
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )

    run_pipeline(my_pipeline, params=P())
    assert seen == [Row(id=1, name="alice"), Row(id=2, name="bob")]


def test_given_diamond_topology_with_multiple_lazy_streams_when_run_then_no_deadlock(
    run_pipeline,
):
    class P(NamedTuple):
        pass

    def source() -> Iterator[int]:
        for i in range(10):
            yield i

    def a(source: Iterator[int]) -> Iterator[int]:
        yield from source

    def b(source: Iterator[int]) -> Iterator[int]:
        yield from source

    audit_seen = []

    def audit(source: Iterator[int]) -> None:
        for x in source:
            audit_seen.append(x)

    call_order = []

    def finalize(a: Iterator[int], b: Iterator[int]) -> None:
        for x in a:
            call_order.append(("a", x))
        for y in b:
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

    # Materialization propagates upstream through lazy stream chains.
    assert my_pipeline.dag.steps["a"].materialized_deps == ["source"]
    assert my_pipeline.dag.steps["b"].materialized_deps == ["source"]
    assert my_pipeline.dag.steps["audit"].materialized_deps == ["source"]
    assert my_pipeline.dag.steps["finalize"].materialized_deps == ["a", "b"]

    run_pipeline(my_pipeline, params=P())
    assert len(call_order) == 20
    assert len(audit_seen) == 10
