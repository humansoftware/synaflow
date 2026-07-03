from collections.abc import AsyncGenerator, AsyncIterator
from typing import NamedTuple

import asyncio
import pytest

from synaflow import OnError, async_run, pipeline, step
from synaflow.core.exceptions import PipelineStopException


class Empty(NamedTuple):
    pass


class Count(NamedTuple):
    count: int = 5


@pytest.mark.asyncio
async def test_given_max_in_flight_1_when_linear_then_preserves_lockstep():
    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    results: list[int] = []

    async def consumer(producer: int) -> None:
        results.append(producer)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=1),
            step("consumer", fn=consumer),
        ],
    )
    await async_run(p, Count(count=5))
    assert results == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_given_max_in_flight_30_when_linear_then_pipeline_completes():
    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    results: list[int] = []

    async def consumer(producer: AsyncIterator[int]) -> None:
        async for x in producer:
            results.append(x)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=30),
            step("consumer", fn=consumer),
        ],
    )
    await async_run(p, Count(count=5))
    assert results == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_given_max_in_flight_on_terminal_step_when_terminal_then_no_effect():
    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def terminal(producer: AsyncIterator[int]) -> None:
        async for x in producer:
            pass

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=30),
            step("terminal", fn=terminal),
        ],
    )
    await async_run(p, Count(count=5))


@pytest.mark.asyncio
async def test_given_max_in_flight_when_on_error_continue_then_still_works():
    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    results: list[int] = []

    async def fragile(producer: int) -> int:
        if producer == 2:
            raise ValueError("item 2 fails")
        return producer

    async def consumer(fragile: int) -> None:
        results.append(fragile)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("fragile", fn=fragile, on_error=OnError.CONTINUE),
            step("consumer", fn=consumer, on_error=OnError.CONTINUE),
        ],
    )
    await async_run(p, Count(count=5))
    assert results == [0, 1, 3, 4]


@pytest.mark.asyncio
async def test_given_max_in_flight_when_producer_does_not_exceed_bounded_ahead():
    produced: list[int] = []
    consumed: list[int] = []
    max_seen_ahead = 0

    async def producer(count: int) -> AsyncGenerator[int, None]:
        nonlocal max_seen_ahead
        for i in range(count):
            produced.append(i)
            max_seen_ahead = max(max_seen_ahead, len(produced) - len(consumed))
            yield i

    async def consumer(producer: int) -> None:
        consumed.append(producer)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer", fn=consumer),
        ],
    )
    await async_run(p, Count(count=20))
    assert produced == list(range(20))
    assert consumed == list(range(20))
    # The producer records "ahead" before yielding the current item, so the
    # observed gap can include the item being handed off plus the bounded queue.
    assert max_seen_ahead <= 4


@pytest.mark.asyncio
async def test_given_max_in_flight_3_when_linear_stream_then_producer_blocks_before_item_4():
    log: list[str] = []

    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            log.append(f"prod {i}")
            yield i

    async def consumer(producer: int) -> None:
        log.append(f"recv {producer}")
        await asyncio.sleep(0.01)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer", fn=consumer),
        ],
    )
    await async_run(p, Count(count=6))

    assert log.index("recv 0") < log.index("prod 4")


@pytest.mark.asyncio
async def test_given_max_in_flight_3_when_linear_stream_then_ahead_distance_stays_bounded():
    produced: list[int] = []
    consumed: list[int] = []
    max_seen_ahead = 0

    async def producer(count: int) -> AsyncGenerator[int, None]:
        nonlocal max_seen_ahead
        for i in range(count):
            produced.append(i)
            max_seen_ahead = max(max_seen_ahead, len(produced) - len(consumed))
            yield i

    async def consumer(producer: int) -> None:
        consumed.append(producer)
        await asyncio.sleep(0.01)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer", fn=consumer),
        ],
    )
    await async_run(p, Count(count=20))

    assert produced == list(range(20))
    assert consumed == list(range(20))
    assert max_seen_ahead <= 4


@pytest.mark.asyncio
async def test_given_max_in_flight_1_when_fanout_slow_branch_then_bound_is_exact():
    log: list[str] = []

    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            log.append(f"prod {i}")
            yield i

    async def fast(producer: int) -> None:
        log.append(f"fast {producer}")

    async def slow(producer: int) -> None:
        log.append(f"slow-recv {producer}")
        await asyncio.sleep(0.01)
        log.append(f"slow {producer}")

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=1),
            step("fast", fn=fast),
            step("slow", fn=slow),
        ],
    )
    await async_run(p, Count(count=5))

    slow_0_index = log.index("slow-recv 0")
    prod_2_index = log.index("prod 2")
    assert slow_0_index < prod_2_index


@pytest.mark.asyncio
async def test_given_max_in_flight_3_when_fanout_two_consumers_then_both_get_all_items():
    from collections.abc import AsyncGenerator, AsyncIterator

    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    results_a: list[int] = []
    results_b: list[int] = []

    async def consumer_a(producer: AsyncIterator[int]) -> None:
        async for x in producer:
            results_a.append(x)

    async def consumer_b(producer: AsyncIterator[int]) -> None:
        async for x in producer:
            results_b.append(x)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a),
            step("consumer_b", fn=consumer_b),
        ],
    )
    await async_run(p, Count(count=10))
    assert results_a == list(range(10))
    assert results_b == list(range(10))


@pytest.mark.asyncio
async def test_given_max_in_flight_3_when_fanout_lazy_and_eager_then_both_receive_items():
    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    lazy_results: list[int] = []
    eager_results: list[list[int]] = []

    async def lazy_consumer(producer: AsyncIterator[int]) -> None:
        async for item in producer:
            lazy_results.append(item)

    async def eager_consumer(producer: list[int]) -> None:
        eager_results.append(producer)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("lazy_consumer", fn=lazy_consumer),
            step("eager_consumer", fn=eager_consumer),
        ],
    )
    await async_run(p, Count(count=10))

    assert lazy_results == list(range(10))
    assert eager_results == [list(range(10))]


@pytest.mark.asyncio
async def test_given_max_in_flight_3_when_cross_level_bypass_then_pipeline_fails_validation():
    transformed: list[int] = []
    bypassed: list[int] = []

    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def first_consumer(producer: AsyncIterator[int]) -> int:
        total = 0
        async for item in producer:
            total += item
        return total

    async def second_consumer(
        first_consumer: int, producer: AsyncIterator[int]
    ) -> None:
        transformed.append(first_consumer)
        async for item in producer:
            bypassed.append(item)

    with pytest.raises(
        ValueError, match="Asymmetric lockstep materialization detected"
    ):
        pipeline(
            name="test",
            params=Count,
            steps=[
                step("producer", fn=producer, max_in_flight=3),
                step("first_consumer", fn=first_consumer),
                step("second_consumer", fn=second_consumer),
            ],
        )


@pytest.mark.asyncio
async def test_given_max_in_flight_3_when_terminal_lazy_consumer_then_stream_drains_fully():
    produced: list[int] = []
    consumed: list[int] = []

    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            produced.append(i)
            yield i

    async def terminal(producer: AsyncIterator[int]) -> None:
        async for item in producer:
            consumed.append(item)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("terminal", fn=terminal),
        ],
    )
    await async_run(p, Count(count=10))

    assert produced == list(range(10))
    assert consumed == list(range(10))


@pytest.mark.asyncio
async def test_given_max_in_flight_3_when_branch_stops_early_then_other_branch_finishes():
    early: list[int] = []
    full: list[int] = []

    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def early_consumer(producer: AsyncIterator[int]) -> None:
        async for item in producer:
            early.append(item)
            break

    async def full_consumer(producer: AsyncIterator[int]) -> None:
        async for item in producer:
            full.append(item)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("early_consumer", fn=early_consumer),
            step("full_consumer", fn=full_consumer),
        ],
    )
    await async_run(p, Count(count=5))

    assert early == [0]
    assert full == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_given_two_lazy_deps_with_max_in_flight_when_unrolled_then_pairs_are_preserved():
    pairs: list[tuple[int, int]] = []

    async def left(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def right(count: int) -> AsyncGenerator[int, None]:
        for i in range(10, 10 + count):
            yield i

    async def join(left: int, right: int) -> None:
        pairs.append((left, right))

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("left", fn=left, max_in_flight=3),
            step("right", fn=right, max_in_flight=3),
            step("join", fn=join),
        ],
    )
    await async_run(p, Count(count=5))

    assert pairs == [(0, 10), (1, 11), (2, 12), (3, 13), (4, 14)]


@pytest.mark.asyncio
async def test_given_flattening_stream_step_when_max_in_flight_2_then_internal_items_define_bound():
    produced: list[int] = []
    consumed: list[int] = []
    max_seen_ahead = 0

    async def source(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def flatten(source: AsyncIterator[int]) -> AsyncGenerator[int, None]:
        nonlocal max_seen_ahead
        async for item in source:
            produced.append(item)
            max_seen_ahead = max(max_seen_ahead, len(produced) - len(consumed))
            yield item
            produced.append(item + 100)
            max_seen_ahead = max(max_seen_ahead, len(produced) - len(consumed))
            yield item + 100

    async def consumer(flatten: AsyncIterator[int]) -> None:
        async for item in flatten:
            consumed.append(item)
            await asyncio.sleep(0.01)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("source", fn=source),
            step("flatten", fn=flatten, max_in_flight=2),
            step("consumer", fn=consumer),
        ],
    )
    await async_run(p, Count(count=3))

    assert consumed == [0, 100, 1, 101, 2, 102]
    assert max_seen_ahead <= 3


@pytest.mark.asyncio
async def test_given_fanout_lazy_and_eager_when_producer_stream_fails_then_pipeline_stops():
    lazy_seen: list[int] = []

    async def producer(count: int) -> AsyncGenerator[int, None]:
        yield 0
        yield 1
        raise ValueError("boom")

    async def lazy_consumer(producer: AsyncIterator[int]) -> None:
        async for item in producer:
            lazy_seen.append(item)

    async def eager_consumer(producer: list[int]) -> None:
        pass

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3, on_error=OnError.STOP),
            step("lazy_consumer", fn=lazy_consumer, on_error=OnError.STOP),
            step("eager_consumer", fn=eager_consumer, on_error=OnError.STOP),
        ],
    )

    with pytest.raises(PipelineStopException):
        await async_run(p, Count(count=5))

    assert lazy_seen == []


@pytest.mark.asyncio
async def test_runner_contract_uses_dag_node_max_in_flight_not_step_max_in_flight():
    produced: list[int] = []
    consumed: list[int] = []
    max_seen_ahead = 0

    async def producer(count: int) -> AsyncGenerator[int, None]:
        nonlocal max_seen_ahead
        for i in range(count):
            produced.append(i)
            max_seen_ahead = max(max_seen_ahead, len(produced) - len(consumed))
            yield i

    async def consumer(producer: int) -> None:
        consumed.append(producer)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=1),
            step("consumer", fn=consumer),
        ],
    )

    # Mutate Step.max_in_flight. The executor should ignore this and use DagNode.max_in_flight (1).
    p.steps[0].max_in_flight = 10

    await async_run(p, Count(count=20))

    assert produced == list(range(20))
    assert consumed == list(range(20))
    assert max_seen_ahead <= 3




@pytest.mark.asyncio
async def test_given_multilevel_each_fanout_when_run_max_in_flight_1_then_completes():
    class P(NamedTuple):
        pass

    async def source() -> AsyncIterator[int]:
        for i in range(20):
            yield i

    async def l1a(source: int) -> int:
        return source

    async def l1b(source: int) -> int:
        return source * 10

    async def l1c(source: int) -> int:
        return source * 100

    seen_x: list[int] = []
    seen_y: list[int] = []

    async def l2x(l1a: int) -> None:
        seen_x.append(l1a)

    async def l2y(l1a: int) -> None:
        seen_y.append(l1a)

    my_pipeline = pipeline(
        name="test_multilevel_each_fanout_mif_1_async",
        params=P,
        steps=[
            step("source", fn=source, max_in_flight=1),
            step("l1a", fn=l1a, max_in_flight=1),
            step("l1b", fn=l1b, max_in_flight=1),
            step("l1c", fn=l1c, max_in_flight=1),
            step("l2x", fn=l2x, max_in_flight=1),
            step("l2y", fn=l2y, max_in_flight=1),
        ],
    )

    await async_run(my_pipeline, params=P())

    assert seen_x == list(range(20))
    assert seen_y == list(range(20))


@pytest.mark.asyncio
async def test_given_multilevel_each_fanout_when_run_max_in_flight_3_then_completes():
    class P(NamedTuple):
        pass

    async def source() -> AsyncIterator[int]:
        for i in range(20):
            yield i

    async def l1a(source: int) -> int:
        return source

    async def l1b(source: int) -> int:
        return source * 10

    async def l1c(source: int) -> int:
        return source * 100

    seen_x: list[int] = []
    seen_y: list[int] = []

    async def l2x(l1a: int) -> None:
        seen_x.append(l1a)

    async def l2y(l1a: int) -> None:
        seen_y.append(l1a)

    my_pipeline = pipeline(
        name="test_multilevel_each_fanout_mif_3_async",
        params=P,
        steps=[
            step("source", fn=source, max_in_flight=3),
            step("l1a", fn=l1a, max_in_flight=3),
            step("l1b", fn=l1b, max_in_flight=3),
            step("l1c", fn=l1c, max_in_flight=3),
            step("l2x", fn=l2x, max_in_flight=3),
            step("l2y", fn=l2y, max_in_flight=3),
        ],
    )

    await async_run(my_pipeline, params=P())

    assert seen_x == list(range(20))
    assert seen_y == list(range(20))
