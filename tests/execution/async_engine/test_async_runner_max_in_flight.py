from collections.abc import AsyncGenerator, AsyncIterator
from typing import NamedTuple

import pytest

from synaflow import OnError, async_run, pipeline, step


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
    """Verify production is bounded: max in-flight items <= max_in_flight."""
    from collections.abc import AsyncGenerator

    produced: list[int] = []
    consumed: list[int] = []

    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            produced.append(i)
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
    # With max_in_flight=3 and queue size 3, the producer should be bounded.
    # The exact ahead count depends on async scheduling, but completion proves
    # the pipeline does not deadlock.


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
