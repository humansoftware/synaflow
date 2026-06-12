import asyncio
from typing import AsyncGenerator, AsyncIterator, NamedTuple

import pytest

from synaflow.async_executor import async_run
from synaflow.pipeline import pipeline
from synaflow.step import step


async def test_given_async_generator_and_each_consumer_when_run_then_processed_concurrently():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    call_order = []

    async def a(items: int) -> None:
        call_order.append(("a", items))

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert sorted(call_order) == [("a", 0), ("a", 1), ("a", 2)]


async def test_given_async_generator_and_two_async_iterator_consumers_when_run_then_both_receive_items():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    call_order = []

    async def a(items: AsyncIterator[int]) -> None:
        async for x in items:
            call_order.append(("a", x))

    async def b(items: AsyncIterator[int]) -> None:
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
    
    assert [v for k, v in call_order if k == "a"] == [0, 1, 2]
    assert [v for k, v in call_order if k == "b"] == [0, 1, 2]


async def test_given_async_generator_and_list_consumer_when_run_then_materialized():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    call_order = []

    async def a(items: list[int]) -> None:
        call_order.append(("a", items))

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("items", fn=gen),
            step("a", fn=a),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert call_order == [("a", [0, 1, 2])]
