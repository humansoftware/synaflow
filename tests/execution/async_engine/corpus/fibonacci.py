from collections.abc import AsyncGenerator, AsyncIterator
from typing import NamedTuple

from synaflow import pipeline, step


class FibonacciParams(NamedTuple):
    count: int = 10


async def fibonacci_generator(count: int) -> AsyncGenerator[int, None, None]:
    a, b = 0, 1
    for _ in range(count):
        yield a
        a, b = b, a + b


async def square_numbers(
    fibonacci_generator: AsyncIterator[int],
) -> AsyncGenerator[int, None, None]:
    async for x in fibonacci_generator:
        yield x * x


async def consumer(square_numbers: AsyncIterator[int]) -> None:
    pass


from tests.pipeline_pack import PipelinePack

pipeline_def = pipeline(
    name="fibonacci",
    params=FibonacciParams,
    steps=[
        step("fibonacci_generator", fn=fibonacci_generator),
        step("square_numbers", fn=square_numbers),
        step("consumer", fn=consumer),
    ],
)

pack = PipelinePack(
    pipeline=pipeline_def,
    input_params=FibonacciParams(count=10),
    expected_results={
        "fibonacci_generator": None,
        "square_numbers": None,
        "consumer": None,
    },
)
