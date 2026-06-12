from typing import NamedTuple
from collections.abc import AsyncGenerator, AsyncIterator

from synaflow import pipeline, step

class ComplexParallelParams(NamedTuple):
    base: int = 1

async def step1(base: int) -> AsyncGenerator[int, None, None]:
    for i in range(5):
        yield base + i

async def step2(step1: AsyncIterator[int]) -> AsyncGenerator[int, None, None]:
    async for x in step1:
        yield x * 10

async def step3(step2: AsyncIterator[int]) -> AsyncGenerator[int, None, None]:
    async for x in step2:
        yield x + 1

async def step4(step1: AsyncIterator[int]) -> AsyncGenerator[int, None, None]:
    async for x in step1:
        yield x * 100

async def step5(step3: AsyncIterator[int], step4: AsyncIterator[int]) -> None:
    pass

# Topology:
# step1 -> step2 -> step3 \
#       -> step4 --------> step5
pipeline_def = pipeline(
    name="complex_parallel",
    params=ComplexParallelParams,
    steps=[
        step("step1", fn=step1),
        step("step2", fn=step2),
        step("step3", fn=step3),
        step("step4", fn=step4),
        step("step5", fn=step5),
    ]
)
