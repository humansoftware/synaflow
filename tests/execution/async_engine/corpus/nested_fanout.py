from collections.abc import AsyncIterator
from typing import NamedTuple
from synaflow import pipeline, step


class Params(NamedTuple):
    count: int = 10


async def generator(count: int) -> AsyncIterator[int]:
    for i in range(count):
        yield i


async def stage1_a(generator: int) -> int:
    return generator


async def stage1_b(generator: int) -> int:
    return generator * 2


async def stage2_a(stage1_a: int) -> int:
    return stage1_a + 1


async def stage2_b(stage1_a: int) -> int:
    return stage1_a + 2


async def stage3(stage2_a: int, stage2_b: int) -> int:
    return stage2_a + stage2_b


nested_fanout_pipeline = pipeline(
    name="nested_fanout",
    params=Params,
    steps=[
        step("generator", fn=generator),
        step("stage1_a", fn=stage1_a),
        step("stage1_b", fn=stage1_b),
        step("stage2_a", fn=stage2_a),
        step("stage2_b", fn=stage2_b),
        step("stage3", fn=stage3),
    ],
)
