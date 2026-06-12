from collections.abc import AsyncGenerator, AsyncIterator
from typing import NamedTuple

from synaflow import pipeline, step


class LinearParams(NamedTuple):
    count: int = 3


async def gen(count: int) -> AsyncGenerator[int, None, None]:
    for _i in range(count):
        yield _i


async def transformer(gen: int) -> int:
    return gen * 2


async def consumer(transformer: AsyncIterator[int]) -> None:
    async for x in transformer:
        pass


from tests.pipeline_pack import PipelinePack

linear_pipeline = pipeline(
    name="linear_example",
    params=LinearParams,
    steps=[
        step("gen", fn=gen),
        step("transformer", fn=transformer),
        step("consumer", fn=consumer),
    ],
)

pack = PipelinePack(
    pipeline=linear_pipeline,
    input_params=LinearParams(count=3),
    expected_results={
        "gen": None,
        "transformer": None,
        "consumer": None,
    },
)
