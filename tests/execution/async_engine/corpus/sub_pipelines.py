from typing import AsyncIterator, NamedTuple

from synaflow import include, pipeline, step


class BParams(NamedTuple):
    text: str


async def func_b1(text: str) -> str:
    return text.upper()


async def func_b2(func_b1: str) -> int:
    return len(func_b1)


pipe_b = pipeline(
    name="TextProcessor",
    params=BParams,
    exports="func_b2",
    steps=[step("func_b1", fn=func_b1), step("func_b2", fn=func_b2)],
)


class AParams(NamedTuple):
    raw_texts: list[str]


async def prepare_b_each(raw_texts: list[str]) -> AsyncIterator[BParams]:
    for t in raw_texts:
        yield BParams(text=t)


async def consolidate(my_text_processor: list[int]) -> int:
    return sum(my_text_processor)


pipe = pipeline(
    name="MainPipeline",
    params=AParams,
    steps=[
        include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
        step("consolidate", fn=consolidate),
    ],
)

from tests.pipeline_pack import PipelinePack

pack = PipelinePack(
    pipeline=pipe,
    input_params=AParams(raw_texts=["hi", "world", "synaflow"]),
    expected_results={
        "my_text_processor__adapter": [
            BParams(text="hi"),
            BParams(text="world"),
            BParams(text="synaflow"),
        ],
        "my_text_processor__func_b1": ["HI", "WORLD", "SYNAFLOW"],
        "my_text_processor": [2, 5, 8],
        "consolidate": 15,
    },
)
