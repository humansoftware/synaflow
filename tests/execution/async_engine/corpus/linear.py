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


from tests.common.pipeline_pack import PipelinePack

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
    json_dag={
        "name": "linear_example",
        "params": {"count": "int"},
        "steps": {
            "gen": {
                "deps": {"count": "int"},
                "output": "Stream[int, None, None]",
                "fn": "gen",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
            },
            "transformer": {
                "deps": {"gen": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "transformer",
                "on_error": "continue",
                "mode": "each",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": ["gen"],
                "pipeline": "linear_example",
                "parent_pipeline": None,
            },
            "consumer": {
                "deps": {"transformer": "Stream[int]"},
                "output": "None",
                "fn": "consumer",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
            },
        },
        "error_materializer_factory": "log_error_materializer",
    },
    pipeline=linear_pipeline,
    input_params=LinearParams(count=3),
    step_results={
        "gen": [0, 1, 2],
        "transformer": [0, 2, 4],
        "consumer": None,
    },
    expected_call_order=["gen", "transformer", "consumer"],
    expected_execution_levels=[
        ["gen"],
        ["transformer"],
        ["consumer"],
    ],
)
