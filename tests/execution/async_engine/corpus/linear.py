from collections.abc import AsyncGenerator, AsyncIterator
from typing import NamedTuple

from synaflow import pipeline, step


class LinearParams(NamedTuple):
    count: int = 3


async def numbers(count: int) -> AsyncGenerator[int, None, None]:
    for _i in range(count):
        yield _i


async def transformer(number: int) -> int:
    return number * 2


async def consumer(transformer: AsyncIterator[int]) -> None:
    async for x in transformer:
        pass


from tests.common.pipeline_pack import PipelinePack

linear_pipeline = pipeline(
    name="linear_example",
    params=LinearParams,
    steps=[
        step("numbers", fn=numbers),
        step("transformer", fn=transformer),
        step("consumer", fn=consumer),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "linear_example",
        "params": {"count": "int"},
        "steps": {
            "numbers": {
                "deps": {"count": "int"},
                "output": "Stream[int, None, None]",
                "fn": "numbers",
                "on_error": "continue",
                "mode": "all",
                "materializer": "_identity",
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "transformer": {
                "deps": {"numbers": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "transformer",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["numbers"],
                "pipeline": "linear_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
                "dataset_param_names": {"numbers": "number"},
            },
            "consumer": {
                "deps": {"transformer": "Stream[int]"},
                "output": "None",
                "fn": "consumer",
                "on_error": "continue",
                "mode": "all",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=linear_pipeline,
    input_params=LinearParams(count=3),
    step_results={
        "numbers": [0, 1, 2],
        "transformer": [0, 2, 4],
        "consumer": None,
    },
    expected_call_order=["numbers", "transformer", "consumer"],
    expected_execution_levels=[
        ["numbers"],
        ["transformer"],
        ["consumer"],
    ],
)
