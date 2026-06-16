from collections.abc import AsyncGenerator, AsyncIterator
from typing import NamedTuple
from synaflow import pipeline, step


class ErrorHandlingParams(NamedTuple):
    pass


errors_list = []


def custom_error_handler(exc: BaseException) -> None:
    errors_list.append(str(exc))


def custom_err_mat(ctx):
    return custom_error_handler


async def gen() -> AsyncGenerator[int, None]:
    yield 1
    raise ValueError("gen failed")


async def consumer(gen: AsyncIterator[int]) -> None:
    async for x in gen:
        pass


from tests.common.pipeline_pack import PipelinePack

error_pipeline = pipeline(
    name="error_handling_example",
    params=ErrorHandlingParams,
    error_materializer=custom_err_mat,
    steps=[
        step("gen", fn=gen),
        step("consumer", fn=consumer),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "error_handling_example",
        "params": {},
        "steps": {
            "gen": {
                "deps": {},
                "output": "Stream[int, None]",
                "fn": "gen",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "custom_err_mat",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "error_handling_example",
                "parent_pipeline": None,
            },
            "consumer": {
                "deps": {"gen": "Stream[int]"},
                "output": "None",
                "fn": "consumer",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "custom_err_mat",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "error_handling_example",
                "parent_pipeline": None,
            },
        },
        "error_materializer": "custom_err_mat",
    },
    pipeline=error_pipeline,
    input_params=ErrorHandlingParams(),
    step_results={
        "gen": [1],
        "consumer": None,
    },
    expected_call_order=["gen", "consumer"],
    expected_execution_levels=[
        ["gen"],
        ["consumer"],
    ],
)
