from collections.abc import Generator, Iterator
from typing import NamedTuple
from synaflow import pipeline, step


class ErrorHandlingParams(NamedTuple):
    pass


errors_list = []


def custom_error_handler(error_ctx) -> None:
    errors_list.append(str(error_ctx.exception))


def custom_err_mat(ctx):
    return custom_error_handler


def gen() -> Generator[int, None, None]:
    yield 1
    raise ValueError("gen failed")


def consumer(gen: Iterator[int]) -> None:
    for x in gen:
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
                "output": "Stream[int, None, None]",
                "fn": "gen",
                "on_error": "continue",
                "mode": "all",
                "materializer": "list",
                "error_materializer": "custom_error_handler",
                "each_mode_deps": [],
                "pipeline": "error_handling_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "consumer": {
                "deps": {"gen": "Stream[int]"},
                "output": "None",
                "fn": "consumer",
                "on_error": "continue",
                "mode": "all",
                "materializer": None,
                "error_materializer": "custom_error_handler",
                "each_mode_deps": [],
                "pipeline": "error_handling_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
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
