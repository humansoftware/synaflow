from collections.abc import AsyncGenerator
from typing import NamedTuple

from synaflow import StepMode, pipeline, step
from tests.common.pipeline_pack import PipelinePack


class ExplicitModesParams(NamedTuple):
    items: list[int] = [1, 2, 3]


async def emit(items: list[int]) -> AsyncGenerator[int, None]:
    for item in items:
        yield item


async def double(emit: int) -> int:
    return emit * 2


async def summarize(double: list[int]) -> int:
    return sum(double)


explicit_modes_pipeline = pipeline(
    name="explicit_modes",
    params=ExplicitModesParams,
    steps=[
        step("emit", fn=emit),
        step("double", fn=double, mode=StepMode.EACH),
        step("summarize", fn=summarize, mode=StepMode.ALL),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "explicit_modes",
        "params": {"items": "list[int]"},
        "steps": {
            "emit": {
                "deps": {"items": "list[int]"},
                "output": "Stream[int, None]",
                "fn": "emit",
                "on_error": "continue",
                "mode": "all",
                "materializer": "_identity",
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "explicit_modes",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "double": {
                "deps": {"emit": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "double",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["emit"],
                "pipeline": "explicit_modes",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "summarize": {
                "deps": {"double": "list[int]"},
                "output": "int",
                "fn": "summarize",
                "on_error": "continue",
                "mode": "all",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "explicit_modes",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=explicit_modes_pipeline,
    input_params=ExplicitModesParams(items=[1, 2, 3]),
    step_results={
        "emit": [1, 2, 3],
        "double": [2, 4, 6],
        "summarize": 12,
    },
    expected_execution_levels=[["emit"], ["double"], ["summarize"]],
)
