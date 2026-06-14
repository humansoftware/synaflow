from collections.abc import AsyncGenerator, AsyncIterator
from typing import NamedTuple

from synaflow import pipeline, step
from tests.common.pipeline_pack import PipelinePack


class MixedFanoutParams(NamedTuple):
    count: int = 3


async def gen(count: int) -> AsyncGenerator[int, None]:
    for i in range(count):
        yield i


async def lazy(gen: AsyncIterator[int]) -> tuple[bool, list[int]]:
    values = []
    async for value in gen:
        values.append(value)
    return (not isinstance(gen, list), values)


async def eager(gen: list[int]) -> tuple[bool, list[int]]:
    return (isinstance(gen, list), gen)


mixed_fanout_pipeline = pipeline(
    name="mixed_fanout",
    params=MixedFanoutParams,
    steps=[
        step("gen", fn=gen),
        step("lazy", fn=lazy),
        step("eager", fn=eager),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "mixed_fanout",
        "params": {"count": "int"},
        "steps": {
            "gen": {
                "deps": {"count": "int"},
                "output": "Stream[int, None]",
                "fn": "gen",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [],
                "pipeline": "mixed_fanout",
                "parent_pipeline": None,
                "observers": [],
            },
            "lazy": {
                "deps": {"gen": "Stream[int]"},
                "output": "tuple[bool, list[int]]",
                "fn": "lazy",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [],
                "pipeline": "mixed_fanout",
                "parent_pipeline": None,
                "observers": [],
            },
            "eager": {
                "deps": {"gen": "list[int]"},
                "output": "tuple[bool, list[int]]",
                "fn": "eager",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": ["gen"],
                "each_mode_deps": [],
                "pipeline": "mixed_fanout",
                "parent_pipeline": None,
                "observers": [],
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=mixed_fanout_pipeline,
    input_params=MixedFanoutParams(count=3),
    step_results={
        "gen": [0, 1, 2],
        "lazy": (True, [0, 1, 2]),
        "eager": (True, [0, 1, 2]),
    },
    expected_execution_levels=[["gen"], ["lazy", "eager"]],
)
