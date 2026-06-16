from collections.abc import Generator, Iterator
from typing import NamedTuple

from synaflow import pipeline, step


class ComplexParallelParams(NamedTuple):
    base: int = 1


def step1(base: int) -> Generator[int, None, None]:
    for i in range(5):
        yield base + i


def step2(step1: Iterator[int]) -> Generator[int, None, None]:
    for x in step1:
        yield x * 10


def step3(step2: Iterator[int]) -> Generator[int, None, None]:
    for x in step2:
        yield x + 1


def step4(step1: Iterator[int]) -> Generator[int, None, None]:
    for x in step1:
        yield x * 100


def step5(step3: Iterator[int], step4: Iterator[int]) -> None:
    pass


# Topology:
# step1 -> step2 -> step3 \
#       -> step4 --------> step5
from tests.common.pipeline_pack import PipelinePack

pipeline_def = pipeline(
    name="complex_parallel",
    params=ComplexParallelParams,
    steps=[
        step("step1", fn=step1, max_in_flight=100),
        step("step2", fn=step2),
        step("step3", fn=step3),
        step("step4", fn=step4),
        step("step5", fn=step5),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "complex_parallel",
        "params": {"base": "int"},
        "steps": {
            "step1": {
                "deps": {"base": "int"},
                "output": "Stream[int, None, None]",
                "fn": "step1",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 100,
                "pipeline": "complex_parallel",
                "parent_pipeline": None,
            },
            "step2": {
                "deps": {"step1": "Stream[int]"},
                "output": "Stream[int, None, None]",
                "fn": "step2",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "complex_parallel",
                "parent_pipeline": None,
            },
            "step3": {
                "deps": {"step2": "Stream[int]"},
                "output": "Stream[int, None, None]",
                "fn": "step3",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "complex_parallel",
                "parent_pipeline": None,
            },
            "step4": {
                "deps": {"step1": "Stream[int]"},
                "output": "Stream[int, None, None]",
                "fn": "step4",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "complex_parallel",
                "parent_pipeline": None,
            },
            "step5": {
                "deps": {"step3": "Stream[int]", "step4": "Stream[int]"},
                "output": "None",
                "fn": "step5",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "complex_parallel",
                "parent_pipeline": None,
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=pipeline_def,
    input_params=ComplexParallelParams(base=1),
    step_results={
        "step1": [1, 2, 3, 4, 5],
        "step2": [10, 20, 30, 40, 50],
        "step3": [11, 21, 31, 41, 51],
        "step4": [100, 200, 300, 400, 500],
        "step5": None,
    },
)
