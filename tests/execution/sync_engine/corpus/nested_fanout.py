from collections.abc import Iterator
from typing import NamedTuple

from synaflow import pipeline, step
from tests.common.pipeline_pack import PipelinePack


class Params(NamedTuple):
    count: int = 10


def generator(count: int) -> Iterator[int]:
    for i in range(count):
        yield i


def stage1_a(generator: int) -> int:
    return generator


def stage1_b(generator: int) -> int:
    return generator * 2


def stage2_a(stage1_a: int) -> int:
    return stage1_a + 1


def stage2_b(stage1_a: int) -> int:
    return stage1_a + 2


def stage3(stage2_a: int, stage2_b: int) -> int:
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

pack = PipelinePack(
    json_dag={
        "name": "nested_fanout",
        "params": {"count": "int"},
        "steps": {
            "generator": {
                "deps": {"count": "int"},
                "output": "Stream[int]",
                "fn": "generator",
                "on_error": "continue",
                "mode": "all",
                "materializer": "_identity",
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "nested_fanout",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "stage1_a": {
                "deps": {"generator": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "stage1_a",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["generator"],
                "pipeline": "nested_fanout",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "stage1_b": {
                "deps": {"generator": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "stage1_b",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["generator"],
                "pipeline": "nested_fanout",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "stage2_a": {
                "deps": {"stage1_a": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "stage2_a",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["stage1_a"],
                "pipeline": "nested_fanout",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "stage2_b": {
                "deps": {"stage1_a": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "stage2_b",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["stage1_a"],
                "pipeline": "nested_fanout",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "stage3": {
                "deps": {"stage2_a": "int", "stage2_b": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "stage3",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["stage2_a", "stage2_b"],
                "pipeline": "nested_fanout",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=nested_fanout_pipeline,
    input_params=Params(count=10),
    step_results={
        "generator": list(range(10)),
        "stage1_a": list(range(10)),
        "stage1_b": [i * 2 for i in range(10)],
        "stage2_a": [i + 1 for i in range(10)],
        "stage2_b": [i * 2 + 2 for i in range(10)],
        # stage3 zips stage2_a/stage2_b per item: (i + 1) + (2i + 2)
        "stage3": [3 * (i + 1) for i in range(10)],
    },
    expected_execution_levels=[
        ["generator"],
        ["stage1_a", "stage1_b"],
        ["stage2_a", "stage2_b"],
        ["stage3"],
    ],
)
