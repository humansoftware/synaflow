from typing import NamedTuple

from synaflow import pipeline, step


class DiamondParams(NamedTuple):
    base_val: int = 10


def start(base_val: int) -> int:
    return base_val


def branch_a(start: int) -> int:
    return start + 1


def branch_b(start: int) -> int:
    return start + 2


def merge(branch_a: int, branch_b: int) -> int:
    return branch_a + branch_b


from tests.common.pipeline_pack import PipelinePack

diamond_pipeline = pipeline(
    name="diamond_example",
    params=DiamondParams,
    steps=[
        step("start", fn=start),
        step("branch_a", fn=branch_a),
        step("branch_b", fn=branch_b),
        step("merge", fn=merge),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "diamond_example",
        "params": {"base_val": "int"},
        "steps": {
            "start": {
                "deps": {"base_val": "int"},
                "output": "int",
                "fn": "start",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "diamond_example",
                "parent_pipeline": None,
            },
            "branch_a": {
                "deps": {"start": "int"},
                "output": "int",
                "fn": "branch_a",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "diamond_example",
                "parent_pipeline": None,
            },
            "branch_b": {
                "deps": {"start": "int"},
                "output": "int",
                "fn": "branch_b",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "diamond_example",
                "parent_pipeline": None,
            },
            "merge": {
                "deps": {"branch_a": "int", "branch_b": "int"},
                "output": "int",
                "fn": "merge",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [], "max_in_flight": 1,
                "pipeline": "diamond_example",
                "parent_pipeline": None,
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=diamond_pipeline,
    input_params=DiamondParams(base_val=10),
    step_results={
        "start": 10,
        "branch_a": 11,
        "branch_b": 12,
        "merge": 23,
    },
    expected_execution_levels=[
        ["start"],
        ["branch_a", "branch_b"],
        ["merge"],
    ],
)
