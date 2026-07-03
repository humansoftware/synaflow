from typing import NamedTuple

from synaflow import pipeline, step


class DiamondParams(NamedTuple):
    base_val: int = 10


async def start(base_val: int) -> int:
    return base_val


async def branch_a(start: int) -> int:
    return start + 1


async def branch_b(start: int) -> int:
    return start + 2


async def merge(branch_a: int, branch_b: int) -> int:
    return branch_a + branch_b


from tests.common.pipeline_pack import PipelinePack  # noqa: E402

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
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "diamond_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "branch_a": {
                "deps": {"start": "int"},
                "output": "int",
                "fn": "branch_a",
                "on_error": "continue",
                "mode": "all",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "diamond_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "branch_b": {
                "deps": {"start": "int"},
                "output": "int",
                "fn": "branch_b",
                "on_error": "continue",
                "mode": "all",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "diamond_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "merge": {
                "deps": {"branch_a": "int", "branch_b": "int"},
                "output": "int",
                "fn": "merge",
                "on_error": "continue",
                "mode": "all",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "diamond_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
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
