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


from tests.pipeline_pack import PipelinePack

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
    pipeline=diamond_pipeline,
    input_params=DiamondParams(base_val=10),
    expected_results={
        "start": 10,
        "branch_a": 11,
        "branch_b": 12,
        "merge": 23,
    },
)
