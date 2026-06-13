from typing import Iterator, NamedTuple

from synaflow import include, pipeline, step
from tests.pipeline_pack import PipelinePack


# Level 3 (Deepest)
class Level3Params(NamedTuple):
    x: int


def l3_process(x: int) -> int:
    return x * 2


pipe_l3 = pipeline(
    name="Level3",
    params=Level3Params,
    exports="l3_process",
    steps=[step("l3_process", fn=l3_process)],
)


# Level 2 (Middle)
class Level2Params(NamedTuple):
    y: int


def prep_l3(y: int) -> Level3Params:
    return Level3Params(x=y + 1)


def l2_process(l3_res: int) -> int:
    return l3_res + 10


pipe_l2 = pipeline(
    name="Level2",
    params=Level2Params,
    exports="l2_process",
    steps=[
        include("l3_res", pipeline=pipe_l3, fn=prep_l3),
        step("l2_process", fn=l2_process),
    ],
)


# Level 1 (Top)
class Level1Params(NamedTuple):
    values: list[int]


def prep_l2_each(values: list[int]) -> Iterator[Level2Params]:
    for v in values:
        yield Level2Params(y=v)


def prep_l2_single(values: list[int]) -> Level2Params:
    # Just take the first element for the single sub-pipeline example
    return Level2Params(y=values[0])


def consolidate(l2_each: list[int], l2_single: int) -> dict:
    return {"each_res": sum(l2_each), "single_res": l2_single}


pipe_l1 = pipeline(
    name="DeepSubPipelines",
    params=Level1Params,
    steps=[
        include("l2_each", pipeline=pipe_l2, fn=prep_l2_each),
        include("l2_single", pipeline=pipe_l2, fn=prep_l2_single),
        step("consolidate", fn=consolidate),
    ],
)

pack = PipelinePack(
    json_dag={
        "l2_each__adapter": {
            "deps": {"values": "list[int]"},
            "output": "Stream[Level2Params]",
            "fn": "prep_l2_each",
            "on_error": "stop",
            "needs_materialize": True,
            "materializer": "default_materializer_factory",
            "materialized_deps": ["values"],
            "pipeline": "DeepSubPipelines",
            "parent_pipeline": None,
        },
        "l2_each__l3_res__adapter": {
            "deps": {"l2_each__adapter": "Level2Params"},
            "output": "ListType(<class 'tests.execution.sync_engine.corpus.deep_sub_pipelines.Level3Params'>)",
            "fn": "prep_l3",
            "on_error": "stop",
            "needs_materialize": True,
            "materializer": "default_materializer_factory",
            "materialized_deps": ["l2_each__adapter"],
            "pipeline": "Level2",
            "parent_pipeline": "DeepSubPipelines",
        },
        "l2_each__l3_res": {
            "deps": {"l2_each__l3_res__adapter": "Level3Params"},
            "output": "ListType(<class 'int'>)",
            "fn": "l3_process",
            "on_error": "continue",
            "needs_materialize": False,
            "materializer": "default_materializer_factory",
            "materialized_deps": ["l2_each__l3_res__adapter"],
            "pipeline": "Level2",
            "parent_pipeline": "DeepSubPipelines",
        },
        "l2_each": {
            "deps": {"l2_each__l3_res": "int"},
            "output": "ListType(<class 'int'>)",
            "fn": "l2_process",
            "on_error": "continue",
            "needs_materialize": True,
            "materializer": "default_materializer_factory",
            "materialized_deps": [],
            "pipeline": "Level2",
            "parent_pipeline": "DeepSubPipelines",
        },
        "l2_single__adapter": {
            "deps": {"values": "list[int]"},
            "output": "Level2Params",
            "fn": "prep_l2_single",
            "on_error": "stop",
            "needs_materialize": True,
            "materializer": None,
            "materialized_deps": ["values"],
            "pipeline": "DeepSubPipelines",
            "parent_pipeline": None,
        },
        "l2_single__l3_res__adapter": {
            "deps": {"l2_single__adapter": "Level2Params"},
            "output": "Level3Params",
            "fn": "prep_l3",
            "on_error": "stop",
            "needs_materialize": True,
            "materializer": None,
            "materialized_deps": ["l2_single__adapter"],
            "pipeline": "Level2",
            "parent_pipeline": "DeepSubPipelines",
        },
        "l2_single__l3_res": {
            "deps": {"l2_single__l3_res__adapter": "Level3Params"},
            "output": "int",
            "fn": "l3_process",
            "on_error": "continue",
            "needs_materialize": False,
            "materializer": None,
            "materialized_deps": ["l2_single__l3_res__adapter"],
            "pipeline": "Level2",
            "parent_pipeline": "DeepSubPipelines",
        },
        "l2_single": {
            "deps": {"l2_single__l3_res": "int"},
            "output": "int",
            "fn": "l2_process",
            "on_error": "continue",
            "needs_materialize": False,
            "materializer": None,
            "materialized_deps": [],
            "pipeline": "Level2",
            "parent_pipeline": "DeepSubPipelines",
        },
        "consolidate": {
            "deps": {"l2_each": "list[int]", "l2_single": "int"},
            "output": "dict",
            "fn": "consolidate",
            "on_error": "continue",
            "needs_materialize": False,
            "materializer": None,
            "materialized_deps": ["l2_each"],
            "pipeline": None,
            "parent_pipeline": None,
        },
        "values": {
            "deps": {},
            "output": "list[int]",
            "fn": None,
            "on_error": None,
            "needs_materialize": True,
            "materializer": None,
            "materialized_deps": [],
            "pipeline": None,
            "parent_pipeline": None,
        },
    },
    pipeline=pipe_l1,
    input_params=Level1Params(values=[10, 20]),
    step_results={
        "l2_each__adapter": [Level2Params(y=10), Level2Params(y=20)],
        "l2_single__adapter": Level2Params(y=10),
        "l2_each__l3_res__adapter": [Level3Params(x=11), Level3Params(x=21)],
        "l2_single__l3_res__adapter": Level3Params(x=11),
        "l2_each__l3_res": [22, 42],
        "l2_single__l3_res": 22,
        "l2_each": [32, 52],
        "l2_single": 32,
        "consolidate": {"each_res": 84, "single_res": 32},
    },
)
