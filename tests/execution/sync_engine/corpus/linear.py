from collections.abc import Generator, Iterator
from typing import NamedTuple

from synaflow import pipeline, step, PipelineEvent, StepEvent, Observer


class LinearParams(NamedTuple):
    count: int = 3


def gen(count: int) -> Generator[int, None, None]:
    yield from range(count)


def transformer(gen: int) -> int:
    return gen * 2


def consumer(transformer: Iterator[int]) -> None:
    for x in transformer:
        pass


def on_pipeline_started(ctx):
    pass


def on_step_failed(ctx):
    pass


def on_step_completed(ctx):
    pass


from tests.common.pipeline_pack import PipelinePack

linear_pipeline = pipeline(
    name="linear_example",
    params=LinearParams,
    observers=[
        Observer(PipelineEvent.STARTED, on_pipeline_started),
        Observer(StepEvent.FAILED, on_step_failed),
    ],
    steps=[
        step(
            "gen",
            fn=gen,
            observers=[Observer(StepEvent.COMPLETED, on_step_completed)],
        ),
        step("transformer", fn=transformer),
        step("consumer", fn=consumer),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "linear_example",
        "params": {"count": "int"},
        "steps": {
            "gen": {
                "deps": {"count": "int"},
                "output": "Stream[int, None, None]",
                "fn": "gen",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
                "observers": [
                    {"event": "step_failed", "source": "pipeline"},
                    {"event": "step_completed", "source": "step"},
                ],
            },
            "transformer": {
                "deps": {"gen": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "transformer",
                "on_error": "continue",
                "mode": "each",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": ["gen"],
                "pipeline": "linear_example",
                "parent_pipeline": None,
                "observers": [{"event": "step_failed", "source": "pipeline"}],
            },
            "consumer": {
                "deps": {"transformer": "Stream[int]"},
                "output": "None",
                "fn": "consumer",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
                "observers": [{"event": "step_failed", "source": "pipeline"}],
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=linear_pipeline,
    input_params=LinearParams(count=3),
    step_results={
        "gen": [0, 1, 2],
        "transformer": [0, 2, 4],
        "consumer": None,
    },
    expected_call_order=["gen", "transformer", "consumer"],
    expected_execution_levels=[
        ["gen"],
        ["transformer"],
        ["consumer"],
    ],
)
