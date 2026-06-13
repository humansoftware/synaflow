from collections.abc import Generator, Iterator
from typing import NamedTuple

from synaflow import pipeline, step


class LinearParams(NamedTuple):
    count: int = 3


def gen(count: int) -> Generator[int, None, None]:
    yield from range(count)


def transformer(gen: int) -> int:
    return gen * 2


def consumer(transformer: Iterator[int]) -> None:
    for x in transformer:
        pass


from tests.pipeline_pack import PipelinePack

linear_pipeline = pipeline(
    name="linear_example",
    params=LinearParams,
    steps=[
        step("gen", fn=gen),
        step("transformer", fn=transformer),
        step("consumer", fn=consumer),
    ],
)

pack = PipelinePack(
    json_dag={
        "params": {"count": "int"},
        "steps": {
            "gen": {
                "deps": {"count": "int"},
                "output": "Stream[int, None, None]",
                "fn": "gen",
                "on_error": "continue",
                "materializer": "default_materializer_factory",
                "materialized_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
            },
            "transformer": {
                "deps": {"gen": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "transformer",
                "on_error": "continue",
                "materializer": "default_materializer_factory",
                "materialized_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
            },
            "consumer": {
                "deps": {"transformer": "Stream[int]"},
                "output": "None",
                "fn": "consumer",
                "on_error": "continue",
                "materializer": "default_materializer_factory",
                "materialized_deps": [],
                "pipeline": "linear_example",
                "parent_pipeline": None,
            },
        },
        "error_materializer_factory": "default_error_materializer_factory",
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
