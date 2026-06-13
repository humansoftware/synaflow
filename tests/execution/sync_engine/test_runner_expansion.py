from typing import Iterator, NamedTuple

import pytest

from synaflow import include, pipeline, run, step


class BParams(NamedTuple):
    text: str


def func_b1(text: str) -> str:
    return text.upper()


def func_b2(func_b1: str) -> int:
    return len(func_b1)


pipe_b = pipeline(
    name="TextProcessor",
    params=BParams,
    exports="func_b2",
    steps=[step("func_b1", fn=func_b1), step("func_b2", fn=func_b2)],
)


class AParams(NamedTuple):
    raw_texts: list[str]


def prepare_b_each(raw_texts: list[str]) -> Iterator[BParams]:
    for t in raw_texts:
        yield BParams(text=t)


def consolidate(my_text_processor: list[int]) -> int:
    return sum(my_text_processor)


def test_runner_executes_flattened_pipeline_each_mode():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )

    from synaflow.execution.sync_engine.executor import PipelineExecutor

    executor = PipelineExecutor(pipe_a.dag)
    executor.execute(params=AParams(raw_texts=["hi", "world", "synaflow"]))
    # len("hi") = 2, len("world") = 5, len("synaflow") = 8
    # sum = 15
    assert executor.outputs["consolidate"] == 15
