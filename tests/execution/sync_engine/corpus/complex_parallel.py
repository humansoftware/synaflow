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
from tests.pipeline_pack import PipelinePack

pipeline_def = pipeline(
    name="complex_parallel",
    params=ComplexParallelParams,
    steps=[
        step("step1", fn=step1),
        step("step2", fn=step2),
        step("step3", fn=step3),
        step("step4", fn=step4),
        step("step5", fn=step5),
    ],
)

pack = PipelinePack(
    pipeline=pipeline_def,
    input_params=ComplexParallelParams(base=1),
    expected_results={
        "step1": None,
        "step2": None,
        "step3": None,
        "step4": None,
        "step5": None,
    },
)
