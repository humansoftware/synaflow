from collections.abc import Generator, Iterator
from typing import NamedTuple

from synaflow import pipeline, step


class FibonacciParams(NamedTuple):
    count: int = 10


def fibonacci_generator(count: int) -> Generator[int, None, None]:
    a, b = 0, 1
    for _ in range(count):
        yield a
        a, b = b, a + b


def square_numbers(fibonacci_generator: Iterator[int]) -> Generator[int, None, None]:
    for x in fibonacci_generator:
        yield x * x


def consumer(square_numbers: Iterator[int]) -> None:
    pass


from tests.pipeline_pack import PipelinePack

pipeline_def = pipeline(
    name="fibonacci",
    params=FibonacciParams,
    steps=[
        step("fibonacci_generator", fn=fibonacci_generator),
        step("square_numbers", fn=square_numbers),
        step("consumer", fn=consumer),
    ],
)

pack = PipelinePack(
    pipeline=pipeline_def,
    input_params=FibonacciParams(count=10),
    step_results={
        "fibonacci_generator": [0, 1, 1, 2, 3, 5, 8, 13, 21, 34],
        "square_numbers": [0, 1, 1, 4, 9, 25, 64, 169, 441, 1156],
        "consumer": None,
    },
)
