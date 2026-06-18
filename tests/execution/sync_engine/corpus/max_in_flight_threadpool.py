from collections.abc import Generator, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event
from typing import NamedTuple

from synaflow import pipeline, step
from tests.common.pipeline_pack import PipelinePack

_POOL = ThreadPoolExecutor(max_workers=10)
_RELEASE = Event()
_RELEASE.set()
_FUTURES: dict[int, Future] = {}


class ThreadpoolParams(NamedTuple):
    count: int = 3


def numbers(count: int) -> Generator[int, None, None]:
    yield from range(count)


def _resolve(value: int) -> int:
    _RELEASE.wait()
    return value * 10


def start(numbers: int) -> int:
    _FUTURES[numbers] = _POOL.submit(_resolve, numbers)
    return numbers


def await_result(start: Iterator[int]) -> list[int]:
    return [_FUTURES[token].result() for token in start]


threadpool_pipeline = pipeline(
    name="max_in_flight_threadpool",
    params=ThreadpoolParams,
    steps=[
        step("numbers", fn=numbers),
        step("start", fn=start, max_in_flight=5),
        step("await_result", fn=await_result),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "max_in_flight_threadpool",
        "params": {"count": "int"},
        "steps": {
            "numbers": {
                "deps": {"count": "int"},
                "output": "Stream[int, None, None]",
                "fn": "numbers",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [],
                "pipeline": "max_in_flight_threadpool",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "start": {
                "deps": {"numbers": "int"},
                "output": "ListType(<class 'int'>)",
                "fn": "start",
                "on_error": "continue",
                "mode": "each",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": ["numbers"],
                "pipeline": "max_in_flight_threadpool",
                "parent_pipeline": None,
                "max_in_flight": 5,
            },
            "await_result": {
                "deps": {"start": "Stream[int]"},
                "output": "list[int]",
                "fn": "await_result",
                "on_error": "continue",
                "mode": "all",
                "materializer": "memory_materializer",
                "error_materializer": "log_error_materializer",
                "materialized_deps": [],
                "each_mode_deps": [],
                "pipeline": "max_in_flight_threadpool",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=threadpool_pipeline,
    input_params=ThreadpoolParams(count=3),
    step_results={
        "numbers": [0, 1, 2],
        "await_result": [0, 10, 20],
    },
    expected_execution_levels=[["numbers"], ["start"], ["await_result"]],
)
