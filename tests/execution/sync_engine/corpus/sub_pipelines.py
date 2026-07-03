from typing import Iterator, NamedTuple

from synaflow import include, pipeline, step


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


pipe = pipeline(
    name="MainPipeline",
    params=AParams,
    steps=[
        include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
        step("consolidate", fn=consolidate),
    ],
)

from tests.common.pipeline_pack import PipelinePack  # noqa: E402

pack = PipelinePack(
    json_dag={
        "name": "MainPipeline",
        "params": {"raw_texts": "list[str]"},
        "steps": {
            "my_text_processor__adapter": {
                "deps": {"raw_texts": "list[str]"},
                "output": "Stream[BParams]",
                "fn": "prepare_b_each",
                "on_error": "stop",
                "mode": "all",
                "materializer": "_identity",
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "MainPipeline",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "my_text_processor__func_b1": {
                "deps": {"my_text_processor__adapter": "BParams"},
                "output": "ListType(<class 'str'>)",
                "fn": "func_b1",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["my_text_processor__adapter"],
                "pipeline": "TextProcessor",
                "parent_pipeline": "MainPipeline",
                "max_in_flight": 1,
            },
            "my_text_processor": {
                "deps": {"my_text_processor__func_b1": "str"},
                "output": "ListType(<class 'int'>)",
                "fn": "func_b2",
                "on_error": "continue",
                "mode": "each",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": ["my_text_processor__func_b1"],
                "pipeline": "TextProcessor",
                "parent_pipeline": "MainPipeline",
                "max_in_flight": 1,
            },
            "consolidate": {
                "deps": {"my_text_processor": "list[int]"},
                "output": "int",
                "fn": "consolidate",
                "on_error": "continue",
                "mode": "all",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "MainPipeline",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=pipe,
    input_params=AParams(raw_texts=["hi", "world", "synaflow"]),
    step_results={
        "my_text_processor__adapter": [
            BParams(text="hi"),
            BParams(text="world"),
            BParams(text="synaflow"),
        ],
        "my_text_processor__func_b1": ["HI", "WORLD", "SYNAFLOW"],
        "my_text_processor": [2, 5, 8],
        "consolidate": 15,
    },
)
