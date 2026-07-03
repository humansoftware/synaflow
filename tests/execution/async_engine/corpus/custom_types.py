from tests.common.pipeline_pack import PipelinePack
from collections.abc import AsyncGenerator
from typing import NamedTuple
from dataclasses import dataclass

from synaflow import pipeline, step


@dataclass
class CustomRecord:
    id: int
    name: str


class CustomTypesParams(NamedTuple):
    pass


async def records() -> AsyncGenerator[CustomRecord, None, None]:
    yield CustomRecord(id=1, name="alice")
    yield CustomRecord(id=2, name="bob")


async def process(records: list[CustomRecord]) -> int:
    return len(records)


async def async_list(async_iterator) -> list:
    items = []
    async for item in async_iterator:
        items.append(item)
    return items


custom_types_pipeline = pipeline(
    name="custom_types_example",
    params=CustomTypesParams,
    steps=[
        step("records", fn=records, materializer=async_list),
        step("process", fn=process),
    ],
)

pack = PipelinePack(
    json_dag={
        "name": "custom_types_example",
        "params": {},
        "steps": {
            "records": {
                "deps": {},
                "output": "Stream[CustomRecord, None, None]",
                "fn": "records",
                "on_error": "continue",
                "mode": "all",
                "materializer": "async_list",
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "custom_types_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
            "process": {
                "deps": {"records": "list[CustomRecord]"},
                "output": "int",
                "fn": "process",
                "on_error": "continue",
                "mode": "all",
                "materializer": None,
                "error_materializer": "log_error",
                "each_mode_deps": [],
                "pipeline": "custom_types_example",
                "parent_pipeline": None,
                "max_in_flight": 1,
            },
        },
        "error_materializer": "log_error_materializer",
    },
    pipeline=custom_types_pipeline,
    input_params=CustomTypesParams(),
    step_results={
        "records": [CustomRecord(id=1, name="alice"), CustomRecord(id=2, name="bob")],
        "process": 2,
    },
    expected_call_order=["records", "process"],
    expected_execution_levels=[
        ["records"],
        ["process"],
    ],
)
