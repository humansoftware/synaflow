from __future__ import annotations

from typing import NamedTuple
from synaflow import pipeline, step


def test_given_future_annotations_when_pipeline_built_then_types_resolve_correctly():
    class Params(NamedTuple):
        name: str = ""

    def my_step(name: str) -> str:
        return name

    p = pipeline(
        name="test_future_annotations",
        params=Params,
        steps=[
            step("my_step", fn=my_step),
        ],
    )
    assert p.dag is not None
