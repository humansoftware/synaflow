from __future__ import annotations
from synaflow.core.dag_builder import build_dag
from collections.abc import Iterator
from typing import NamedTuple
from synaflow import pipeline, step
from synaflow.execution.sync_engine.executor import run as sync_run


def test_given_future_annotations_when_run_then_executes_successfully():

    class Params(NamedTuple):
        name: str = ""

    captured = None

    def my_step(name: str) -> str:
        nonlocal captured
        captured = name
        return name

    p = pipeline(
        name="test_future_annotations_run",
        params=Params,
        steps=[step("my_step", fn=my_step)],
    )
    sync_run(build_dag(p), Params(name="hello"))
    assert captured == "hello"


def test_given_future_annotations_when_custom_materializer_executed_then_receives_type_object():
    resolved_item_type = None

    def my_factory(ctx):
        nonlocal resolved_item_type
        resolved_item_type = ctx.item_type
        return lambda it: list(it)

    class Params(NamedTuple):
        pass

    def my_step() -> Iterator[str]:
        yield "a"
        yield "b"

    captured = None

    def sink(my_step: list[str]) -> list[str]:
        nonlocal captured
        captured = my_step
        return my_step

    p = pipeline(
        name="test_future_annotations_mat",
        params=Params,
        steps=[
            step("my_step", fn=my_step, materializer=my_factory),
            step("sink", fn=sink),
        ],
    )
    sync_run(build_dag(p), Params())
    assert captured == ["a", "b"]
    assert resolved_item_type == Iterator[str]
