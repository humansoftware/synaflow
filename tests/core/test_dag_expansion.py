from typing import Iterator, NamedTuple

import pytest

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


def test_pipeline_compiles_flattened_dag():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )

    dag = pipe_a.dag
    assert "my_text_processor__adapter" in dag
    assert "my_text_processor__func_b1" in dag
    assert "my_text_processor" in dag  # This is func_b2
    assert "consolidate" in dag

    assert "my_text_processor__adapter" in dag["my_text_processor__func_b1"]["deps"]
    assert "my_text_processor__func_b1" in dag["my_text_processor"]["deps"]
    assert "my_text_processor" in dag["consolidate"]["deps"]


def test_include_step_requires_return_type_hint():
    def bad_adapter(raw_texts: list[str]):
        return BParams(text="test")

    with pytest.raises(ValueError, match="must have a return type hint"):
        pipeline(
            name="MainPipeline",
            params=AParams,
            steps=[include("bad_sub", pipeline=pipe_b, fn=bad_adapter)],
        )


def test_include_step_requires_pipeline_exports():
    pipe_no_exports = pipeline(
        name="NoExports", params=BParams, steps=[step("func_b1", fn=func_b1)]
    )

    with pytest.raises(ValueError, match="does not define 'exports'"):
        pipeline(
            name="MainPipeline",
            params=AParams,
            steps=[include("bad_sub", pipeline=pipe_no_exports, fn=prepare_b_each)],
        )


def test_include_step_requires_strict_type_hint():
    def bad_type_adapter(raw_texts: list[str]) -> int:
        return 5

    with pytest.raises(ValueError, match="must return 'BParams'"):
        pipeline(
            name="MainPipeline",
            params=AParams,
            steps=[include("bad_sub", pipeline=pipe_b, fn=bad_type_adapter)],
        )


def test_infinite_cycle_detection():
    class Empty(NamedTuple):
        pass

    def dummy() -> Empty:
        return Empty()

    pipe_cycle_a = pipeline(
        name="PipeA",
        params=Empty,
        exports="dummy",
        steps=[step("dummy", fn=dummy)],
    )

    pipe_cycle_b = pipeline(
        name="PipeB",
        params=Empty,
        exports="dummy",
        steps=[
            include("inc_a", pipeline=pipe_cycle_a, fn=dummy),
            step("dummy", fn=dummy),
        ],
    )

    # Now make A include B
    pipe_cycle_a.steps.append(include("inc_b", pipeline=pipe_cycle_b, fn=dummy))

    with pytest.raises(ValueError, match="Infinite cycle detected"):
        pipeline(
            name="TriggerCycle",
            params=Empty,
            steps=[include("start", pipeline=pipe_cycle_a, fn=dummy)],
        )
