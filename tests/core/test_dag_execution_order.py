from typing import NamedTuple

import pytest

from synaflow import pipeline, step


def test_given_output_compatible_but_executed_after_when_constructed_then_raises():
    class P(NamedTuple):
        x: int = 1

    def s1(s2: int) -> int:
        return s2

    def s2(x: int) -> int:
        return x

    with pytest.raises(ValueError, match="no prior step"):
        pipeline(name="t", params=P, steps=[step("s1", fn=s1), step("s2", fn=s2)])


def test_given_independent_steps_when_constructed_then_passes():
    class P(NamedTuple):
        a: int = 1
        b: int = 2

    def s1(a: int) -> int:
        return a

    def s2(b: int) -> int:
        return b

    pipeline(name="t", params=P, steps=[step("s1", fn=s1), step("s2", fn=s2)])


def test_given_independent_steps_reversed_when_constructed_then_passes():
    class P(NamedTuple):
        a: int = 1
        b: int = 2

    def s1(a: int) -> int:
        return a

    def s2(b: int) -> int:
        return b

    pipeline(name="t", params=P, steps=[step("s2", fn=s2), step("s1", fn=s1)])


def test_given_fan_out_when_constructed_then_passes():
    class P(NamedTuple):
        x: int = 5

    def gen(x: int) -> list[int]:
        return [x]

    def a(gen: int):
        pass

    def b(gen: int):
        pass

    def c(gen: int):
        pass

    pipeline(
        name="t",
        params=P,
        steps=[
            step("gen", fn=gen),
            step("a", fn=a),
            step("b", fn=b),
            step("c", fn=c),
        ],
    )


from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS

PACKS = {**SYNC_PACKS, **ASYNC_PACKS}


@pytest.mark.parametrize("pack_name, pack", list(PACKS.items()), ids=list(PACKS.keys()))
def test_corpus_execution_levels(pack_name, pack):
    if pack.expected_execution_levels is not None:
        assert pack.pipeline.get_execution_levels() == pack.expected_execution_levels
    if pack.json_dag is not None:
        assert pack.pipeline.to_dict() == pack.json_dag


def test_given_diamond_dag_when_consumers_of_branch_then_returns_merge():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "start": DagNode(deps={}),
        "branch_a": DagNode(deps={"start": "int"}),
        "branch_b": DagNode(deps={"start": "int"}),
        "merge": DagNode(deps={"branch_a": "int", "branch_b": "int"}),
    }
    assert set(dag.consumers_of("start")) == {"branch_a", "branch_b"}
    assert dag.consumers_of("branch_a") == ["merge"]
    assert dag.consumers_of("merge") == []
