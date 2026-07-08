from typing import NamedTuple

import pytest

from synaflow import StepMode, pipeline, step
from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS


def test_given_output_compatible_but_executed_after_when_constructed_then_raises():
    class P(NamedTuple):
        x: int = 1

    def s1(s2: int) -> int:
        return s2

    def s2(x: int) -> int:
        return x

    with pytest.raises(ValueError, match="no resource, prior step, or params field"):
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


PACKS = {**SYNC_PACKS, **ASYNC_PACKS}


def _normalize_exported_dag_for_contract_assertions(dag_dict: dict) -> dict:
    normalized = {
        "name": dag_dict["name"],
        "params": dict(dag_dict["params"]),
        "steps": {},
    }
    if "resources" in dag_dict:
        normalized["resources"] = dict(dag_dict["resources"])
    if "error_materializer" in dag_dict:
        normalized["error_materializer"] = dag_dict["error_materializer"]
    if "pipeline_observers" in dag_dict:
        normalized["pipeline_observers"] = list(dag_dict["pipeline_observers"])

    for step_name, step_def in dag_dict["steps"].items():
        normalized["steps"][step_name] = {
            key: value
            for key, value in step_def.items()
            if key
            not in {
                "materialized_deps",
                "needs_materialize_reasons",
                "output_contract",
                "consumer_contracts",
                "publish_plan",
                # Issue #105: scope stamping fields don't affect DAG
                # structure or execution levels, so they're excluded
                # from the corpus contract assertion. They are still
                # emitted by ``to_serializable()`` for runtime/UI use.
                "step_index_in_scope",
                "step_total_in_scope",
            }
        }
    return normalized


@pytest.mark.parametrize("pack_name, pack", list(PACKS.items()), ids=list(PACKS.keys()))
def test_corpus_execution_levels(pack_name, pack):
    if pack.expected_execution_levels is not None:
        assert pack.pipeline.get_execution_levels() == pack.expected_execution_levels
    if pack.json_dag is not None:
        assert _normalize_exported_dag_for_contract_assertions(
            pack.pipeline.to_dict()
        ) == _normalize_exported_dag_for_contract_assertions(pack.json_dag)


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


def test_given_single_consumer_when_output_key_then_returns_producer_name():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "producer": DagNode(deps={}),
        "consumer": DagNode(deps={"producer": int}),
    }

    assert dag.output_key("producer", "consumer") == "producer"


def test_given_multiple_consumers_when_output_key_then_scopes_by_consumer():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "producer": DagNode(deps={}),
        "left": DagNode(deps={"producer": int}),
        "right": DagNode(deps={"producer": int}),
    }

    assert dag.output_key("producer", "left") == "producer__left"
    assert dag.output_key("producer", "right") == "producer__right"


def test_given_step_name_prefixed_with_underscore_when_is_hidden_step_then_true():
    from synaflow.core.dag import Dag

    dag = Dag(name="test")

    assert dag.is_hidden_step("_internal") is True
    assert dag.is_hidden_step("visible") is False


def test_given_hidden_step_when_is_terminal_step_then_true_even_with_consumers():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "_internal": DagNode(deps={}),
        "consumer": DagNode(deps={"_internal": int}),
    }

    assert dag.is_terminal_step("_internal") is True


def test_given_visible_step_without_consumers_when_is_terminal_step_then_true():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "producer": DagNode(deps={}),
    }

    assert dag.is_terminal_step("producer") is True


def test_given_visible_step_with_consumers_when_is_terminal_step_then_false():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "producer": DagNode(deps={}),
        "consumer": DagNode(deps={"producer": int}),
    }

    assert dag.is_terminal_step("producer") is False


def test_given_each_mode_when_each_inputs_then_returns_correct_deps():
    from collections.abc import Iterator

    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "gen": DagNode(output=Iterator[int], deps={}),
        "a": DagNode(deps={"gen": int}, mode=StepMode.EACH, each_mode_deps=["gen"]),
    }
    assert dag.each_inputs("a") == ["gen"]


def test_given_standard_mode_when_each_inputs_then_returns_empty():
    from collections.abc import Iterator

    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "gen": DagNode(output=Iterator[int], deps={}),
        "a": DagNode(deps={"gen": Iterator[int]}, mode=StepMode.ALL),
    }
    assert dag.each_inputs("a") == []


def test_given_dag_node_with_resolved_each_mode_when_each_inputs_then_reads_from_dag():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "items": DagNode(output=list[int], deps={}),
        "transform": DagNode(
            deps={"items": int},
            mode=StepMode.EACH,
            each_mode_deps=["items"],
        ),
    }

    assert dag.each_inputs("transform") == ["items"]


def test_given_materialized_producer_when_needs_materialize_then_true():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "producer": DagNode(deps={}, materialize_output=True),
        "lazy": DagNode(deps={"producer": int}),
        "eager": DagNode(deps={"producer": list[int]}),
    }

    assert dag.needs_materialize("producer") is True


def test_given_linear_dag_when_get_execution_levels_then_returns_sequential_levels():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "a": DagNode(deps={}),
        "b": DagNode(deps={"a": "int"}),
        "c": DagNode(deps={"b": "int"}),
    }
    assert dag.get_execution_levels() == [["a"], ["b"], ["c"]]


def test_given_diamond_dag_when_get_execution_levels_then_parallel_in_same_level():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "a": DagNode(deps={}),
        "b": DagNode(deps={"a": "int"}),
        "c": DagNode(deps={"a": "int"}),
        "d": DagNode(deps={"b": "int", "c": "int"}),
    }
    levels = dag.get_execution_levels()
    assert levels[0] == ["a"]
    assert set(levels[1]) == {"b", "c"}
    assert levels[2] == ["d"]


def test_given_independent_steps_when_get_execution_levels_then_all_in_same_level():
    from synaflow.core.dag import Dag, DagNode

    dag = Dag(name="test")
    dag.steps = {
        "a": DagNode(deps={}),
        "b": DagNode(deps={}),
    }
    assert dag.get_execution_levels() == [["a", "b"]]


def test_given_dag_when_to_dict_then_returns_correct_structure():
    from synaflow.core.dag import Dag, DagNode
    from synaflow.core.types import OnError

    dag = Dag(name="unit_test")
    dag.params = {"count": int}
    dag.steps = {
        "gen": DagNode(
            fn=lambda: None,
            deps={"count": int},
            output="Stream[int, None, None]",
            on_error=OnError.CONTINUE,
            mode=StepMode.EACH,
            each_mode_deps=["count"],
        ),
    }

    result = dag.to_dict()
    assert result["name"] == "unit_test"
    assert result["params"] == {"count": "int"}
    assert "gen" in result["steps"]
    assert result["steps"]["gen"]["fn"] == "<lambda>"
    assert result["steps"]["gen"]["on_error"] == "continue"
    assert result["steps"]["gen"]["mode"] == "each"
    assert result["steps"]["gen"]["deps"] == {"count": "int"}
    assert result["steps"]["gen"]["each_mode_deps"] == ["count"]
