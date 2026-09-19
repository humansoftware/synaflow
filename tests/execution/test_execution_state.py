"""Unit tests for the shared ExecutionState key scheme and seeding."""

from typing import NamedTuple

import pytest

from synaflow.core.dag import Dag, DagNode
from synaflow.execution.state import ExecutionState


class _Params(NamedTuple):
    count: int
    label: str


def _state() -> ExecutionState:
    return ExecutionState(Dag(name="state-test"))


def test_given_namedtuple_params_when_seeded_then_fields_become_outputs():
    state = _state()
    state.seed(_Params(count=3, label="a"))
    assert state.raw_outputs()["count"] == 3
    assert state.raw_outputs()["label"] == "a"


def test_given_plain_dict_params_when_seeded_then_keys_become_outputs():
    state = _state()
    state.seed({"count": 1, "label": "x"})
    assert state.raw_outputs()["count"] == 1
    assert state.raw_outputs()["label"] == "x"


def test_given_unsupported_params_type_when_seeded_then_raises_type_error():
    state = _state()
    with pytest.raises(TypeError, match="must be a NamedTuple, dataclass or dict"):
        state.seed(42)


def test_given_consumer_scoped_output_when_read_by_other_consumer_then_no_cross_leak():
    state = _state()
    state._dag.steps = {
        "producer": DagNode(),
        "left": DagNode(deps={"producer": int}),
        "right": DagNode(deps={"producer": int}),
    }
    state.set_output("producer", "scoped", "left")
    assert state.get_output("producer", "left") == "scoped"
    # With multiple consumers the key space is scoped per consumer; a
    # branch that was never fed sees nothing.
    assert state.get_output("producer", "right") is None


def test_given_single_consumer_when_output_key_then_uses_producer_name():
    state = _state()
    state._dag.steps = {
        "producer": DagNode(),
        "only": DagNode(deps={"producer": int}),
    }
    state.set_output("producer", 7, "only")
    assert state.get_output("producer", "only") == 7


def test_given_missing_dependency_when_inputs_available_then_false():
    state = _state()
    state._dag.steps = {
        "a": DagNode(),
        "b": DagNode(deps={"a": int}),
    }
    assert state.inputs_available("b") is False
    state.set_output("a", 1)
    assert state.inputs_available("b") is True
