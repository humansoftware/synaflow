from synaflow.core.dag_builder import build_dag
from typing import NamedTuple
import pytest
from synaflow import pipeline, step
from collections.abc import Iterator
from synaflow import include


class Empty(NamedTuple):
    pass


def test_given_max_in_flight_default_when_compiled_then_dag_node_has_1():
    p = pipeline(name="test", params=Empty, steps=[step("s", fn=lambda: None)])
    assert build_dag(p).steps["s"].max_in_flight == 1


def test_given_max_in_flight_explicit_when_compiled_then_stored():
    p = pipeline(
        name="test", params=Empty, steps=[step("s", fn=lambda: None, max_in_flight=30)]
    )
    assert build_dag(p).steps["s"].max_in_flight == 30


def test_given_max_in_flight_serialized_when_to_dict_then_present():
    p = pipeline(
        name="test", params=Empty, steps=[step("s", fn=lambda: None, max_in_flight=30)]
    )
    d = build_dag(p).to_dict()
    assert d["steps"]["s"]["max_in_flight"] == 30


def test_given_max_in_flight_default_when_to_dict_then_present():
    p = pipeline(name="test", params=Empty, steps=[step("s", fn=lambda: None)])
    d = build_dag(p).to_dict()
    assert d["steps"]["s"]["max_in_flight"] == 1


def test_given_max_in_flight_zero_when_built_then_raises():
    p = pipeline(
        name="test", params=Empty, steps=[step("s", fn=lambda: None, max_in_flight=0)]
    )
    with pytest.raises(ValueError, match="max_in_flight must be >= 1"):
        build_dag(p)


def test_given_max_in_flight_negative_when_built_then_raises():
    p = pipeline(
        name="test", params=Empty, steps=[step("s", fn=lambda: None, max_in_flight=-5)]
    )
    with pytest.raises(ValueError, match="max_in_flight must be >= 1"):
        build_dag(p)


def test_given_max_in_flight_non_int_when_built_then_raises():
    p = pipeline(
        name="test", params=Empty, steps=[step("s", fn=lambda: None, max_in_flight=3.5)]
    )
    with pytest.raises(ValueError, match="max_in_flight must be an integer"):
        build_dag(p)


def test_given_max_in_flight_string_when_built_then_raises():
    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("s", fn=lambda: None, max_in_flight="30")],
    )
    with pytest.raises(ValueError, match="max_in_flight must be an integer"):
        build_dag(p)


def test_given_max_in_flight_bool_when_built_then_raises():
    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("s", fn=lambda: None, max_in_flight=True)],
    )
    with pytest.raises(ValueError, match="max_in_flight must be an integer"):
        build_dag(p)


def test_adapter_steps_serialize_max_in_flight_1():

    class SubParams(NamedTuple):
        val: int

    def sub_step(val: int) -> int:
        return val

    sub_pipe = pipeline(
        name="sub",
        params=SubParams,
        exports="sub_step",
        steps=[step("sub_step", fn=sub_step)],
    )

    class MainParams(NamedTuple):
        vals: list[int]

    def adapter(vals: list[int]) -> Iterator[SubParams]:
        for v in vals:
            yield SubParams(val=v)

    p = pipeline(
        name="main",
        params=MainParams,
        steps=[include("sub_instance", pipeline=sub_pipe, fn=adapter)],
    )
    d = build_dag(p).to_dict()
    assert d["steps"]["sub_instance__adapter"]["max_in_flight"] == 1
