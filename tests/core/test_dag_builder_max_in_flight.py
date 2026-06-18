from typing import NamedTuple

import pytest

from synaflow import pipeline, step


class Empty(NamedTuple):
    pass


def test_given_max_in_flight_default_when_compiled_then_dag_node_has_1():
    p = pipeline(name="test", params=Empty, steps=[step("s", fn=lambda: None)])
    assert p.dag.steps["s"].max_in_flight == 1


def test_given_max_in_flight_explicit_when_compiled_then_stored():
    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("s", fn=lambda: None, max_in_flight=30)],
    )
    assert p.dag.steps["s"].max_in_flight == 30


def test_given_max_in_flight_serialized_when_to_dict_then_present():
    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("s", fn=lambda: None, max_in_flight=30)],
    )
    d = p.to_dict()
    assert d["steps"]["s"]["max_in_flight"] == 30


def test_given_max_in_flight_default_when_to_dict_then_present():
    p = pipeline(name="test", params=Empty, steps=[step("s", fn=lambda: None)])
    d = p.to_dict()
    assert d["steps"]["s"]["max_in_flight"] == 1


def test_given_max_in_flight_zero_when_compiled_then_raises():
    with pytest.raises(ValueError, match="max_in_flight must be >= 1"):
        pipeline(
            name="test",
            params=Empty,
            steps=[step("s", fn=lambda: None, max_in_flight=0)],
        )


def test_given_max_in_flight_negative_when_compiled_then_raises():
    with pytest.raises(ValueError, match="max_in_flight must be >= 1"):
        pipeline(
            name="test",
            params=Empty,
            steps=[step("s", fn=lambda: None, max_in_flight=-5)],
        )


def test_given_max_in_flight_non_int_when_compiled_then_raises():
    with pytest.raises(ValueError, match="max_in_flight must be an integer"):
        pipeline(
            name="test",
            params=Empty,
            steps=[step("s", fn=lambda: None, max_in_flight=3.5)],
        )


def test_given_max_in_flight_string_when_compiled_then_raises():
    with pytest.raises(ValueError, match="max_in_flight must be an integer"):
        pipeline(
            name="test",
            params=Empty,
            steps=[step("s", fn=lambda: None, max_in_flight="30")],
        )


def test_given_max_in_flight_bool_when_compiled_then_raises():
    with pytest.raises(ValueError, match="max_in_flight must be an integer"):
        pipeline(
            name="test",
            params=Empty,
            steps=[step("s", fn=lambda: None, max_in_flight=True)],
        )
