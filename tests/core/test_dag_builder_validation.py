from typing import NamedTuple

import pytest

from synaflow import pipeline, step

from .conftest import build_minimal_dag


def test_given_duplicate_step_names_when_dag_built_then_raises():
    class Empty(NamedTuple):
        pass

    with pytest.raises(ValueError, match="duplicate"):
        pipeline(
            name="test",
            params=Empty,
            steps=[
                step("s1", fn=lambda: None),
                step("s1", fn=lambda: None),
            ],
        )


def test_given_circular_dependency_when_dag_built_then_raises():
    class Empty(NamedTuple):
        pass

    def s1(s2: int) -> int:
        return s2

    def s2(s1: int) -> int:
        return s1

    with pytest.raises(ValueError, match="no prior step"):
        pipeline(
            name="test",
            params=Empty,
            steps=[
                step("s1", fn=s1),
                step("s2", fn=s2),
            ],
        )
