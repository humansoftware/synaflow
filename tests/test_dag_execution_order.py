from typing import NamedTuple

import pytest

from synaflow.pipeline import pipeline
from synaflow.step import step


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
