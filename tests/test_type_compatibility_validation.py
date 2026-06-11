from typing import Generator, Iterator, NamedTuple

import pytest

from synaflow.pipeline import pipeline
from synaflow.step import step


@pytest.mark.parametrize(
    "description, producer_return, consumer_param, should_pass",
    [
        ("list[int] -> int", list[int], int, True),
        ("list[int] -> list[int]", list[int], list[int], True),
        ("Generator[int] -> int", Generator[int, None, None], int, True),
        ("int -> int | str", int, int | str, True),
        ("int -> list[int]", int, list[int], True),
        ("int -> Iterator[int]", int, Iterator[int], True),
        ("str -> int", str, int, False),
        ("int | str -> int", int | str, int, False),
        ("int | str -> int | str", int | str, int | str, True),
        ("str -> int | str", str, int | str, True),
        ("int -> int | str | None", int, int | str | None, True),
        ("int | str -> list[int | str]", int | str, list[int | str], True),
    ],
)
def test_given_compatibility_cases_when_constructed_then_validates_correctly(
    description, producer_return, consumer_param, should_pass
):
    class P(NamedTuple):
        pass

    def s1():
        pass

    s1.__annotations__ = {"return": producer_return}

    def s2(s1):
        pass

    s2.__annotations__ = {"s1": consumer_param}

    if should_pass:
        pipeline(name="t", params=P, steps=[step("s1", fn=s1), step("s2", fn=s2)])
    else:
        with pytest.raises(ValueError, match="expects"):
            pipeline(name="t", params=P, steps=[step("s1", fn=s1), step("s2", fn=s2)])


def test_given_two_consumers_of_same_producer_when_constructed_then_passes():
    class P(NamedTuple):
        pass

    def s1() -> list[int]:
        return [1, 2, 3]

    def s2(s1: int):
        pass

    def s3(s1: int):
        pass

    pipeline(
        name="t",
        params=P,
        steps=[step("s1", fn=s1), step("s2", fn=s2), step("s3", fn=s3)],
    )


def test_given_step_with_no_params_when_constructed_then_passes():
    class P(NamedTuple):
        pass

    def fn():
        pass

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_underscore_step_with_no_producer_when_constructed_then_passes():
    class P(NamedTuple):
        count: int = 3

    def gen(count: int) -> list[int]:
        return [count]

    def side(items: int):
        pass

    pipeline(
        name="t",
        params=P,
        steps=[step("items", fn=gen), step("_side", fn=side)],
    )


def test_given_param_type_is_nested_union_when_constructed_then_passes():
    class P(NamedTuple):
        x: int | str | None = 5

    def fn(x: int | str | None) -> str:
        return str(x)

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_producer_list_int_consumer_int_to_iterator_int_when_constructed_then_passes():
    class P(NamedTuple):
        pass

    def s1() -> list[int]:
        return [1, 2, 3]

    def s2(s1: int) -> int:
        return s1

    def s3(s2: Iterator[int]):
        pass

    pipeline(
        name="t",
        params=P,
        steps=[step("s1", fn=s1), step("s2", fn=s2), step("s3", fn=s3)],
    )
