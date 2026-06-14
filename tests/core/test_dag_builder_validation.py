from typing import NamedTuple

import pytest

from synaflow import StepMode, pipeline, step


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


def test_given_mode_each_when_no_iterable_dep_can_be_unrolled_then_raises():
    class P(NamedTuple):
        count: int = 3

    def transform(count: int) -> int:
        return count * 2

    with pytest.raises(ValueError, match="forced to EACH mode"):
        pipeline(
            name="test",
            params=P,
            steps=[step("transform", fn=transform, mode=StepMode.EACH)],
        )


def test_given_mode_all_when_signature_requires_each_then_raises():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> int:
        return items * 2

    with pytest.raises(ValueError, match="forced to ALL mode"):
        pipeline(
            name="test",
            params=P,
            steps=[step("transform", fn=transform, mode=StepMode.ALL)],
        )


def test_given_mode_each_when_output_would_require_nested_streams_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> Iterator[int]:
        yield items

    with pytest.raises(ValueError, match="nested streams"):
        pipeline(
            name="test",
            params=P,
            steps=[step("transform", fn=transform, mode=StepMode.EACH)],
        )


def test_given_mode_each_when_consumer_would_require_iterator_of_lists_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> Iterator[int]:
        yield items

    def consume(transform: Iterator[list[int]]):
        pass

    with pytest.raises(ValueError, match="nested streams"):
        pipeline(
            name="test",
            params=P,
            steps=[
                step("transform", fn=transform, mode=StepMode.EACH),
                step("consume", fn=consume),
            ],
        )


def test_given_mode_each_when_only_some_dependencies_can_be_unrolled_then_non_matching_dependencies_are_not_forced():
    class P(NamedTuple):
        scalar: int = 2
        items: list[int] = [1, 2, 3]

    def transform(scalar: int, items: int) -> int:
        return scalar * items

    p = pipeline(
        name="test",
        params=P,
        steps=[step("transform", fn=transform, mode=StepMode.EACH)],
    )

    assert p.dag.steps["transform"].each_mode_deps == ["items"]
