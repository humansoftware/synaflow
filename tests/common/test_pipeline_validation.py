from typing import NamedTuple

import pytest

from synaflow.pipeline import pipeline
from synaflow.step import step


def test_given_scalar_params_when_constructed_then_passes():
    class P(NamedTuple):
        x: int = 5

    def fn(x: int) -> int:
        return x

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_list_param_when_constructed_then_passes():
    class P(NamedTuple):
        items: list[int] = []

    def fn(items: list[int]) -> int:
        return len(items)

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_union_param_when_constructed_then_passes():
    class P(NamedTuple):
        x: int = 5

    def fn(x: int | str) -> str:
        return str(x)

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_dependency_on_prior_step_when_constructed_then_passes():
    class P(NamedTuple):
        count: int = 3

    def s1(count: int) -> list[int]:
        return [count]

    def s2(s1: list[int]) -> int:
        return len(s1)

    pipeline(name="t", params=P, steps=[step("s1", fn=s1), step("s2", fn=s2)])


def test_given_dependency_on_future_step_when_constructed_then_raises():
    class P(NamedTuple):
        count: int = 3

    def s1(future: int) -> int:
        return future

    def s2(count: int) -> int:
        return count

    with pytest.raises(ValueError, match="no prior step"):
        pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn=s1), step("future", fn=s2)],
        )


def test_given_dependency_on_pipeline_param_when_constructed_then_passes():
    class P(NamedTuple):
        limit: int = 10

    def fn(limit: int) -> int:
        return limit

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_duplicate_step_name_when_constructed_then_raises():
    class P(NamedTuple):
        pass

    def fn():
        pass

    with pytest.raises(ValueError, match="duplicate"):
        pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn=fn), step("s1", fn=fn)],
        )


def test_given_non_namedtuple_params_when_constructed_then_raises():
    class P:
        pass

    def fn():
        pass

    with pytest.raises(ValueError, match="must be a NamedTuple"):
        my_pipeline = pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn=fn)],
        )


def test_given_non_callable_step_when_constructed_then_raises():
    class P(NamedTuple):
        pass

    with pytest.raises(ValueError, match="must have a callable 'fn'"):
        my_pipeline = pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn="not_a_function")],
        )


def test_given_dependency_on_nonexistent_param_when_constructed_then_raises():
    class P(NamedTuple):
        pass

    def fn(missing: int):
        pass

    with pytest.raises(ValueError, match="but no prior step or param produces it"):
        my_pipeline = pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn=fn)],
        )


def test_given_explicit_none_producer_and_strict_consumer_when_constructed_then_raises():
    class P(NamedTuple):
        pass

    def producer() -> type(None):
        return None

    def consumer(producer: int):
        pass

    with pytest.raises(ValueError, match="produces explicit NoneType"):
        my_pipeline = pipeline(
            name="t",
            params=P,
            steps=[step("producer", fn=producer), step("consumer", fn=consumer)],
        )


def test_given_mixed_sync_and_async_functions_when_constructed_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def sync_generator(items: list[int]) -> Iterator[int]:
        for i in items:
            yield i

    async def async_consumer(sync_generator: int):
        pass

    with pytest.raises(ValueError, match="UNRUNNABLE"):
        pipeline(
            name="test",
            params=P,
            steps=[
                step("sync_generator", fn=sync_generator),
                step("async_consumer", fn=async_consumer),
            ],
        )
