import pytest
from synaflow import pipeline, step
from collections.abc import Iterator

from typing import NamedTuple


class Params(NamedTuple):
    pass


def producer() -> Iterator[int]:
    yield 1


def test_max_in_flight_validation_zero():
    with pytest.raises(ValueError, match="max_in_flight must be >= 1"):
        pipeline(
            name="test", params=Params, steps=[step("p", fn=producer, max_in_flight=0)]
        )


def test_max_in_flight_validation_negative():
    with pytest.raises(ValueError, match="max_in_flight must be >= 1"):
        pipeline(
            name="test", params=Params, steps=[step("p", fn=producer, max_in_flight=-1)]
        )


def test_max_in_flight_validation_type():
    with pytest.raises(ValueError, match="must be an integer"):
        pipeline(
            name="test",
            params=Params,
            steps=[step("p", fn=producer, max_in_flight="1")],
        )
