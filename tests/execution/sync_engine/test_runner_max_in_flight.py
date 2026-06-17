from collections.abc import Generator, Iterator
from typing import NamedTuple

from synaflow import OnError, pipeline, run, step


class Empty(NamedTuple):
    pass


class Count(NamedTuple):
    count: int = 5


def test_given_max_in_flight_1_when_linear_then_preserves_lockstep():
    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    results: list[int] = []

    def consumer(producer: int) -> None:
        results.append(producer)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=1),
            step("consumer", fn=consumer),
        ],
    )
    run(p, Count(count=5))
    assert results == [0, 1, 2, 3, 4]


def test_given_max_in_flight_30_when_linear_then_pipeline_completes():
    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    results: list[int] = []

    def consumer(producer: Iterator[int]) -> None:
        for x in producer:
            results.append(x)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=30),
            step("consumer", fn=consumer),
        ],
    )
    run(p, Count(count=5))
    assert results == [0, 1, 2, 3, 4]


def test_given_max_in_flight_on_terminal_step_when_terminal_then_no_effect():
    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def terminal(producer: Iterator[int]) -> None:
        for x in producer:
            pass

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=30),
            step("terminal", fn=terminal),
        ],
    )
    run(p, Count(count=5))


def test_given_max_in_flight_when_on_error_continue_then_still_works():
    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    results: list[int] = []

    def fragile(producer: int) -> int:
        if producer == 2:
            raise ValueError("item 2 fails")
        return producer

    def consumer(fragile: int) -> None:
        results.append(fragile)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("fragile", fn=fragile, on_error=OnError.CONTINUE),
            step("consumer", fn=consumer, on_error=OnError.CONTINUE),
        ],
    )
    run(p, Count(count=5))
    assert results == [0, 1, 3, 4]


def test_given_max_in_flight_3_when_fanout_two_consumers_then_both_get_all_items():
    """Fan-out with bounded handoff: both consumers should receive all items."""

    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    results_a: list[int] = []
    results_b: list[int] = []

    def consumer_a(producer: Iterator[int]) -> None:
        for x in producer:
            results_a.append(x)

    def consumer_b(producer: Iterator[int]) -> None:
        for x in producer:
            results_b.append(x)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a),
            step("consumer_b", fn=consumer_b),
        ],
    )
    run(p, Count(count=10))
    assert results_a == list(range(10))
    assert results_b == list(range(10))


def test_given_max_in_flight_when_producer_does_not_exceed_bounded_ahead():
    """With max_in_flight=3, the BoundedIterator limits producer advancement."""

    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    results: list[int] = []

    def consumer(producer: Iterator[int]) -> None:
        for x in producer:
            results.append(x)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer", fn=consumer),
        ],
    )
    run(p, Count(count=20))
    assert results == list(range(20))
