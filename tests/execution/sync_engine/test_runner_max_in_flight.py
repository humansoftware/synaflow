from collections.abc import Generator, Iterator
from time import sleep
import threading
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


def test_given_max_in_flight_1_when_fanout_slow_branch_then_bound_is_exact():
    log: list[str] = []

    def producer(count: int) -> Generator[int, None, None]:
        for i in range(count):
            log.append(f"prod {i}")
            yield i

    def fast(producer: Iterator[int]) -> None:
        for item in producer:
            log.append(f"fast {item}")

    def slow(producer: Iterator[int]) -> None:
        for item in producer:
            log.append(f"slow-recv {item}")
            sleep(0.01)
            log.append(f"slow {item}")

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=1),
            step("fast", fn=fast),
            step("slow", fn=slow),
        ],
    )
    run(p, Count(count=5))

    slow_0_index = log.index("slow-recv 0")
    prod_2_index = log.index("prod 2")
    assert slow_0_index < prod_2_index


def test_given_max_in_flight_3_when_fanout_lazy_and_eager_then_both_receive_items():
    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    lazy_results: list[int] = []
    eager_results: list[list[int]] = []

    def lazy_consumer(producer: Iterator[int]) -> None:
        for item in producer:
            lazy_results.append(item)

    def eager_consumer(producer: list[int]) -> None:
        eager_results.append(producer)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("lazy_consumer", fn=lazy_consumer),
            step("eager_consumer", fn=eager_consumer),
        ],
    )
    run(p, Count(count=10))

    assert lazy_results == list(range(10))
    assert eager_results == [list(range(10))]


def test_given_handoff_and_unrelated_sibling_when_run_then_unrelated_step_stays_on_main_thread():
    class P(NamedTuple):
        count: int = 5

    main_thread_id = threading.get_ident()
    thread_ids: list[int] = []

    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def lazy_consumer(producer: Iterator[int]) -> None:
        for _ in producer:
            pass

    def unrelated(count: int) -> None:
        thread_ids.append(threading.get_ident())

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("lazy_consumer", fn=lazy_consumer),
            step("unrelated", fn=unrelated),
        ],
    )
    run(p, Count(count=5))

    assert thread_ids == [main_thread_id]


def test_given_user_resource_with_close_when_used_as_param_then_executor_does_not_close_it():
    class Resource:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class P(NamedTuple):
        resource: Resource

    seen: list[Resource] = []

    def use_resource(resource: Resource) -> None:
        seen.append(resource)

    resource = Resource()
    p = pipeline(
        name="test",
        params=P,
        steps=[step("use_resource", fn=use_resource)],
    )
    run(p, P(resource=resource))

    assert seen == [resource]
    assert resource.closed is False
