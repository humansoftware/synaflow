from collections.abc import Generator, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from time import sleep
import threading
from typing import NamedTuple

import pytest

from synaflow import OnError, pipeline, run, step
from synaflow.core.exceptions import PipelineStopException


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


def test_given_max_in_flight_3_when_linear_stream_then_ahead_distance_stays_bounded():
    produced: list[int] = []
    consumed: list[int] = []
    max_seen_ahead = 0

    def producer(count: int) -> Generator[int, None, None]:
        nonlocal max_seen_ahead
        for i in range(count):
            produced.append(i)
            max_seen_ahead = max(max_seen_ahead, len(produced) - len(consumed))
            yield i

    def consumer(producer: Iterator[int]) -> None:
        for item in producer:
            consumed.append(item)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer", fn=consumer),
        ],
    )
    run(p, Count(count=20))

    assert produced == list(range(20))
    assert consumed == list(range(20))
    assert max_seen_ahead <= 3


def test_given_max_in_flight_3_when_linear_stream_then_producer_blocks_before_item_4():
    log: list[str] = []

    def producer(count: int) -> Generator[int, None, None]:
        for i in range(count):
            log.append(f"prod {i}")
            yield i

    def consumer(producer: Iterator[int]) -> None:
        for item in producer:
            log.append(f"recv {item}")
            sleep(0.01)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer", fn=consumer),
        ],
    )
    run(p, Count(count=6))

    assert log.index("recv 0") < log.index("prod 4")


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


def test_given_max_in_flight_3_when_terminal_lazy_consumer_then_stream_drains_fully():
    produced: list[int] = []
    consumed: list[int] = []

    def producer(count: int) -> Generator[int, None, None]:
        for i in range(count):
            produced.append(i)
            yield i

    def terminal(producer: Iterator[int]) -> None:
        for item in producer:
            consumed.append(item)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("terminal", fn=terminal),
        ],
    )
    run(p, Count(count=10))

    assert produced == list(range(10))
    assert consumed == list(range(10))


def test_given_max_in_flight_3_when_branch_stops_early_then_other_branch_finishes():
    early: list[int] = []
    full: list[int] = []

    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def early_consumer(producer: Iterator[int]) -> None:
        for item in producer:
            early.append(item)
            break

    def full_consumer(producer: Iterator[int]) -> None:
        for item in producer:
            full.append(item)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("early_consumer", fn=early_consumer),
            step("full_consumer", fn=full_consumer),
        ],
    )
    run(p, Count(count=5))

    assert early == [0]
    assert full == [0, 1, 2, 3, 4]


def test_given_two_lazy_deps_with_max_in_flight_when_unrolled_then_pairs_are_preserved():
    pairs: list[tuple[int, int]] = []

    def left(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def right(count: int) -> Generator[int, None, None]:
        yield from range(10, 10 + count)

    def join(left: int, right: int) -> None:
        pairs.append((left, right))

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("left", fn=left, max_in_flight=3),
            step("right", fn=right, max_in_flight=3),
            step("join", fn=join),
        ],
    )
    run(p, Count(count=5))

    assert pairs == [(0, 10), (1, 11), (2, 12), (3, 13), (4, 14)]


def test_given_flattening_stream_step_when_max_in_flight_2_then_internal_items_define_bound():
    log: list[str] = []
    seen: list[int] = []

    def source(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def flatten(source: Iterator[int]) -> Iterator[int]:
        for item in source:
            log.append(f"emit {item}")
            yield item
            log.append(f"emit {item + 100}")
            yield item + 100

    def consumer(flatten: Iterator[int]) -> None:
        for item in flatten:
            log.append(f"recv {item}")
            seen.append(item)
            sleep(0.01)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("source", fn=source),
            step("flatten", fn=flatten, max_in_flight=2),
            step("consumer", fn=consumer),
        ],
    )
    run(p, Count(count=3))

    assert seen == [0, 100, 1, 101, 2, 102]
    assert log.index("recv 0") < log.index("emit 1")


def test_given_fanout_lazy_and_eager_when_producer_stream_fails_then_pipeline_stops():
    lazy_seen: list[int] = []

    def producer(count: int) -> Generator[int, None, None]:
        yield 0
        yield 1
        raise ValueError("boom")

    def lazy_consumer(producer: Iterator[int]) -> None:
        for item in producer:
            lazy_seen.append(item)

    def eager_consumer(producer: list[int]) -> None:
        pass

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=3, on_error=OnError.STOP),
            step("lazy_consumer", fn=lazy_consumer, on_error=OnError.STOP),
            step("eager_consumer", fn=eager_consumer, on_error=OnError.STOP),
        ],
    )

    with pytest.raises(PipelineStopException):
        run(p, Count(count=5))

    assert lazy_seen == []


def test_given_threadpool_start_and_await_when_max_in_flight_5_then_only_five_tasks_start():
    class P(NamedTuple):
        count: int = 10

    pool = ThreadPoolExecutor(max_workers=10)
    release = threading.Event()
    started = threading.Event()
    lock = threading.Lock()
    futures: dict[int, Future] = {}
    in_flight = 0
    max_in_flight_seen = 0
    started_count = 0

    def numbers(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def work(value: int) -> int:
        nonlocal in_flight, max_in_flight_seen, started_count
        with lock:
            in_flight += 1
            started_count += 1
            max_in_flight_seen = max(max_in_flight_seen, in_flight)
            if started_count >= 5:
                started.set()
        release.wait()
        with lock:
            in_flight -= 1
        return value * 10

    def start(numbers: int) -> int:
        futures[numbers] = pool.submit(work, numbers)
        return numbers

    def await_result(start: Iterator[int]) -> list[int]:
        return [futures[token].result() for token in start]

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step("start", fn=start, max_in_flight=5),
            step("await_result", fn=await_result),
        ],
    )

    result_holder = {}
    error_holder = {}

    def target():
        try:
            result_holder["done"] = run(p, P(count=10))
        except BaseException as exc:  # pragma: no cover - assertion below
            error_holder["exc"] = exc

    thread = threading.Thread(target=target)
    thread.start()
    assert started.wait(timeout=5), "expected five tasks to start"
    sleep(0.1)
    with lock:
        assert started_count == 5
        assert max_in_flight_seen == 5
        assert in_flight == 5
    release.set()
    thread.join(timeout=5)
    pool.shutdown(wait=True)

    assert "exc" not in error_holder
    assert not thread.is_alive()


def test_runner_contract_uses_dag_node_max_in_flight_not_step_max_in_flight():
    produced: list[int] = []
    consumed: list[int] = []
    max_seen_ahead = 0

    def producer(count: int) -> Generator[int, None, None]:
        nonlocal max_seen_ahead
        for i in range(count):
            produced.append(i)
            max_seen_ahead = max(max_seen_ahead, len(produced) - len(consumed))
            yield i

    def consumer(producer: Iterator[int]) -> None:
        for item in producer:
            consumed.append(item)

    p = pipeline(
        name="test",
        params=Count,
        steps=[
            step("producer", fn=producer, max_in_flight=1),
            step("consumer", fn=consumer),
        ],
    )

    # Mutate Step.max_in_flight. The executor should ignore this and use DagNode.max_in_flight (1).
    p.steps[0].max_in_flight = 10

    run(p, Count(count=20))

    assert produced == list(range(20))
    assert consumed == list(range(20))
    assert max_seen_ahead <= 1


def test_given_multilevel_each_fanout_when_run_max_in_flight_1_then_completes(
    run_pipeline,
):
    class P(NamedTuple):
        pass

    def source() -> Iterator[int]:
        for i in range(20):
            yield i

    def l1a(source: int) -> int:
        return source

    def l1b(source: int) -> int:
        return source * 10

    def l1c(source: int) -> int:
        return source * 100

    seen_x: list[int] = []
    seen_y: list[int] = []

    def l2x(l1a: int) -> None:
        seen_x.append(l1a)

    def l2y(l1a: int) -> None:
        seen_y.append(l1a)

    my_pipeline = pipeline(
        name="test_multilevel_each_fanout_mif_1",
        params=P,
        steps=[
            step("source", fn=source, max_in_flight=1),
            step("l1a", fn=l1a, max_in_flight=1),
            step("l1b", fn=l1b, max_in_flight=1),
            step("l1c", fn=l1c, max_in_flight=1),
            step("l2x", fn=l2x, max_in_flight=1),
            step("l2y", fn=l2y, max_in_flight=1),
        ],
    )

    run_pipeline(my_pipeline, params=P())

    assert seen_x == list(range(20))
    assert seen_y == list(range(20))


def test_given_multilevel_each_fanout_when_run_max_in_flight_3_then_completes(
    run_pipeline,
):
    class P(NamedTuple):
        pass

    def source() -> Iterator[int]:
        for i in range(20):
            yield i

    def l1a(source: int) -> int:
        return source

    def l1b(source: int) -> int:
        return source * 10

    def l1c(source: int) -> int:
        return source * 100

    seen_x: list[int] = []
    seen_y: list[int] = []

    def l2x(l1a: int) -> None:
        seen_x.append(l1a)

    def l2y(l1a: int) -> None:
        seen_y.append(l1a)

    my_pipeline = pipeline(
        name="test_multilevel_each_fanout_mif_3",
        params=P,
        steps=[
            step("source", fn=source, max_in_flight=3),
            step("l1a", fn=l1a, max_in_flight=3),
            step("l1b", fn=l1b, max_in_flight=3),
            step("l1c", fn=l1c, max_in_flight=3),
            step("l2x", fn=l2x, max_in_flight=3),
            step("l2y", fn=l2y, max_in_flight=3),
        ],
    )

    run_pipeline(my_pipeline, params=P())

    assert seen_x == list(range(20))
    assert seen_y == list(range(20))
