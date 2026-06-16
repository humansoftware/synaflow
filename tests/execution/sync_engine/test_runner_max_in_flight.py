import pytest
from typing import NamedTuple
from collections.abc import Iterator
from synaflow import pipeline, step
from synaflow.core.types import StepMode, OnError
from synaflow.execution.sync_engine.executor import run


def test_sync_single_consumer_max_in_flight():
    log = []

    def producer() -> Iterator[int]:
        for i in range(4):
            log.append(f"prod {i}")
            yield i

    def consumer(producer: int) -> None:
        log.append(f"cons {producer}")

    class Params(NamedTuple):
        pass

    p = pipeline(
        name="test",
        params=Params,
        steps=[
            step("producer", fn=producer, max_in_flight=2),
            step("consumer", fn=consumer, mode=StepMode.EACH),
        ],
    )

    run(p, params=Params())

    # init pulls 2 items: prod 0, prod 1
    # next() pops 0, fills: prod 2
    # runs: cons 0
    # next() pops 1, fills: prod 3
    # runs: cons 1
    # next() pops 2, source exhausted
    # runs: cons 2
    # next() pops 3, exhausted
    # runs: cons 3
    expected = [
        "prod 0",
        "prod 1",
        "prod 2",
        "cons 0",
        "prod 3",
        "cons 1",
        "cons 2",
        "cons 3",
    ]
    assert log == expected


def test_sync_fan_out_exceeds_max_in_flight():
    log = []

    def producer() -> Iterator[int]:
        for i in range(5):
            log.append(f"prod {i}")
            yield i

    def fast_consumer(producer: int) -> None:
        log.append(f"fast {producer}")

    def slow_consumer(producer: int) -> None:
        log.append(f"slow {producer}")

    class Params(NamedTuple):
        pass

    p = pipeline(
        name="test",
        params=Params,
        steps=[
            step("producer", fn=producer, max_in_flight=2),
            step(
                "fast_consumer",
                fn=fast_consumer,
                mode=StepMode.EACH,
                on_error=OnError.STOP,
            ),
            step(
                "slow_consumer",
                fn=slow_consumer,
                mode=StepMode.EACH,
                on_error=OnError.STOP,
            ),
        ],
    )

    run(p, params=Params())
    
    # Should complete without error because branch-local max_in_flight 
    # handles pulling successfully without cross-branch hard failures
    assert len(log) == 15
    # Since it runs sequentially per level, fast_consumer will pull everything first.
    # The BoundedStreamWrapper ensures it pulls in chunks of 2, 
    # but since fast_consumer keeps asking for more, the producer generates all 5 items.
    assert "fast 4" in log
    assert "slow 4" in log
