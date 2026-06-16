from collections.abc import Iterator
from typing import NamedTuple
from synaflow import pipeline, step
from synaflow.core.types import StepMode, OnError
from synaflow.execution.sync_engine.executor import run

def producer() -> Iterator[int]:
    yield 1
    yield 2
    yield 3

def fast_consumer(producer: int) -> None:
    pass

def slow_consumer(producer: int) -> None:
    pass

class Params(NamedTuple):
    pass

p = pipeline(
    name="test",
    params=Params,
    steps=[
        step("producer", fn=producer, max_in_flight=2),
        step("fast_consumer", fn=fast_consumer, mode=StepMode.EACH, on_error=OnError.CONTINUE),
        step("slow_consumer", fn=slow_consumer, mode=StepMode.EACH, on_error=OnError.CONTINUE),
    ],
)

try:
    run(p, params=Params())
    print("FAILED: Did not raise exception!")
except Exception as e:
    print(f"SUCCESS: Raised {type(e).__name__}: {e}")
