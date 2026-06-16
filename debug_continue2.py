import logging
logging.basicConfig(level=logging.DEBUG)
from collections.abc import Iterator
from typing import NamedTuple
from synaflow import pipeline, step
from synaflow.core.types import StepMode, OnError
from synaflow.execution.sync_engine.executor import run

def producer() -> Iterator[int]:
    print("producer yielding 1")
    yield 1
    print("producer yielding 2")
    yield 2
    print("producer yielding 3")
    yield 3
    yield 4
    yield 5

def fast_consumer(producer: int) -> None:
    print(f"fast {producer}")

def slow_consumer(producer: int) -> None:
    print(f"slow {producer}")

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
    import traceback
    traceback.print_exc()
    print(f"SUCCESS: Raised {type(e).__name__}: {e}")
