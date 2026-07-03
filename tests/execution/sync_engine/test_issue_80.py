from typing import Iterator, NamedTuple
import pytest
from synaflow import pipeline, step, run
from synaflow.core.observers import StepCompletedContext, Observer

class DummyParams(NamedTuple):
    pass

def test_observer_success_count_reflects_list_length_issue_80():
    def producer() -> Iterator[int]:
        yield 1
        yield 2
        yield 3

    def consumer(prod: list[int]) -> list[int]:
        return prod

    class EventRecorder:
        def __init__(self):
            self.events = []
        def record(self, event):
            self.events.append(event)

    rec = EventRecorder()

    p = pipeline(
        name="test_p",
        params=DummyParams,
        steps=[
            step("prod", fn=producer),
            step("cons", fn=consumer)
        ],
        observers=[Observer(rec.record)]
    )

    run(p, params=DummyParams())

    cons_event = next(e for e in rec.events if isinstance(e, StepCompletedContext) and getattr(e, "step_name", None) == "cons")
    assert cons_event.success_count == 3
