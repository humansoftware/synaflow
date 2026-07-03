from itertools import tee
from typing import NamedTuple, Iterator
import pytest
import time

from synaflow import pipeline, step
from synaflow.core.types import StepMode


@pytest.mark.skip(reason="We are fixing the engine now")
def test_itertools_tee_concurrent_reentry_crash(run_pipeline):
    class P(NamedTuple):
        pass

    def missing_summaries() -> Iterator[int]:
        for i in range(5):
            time.sleep(0.01)
            yield i

    def report_extracts(missing: int) -> int:
        time.sleep(0.01)
        return missing * 10

    def generated_summaries(missing: int, report: int) -> int:
        return report + missing

    call_counts = {"file": 0, "db": 0}

    def summary_file(missing: int, gen: int) -> None:
        call_counts["file"] += 1

    def db_summary(missing: int, gen: int) -> None:
        call_counts["db"] += 1

    my_pipeline = pipeline(
        name="test_tee_crash",
        params=P,
        steps=[
            step("missing", fn=missing_summaries, mode=StepMode.ALL),
            step("report", fn=report_extracts, mode=StepMode.EACH),
            step("gen", fn=generated_summaries, mode=StepMode.EACH),
            step("file", fn=summary_file, mode=StepMode.EACH),
            step("db", fn=db_summary, mode=StepMode.EACH),
        ],
    )

    run_pipeline(my_pipeline, params=P())

    # Each terminal consumer should receive 5 items if the stream didn't crash
    assert call_counts["file"] == 5, (
        f"Expected 5 calls to file, got {call_counts['file']}"
    )
    assert call_counts["db"] == 5, f"Expected 5 calls to db, got {call_counts['db']}"


def test_marcelo():

    def fibonacci_stream(n: int) -> Iterator[int]:
        """Retorna uma stream com os N primeiros números de Fibonacci."""
        a, b = 0, 1
        for _ in range(n):
            yield a
            a, b = b, a + b

    fib = fibonacci_stream(10)
    c1, c2, c3, c4 = tee(fib, 4)
    print(next(c1))
    print(next(c1))
    print(next(c2))
    print(next(c3))
    print(next(c4))
    print(next(c1))
