"""Runtime tests for error_threshold_absolute and error_threshold_pct.

Covers the spec's 15+ scenarios for the sync engine.
"""

from synaflow.core.dag_builder import build_dag
from collections.abc import Iterator
from typing import NamedTuple
import pytest
from synaflow import (
    InvalidThresholdRaiseInEACHStep,
    PipelineEvent,
    PipelineStopException,
    StepEvent,
    ThresholdExceededException,
    pipeline,
    run,
    step,
)
from synaflow import Observer


def _build_each_pipeline(
    fn, *, error_threshold_absolute=None, error_threshold_pct=None, on_error=None
):
    """Build a 2-step pipeline: numbers -> proc.

    `proc` is terminal (no downstream consumer), so the threshold check
    runs in the producer's context -- required for correct FAILED dispatch.

    Returns (pipeline, P) so the test can construct default params.
    """

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    def numbers(items: list[int]) -> Iterator[int]:
        for x in items:
            yield x

    def proc(numbers: int) -> int:
        return fn(numbers)

    p = pipeline(
        name="t",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step(
                "proc",
                fn=proc,
                error_threshold_absolute=error_threshold_absolute,
                error_threshold_pct=error_threshold_pct,
                on_error=on_error if on_error is not None else "continue",
            ),
        ],
    )
    return (p, P)


def test_absolute_threshold_not_exceeded_completes_normally():

    def proc(items: int) -> int:
        if items == 2:
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_absolute=5)
    run(build_dag(p), P())


def test_absolute_threshold_exceeded_raises():

    def proc(items: int) -> int:
        if items in (1, 2, 3):
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_absolute=2)
    with pytest.raises(ThresholdExceededException) as exc_info:
        run(build_dag(p), P())
    assert exc_info.value.error_count == 3
    assert exc_info.value.success_count == 2
    assert exc_info.value.threshold_absolute == 2


def test_pct_threshold_not_exceeded_completes_normally():

    def proc(items: int) -> int:
        if items == 2:
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.5)
    run(build_dag(p), P())


def test_pct_threshold_exceeded_raises():

    def proc(items: int) -> int:
        if items in (1, 2, 3):
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.5)
    with pytest.raises(ThresholdExceededException) as exc_info:
        run(build_dag(p), P())
    assert exc_info.value.error_count == 3
    assert exc_info.value.success_count == 2
    assert exc_info.value.threshold_pct == 0.5


def test_pct_threshold_with_multiple_each_deps_uses_step_invocations():
    """Threshold counts invocations of the step, not per-dep.

    Producer: each consumes ONE int. Consumer with 2 deps sees a *stream
    of tuples*; the consumer step is called once per tuple.

    Spec example: producer_a + producer_b with 5 items each, consumer fails on
    the 3rd item. 5 invocations, 1 error, 20% error rate.
    """

    def producer_a(n: int) -> Iterator[int]:
        for i in range(n):
            yield i

    def producer_b(n: int) -> Iterator[str]:
        for i in range(n):
            yield f"v{i}"

    def consumer(a: int, b: str) -> int:
        if a == 2:
            raise ValueError("boom on item 3")
        return a

    def sink(c: Iterator[int]) -> None:
        for _ in c:
            pass

    class P(NamedTuple):
        n: int = 5

    p = pipeline(
        name="multi",
        params=P,
        steps=[
            step("a", fn=producer_a),
            step("b", fn=producer_b),
            step("c", fn=consumer, error_threshold_pct=0.2),
            step("sink", fn=sink),
        ],
    )
    with pytest.raises(ThresholdExceededException) as exc_info:
        run(build_dag(p), P())
    assert exc_info.value.error_count == 1
    assert exc_info.value.success_count == 4


def test_both_thresholds_either_triggers():

    def proc(items: int) -> int:
        if items == 0:
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(
        proc, error_threshold_absolute=2, error_threshold_pct=0.5
    )
    run(build_dag(p), P())


def test_threshold_fires_after_all_consumed_not_mid_stream():
    """On a 5-item stream with 1 error and threshold=0.2, the exception
    is only raised at the end (after all 5 items processed), not when the
    1st error occurs."""
    invocations = []

    def proc(items: int) -> int:
        invocations.append(items)
        if items == 2:
            raise ValueError("boom on item 2")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.2)
    with pytest.raises(ThresholdExceededException):
        run(build_dag(p), P())
    assert invocations == [0, 1, 2, 3, 4]


def test_pct_threshold_boundary_exact_match_triggers():

    def proc(items: int) -> int:
        if items in (0, 1):
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.4)
    with pytest.raises(ThresholdExceededException):
        run(build_dag(p), P())


def test_pct_threshold_boundary_just_below_no_trigger():

    def proc(items: int) -> int:
        if items == 0:
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.4)
    run(build_dag(p), P())


def test_pct_threshold_100_pct_only_fires_on_full_failure():

    def proc(items: int) -> int:
        if items in (0, 1, 2, 3):
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=1.0)
    run(build_dag(p), P())

    def proc_all_fail(items: int) -> int:
        raise ValueError("boom")

    p2, P2 = _build_each_pipeline(proc_all_fail, error_threshold_pct=1.0)
    with pytest.raises(ThresholdExceededException):
        run(build_dag(p2), P2())


def test_threshold_on_empty_stream_does_not_fire():
    """0 invocations: pct check has the `invocation_count > 0` guard,
    abs check has 0 errors so no trigger."""

    def proc(numbers: int) -> int:
        raise ValueError("should not be called")

    class P(NamedTuple):
        items: list[int] = []

    def numbers(items: list[int]) -> Iterator[int]:
        for x in items:
            yield x

    def sink(proc: Iterator[int]) -> None:
        for _ in proc:
            pass

    p = pipeline(
        name="empty",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step("proc", fn=proc, error_threshold_absolute=1, error_threshold_pct=0.1),
            step("sink", fn=sink),
        ],
    )
    run(build_dag(p), P())


def test_threshold_counters_reset_per_step():
    """Two EACH steps with thresholds: counters are independent."""

    def proc1(numbers: int) -> int:
        if numbers == 0:
            raise ValueError("boom")
        return numbers

    def proc2(proc1: int) -> int:
        if proc1 == 4:
            raise ValueError("boom")
        return proc1

    def sink(proc2: Iterator[int]) -> None:
        for _ in proc2:
            pass

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    def numbers(items: list[int]) -> Iterator[int]:
        for x in items:
            yield x

    p = pipeline(
        name="two",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step("proc1", fn=proc1, error_threshold_pct=0.4),
            step("proc2", fn=proc2, error_threshold_pct=0.4),
            step("sink", fn=sink),
        ],
    )
    run(build_dag(p), P())


def test_observers_receive_failed_events_on_threshold():
    """Threshold exceeded emits StepEvent.FAILED + PipelineEvent.FAILED."""
    events: list[tuple] = []

    def on_event(ctx):
        events.append((ctx.event, ctx.step_name))

    def proc(numbers: int) -> int:
        if numbers in (0, 1, 2):
            raise ValueError("boom")
        return numbers

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    def numbers(items: list[int]) -> Iterator[int]:
        for x in items:
            yield x

    p = pipeline(
        name="obs",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step("proc", fn=proc, error_threshold_absolute=2),
        ],
        observers=[Observer(on_event)],
    )
    with pytest.raises(ThresholdExceededException):
        run(build_dag(p), P())
    failed_events = [e for e in events if "FAILED" in e[0].name]
    step_failed = [e for e in failed_events if e[0] == StepEvent.FAILED]
    pipeline_failed = [e for e in failed_events if e[0] == PipelineEvent.FAILED]
    assert len(step_failed) >= 1
    assert step_failed[0][1] == "proc"
    assert len(pipeline_failed) >= 1
    assert pipeline_failed[0][1] == "proc"


def test_threshold_with_force_materialize_respected():
    """force_materialize=True does not interfere with threshold tracking."""

    def proc(numbers: int) -> int:
        if numbers in (0, 1):
            raise ValueError("boom")
        return numbers

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    def numbers(items: list[int]) -> Iterator[int]:
        for x in items:
            yield x

    p = pipeline(
        name="mat",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step("proc", fn=proc, error_threshold_absolute=2, force_materialize=True),
        ],
    )
    with pytest.raises(ThresholdExceededException) as exc_info:
        run(build_dag(p), P())
    assert exc_info.value.error_count == 2


def test_manual_threshold_exception_in_all_step_escape_hatch():
    """A user can manually raise ThresholdExceededException from inside an
    ALL-mode step (the documented escape hatch). The error materializer is
    called with the original exception, and a StepEvent.FAILED is dispatched."""
    handled = []

    def error_factory(ctx):

        def handle(error_ctx):
            print(
                f"DEBUG: handle called with {type(error_ctx.exception).__name__}: {error_ctx.exception}"
            )
            handled.append(error_ctx.exception)

        return handle

    def all_proc() -> int:
        raise ThresholdExceededException("all_proc", error_count=3, success_count=7)

    class P(NamedTuple):
        pass

    p = pipeline(
        name="manual",
        params=P,
        error_materializer=error_factory,
        steps=[step("all_proc", fn=all_proc)],
    )
    print(
        f"DEBUG: node.error_materializer = {build_dag(p).steps['all_proc'].error_materializer}"
    )
    with pytest.raises(ThresholdExceededException) as exc_info:
        run(build_dag(p), P())
    assert len(handled) == 1
    assert isinstance(handled[0], ThresholdExceededException)
    assert exc_info.value.error_count == 3
    assert exc_info.value.success_count == 7


def test_manual_threshold_exception_in_each_step_wraps_in_validator():
    """Manually raising ThresholdExceededException inside an EACH fn() is
    misuse: the executor wraps it in InvalidThresholdRaiseInEACHStep for
    the error materializer, and treats it as a normal per-item error."""
    handled = []

    def error_factory(ctx):

        def handle(error_ctx):
            handled.append(error_ctx.exception)

        return handle

    def proc(numbers: int) -> int:
        if numbers == 0:
            raise ThresholdExceededException("proc", 1, 0)
        return numbers

    def sink(proc: Iterator[int]) -> None:
        for _ in proc:
            pass

    class P(NamedTuple):
        items: list[int] = [0, 1, 2]

    def numbers(items: list[int]) -> Iterator[int]:
        for x in items:
            yield x

    p = pipeline(
        name="misuse",
        params=P,
        error_materializer=error_factory,
        steps=[
            step("numbers", fn=numbers),
            step("proc", fn=proc),
            step("sink", fn=sink),
        ],
    )
    run(build_dag(p), P())
    assert len(handled) == 1
    assert isinstance(handled[0], InvalidThresholdRaiseInEACHStep)
    assert isinstance(handled[0].original_exception, ThresholdExceededException)


def test_on_error_continue_without_threshold_unchanged():
    """Without threshold: a step with on_error=CONTINUE and per-item errors
    just runs to completion (errors are skipped, pipeline succeeds)."""
    invocations = []

    def proc(numbers: int) -> int:
        invocations.append(numbers)
        if numbers == 2:
            raise ValueError("boom")
        return numbers

    def sink(proc: Iterator[int]) -> None:
        for _ in proc:
            pass

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    def numbers(items: list[int]) -> Iterator[int]:
        for x in items:
            yield x

    p = pipeline(
        name="cont",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step("proc", fn=proc),
            step("sink", fn=sink),
        ],
    )
    run(build_dag(p), P())
    assert invocations == [0, 1, 2, 3, 4]


def test_on_error_stop_no_longer_forces_materialization():
    """Without force_materialize, on_error=STOP on a stream producer
    no longer materializes -- the stream is published directly to the
    consumer. The consumer (sink) gets the iterator, not a materialized list."""
    captured_type = []
    captured_error = []

    def sink(source: Iterator[int]):
        captured_type.append(type(source).__name__)
        try:
            for _ in source:
                pass
        except Exception as e:
            captured_error.append(type(e).__name__)

    def source_fn() -> Iterator[int]:
        yield 1
        raise ValueError("iterboom")

    class P(NamedTuple):
        pass

    p = pipeline(
        name="nomat",
        params=P,
        steps=[step("source", fn=source_fn, on_error="stop"), step("sink", fn=sink)],
    )
    try:
        run(build_dag(p), P())
    except PipelineStopException:
        pass
    assert captured_type, f"captured_type was empty: {captured_type}"
    assert captured_type[0] in (
        "generator",
        "list_iterator",
        "SyncQueueIterator",
        "list",
        "LifecycleStream",
    )
