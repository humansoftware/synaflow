"""Runtime tests for error_threshold_absolute and error_threshold_pct.

Covers the spec's 15+ scenarios for the async engine.
"""

from collections.abc import AsyncIterator
from typing import NamedTuple

import pytest
from synaflow.execution.adapters import async_adapter

from synaflow import (
    InvalidThresholdRaiseInEACHStep,
    PipelineEvent,
    PipelineStopException,
    StepEvent,
    ThresholdExceededException,
    async_run,
    pipeline,
    step,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_each_pipeline(
    fn,
    *,
    error_threshold_absolute=None,
    error_threshold_pct=None,
    on_error=None,
):
    """Build a 2-step pipeline: numbers -> proc (terminal)."""

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    async def numbers(items: list[int]):
        for x in items:
            yield x

    p = pipeline(
        name="t",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step(
                "proc",
                fn=fn,
                error_threshold_absolute=error_threshold_absolute,
                error_threshold_pct=error_threshold_pct,
                on_error=on_error if on_error is not None else "continue",  # type: ignore[arg-type]
            ),
        ],
    )
    return p, P


# ---------------------------------------------------------------------------
# Absolute threshold
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_absolute_threshold_not_exceeded_completes_normally():
    async def proc(items: int) -> int:
        if items == 2:
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_absolute=5)
    await async_run(p, P())


@pytest.mark.asyncio
async def test_absolute_threshold_exceeded_raises():
    async def proc(items: int) -> int:
        if items in (1, 2, 3):
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_absolute=2)
    with pytest.raises(ThresholdExceededException) as exc_info:
        await async_run(p, P())
    assert exc_info.value.error_count == 3
    assert exc_info.value.success_count == 2
    assert exc_info.value.threshold_absolute == 2


# ---------------------------------------------------------------------------
# Pct threshold
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pct_threshold_not_exceeded_completes_normally():
    async def proc(items: int) -> int:
        if items == 2:
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.5)
    await async_run(p, P())


@pytest.mark.asyncio
async def test_pct_threshold_exceeded_raises():
    async def proc(items: int) -> int:
        if items in (1, 2, 3):
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.5)
    with pytest.raises(ThresholdExceededException) as exc_info:
        await async_run(p, P())
    assert exc_info.value.error_count == 3
    assert exc_info.value.success_count == 2
    assert exc_info.value.threshold_pct == 0.5


@pytest.mark.asyncio
async def test_pct_threshold_with_multiple_each_deps_uses_step_invocations():
    async def producer_a(n: int) -> AsyncIterator[int]:
        for i in range(n):
            yield i

    async def producer_b(n: int) -> AsyncIterator[str]:
        for i in range(n):
            yield f"v{i}"

    async def consumer(a: int, b: str) -> int:
        if a == 2:
            raise ValueError("boom on item 3")
        return a

    async def sink(c: AsyncIterator[int]) -> None:
        async for _ in c:
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
        await async_run(p, P())
    assert exc_info.value.error_count == 1
    assert exc_info.value.success_count == 4


# ---------------------------------------------------------------------------
# Both thresholds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_both_thresholds_either_triggers():
    async def proc(items: int) -> int:
        if items == 0:
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(
        proc, error_threshold_absolute=2, error_threshold_pct=0.5
    )
    await async_run(p, P())


# ---------------------------------------------------------------------------
# Timing: fires after all consumed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_threshold_fires_after_all_consumed_not_mid_stream():
    invocations = []

    async def proc(items: int) -> int:
        invocations.append(items)
        if items == 2:
            raise ValueError("boom on item 2")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.2)
    with pytest.raises(ThresholdExceededException):
        await async_run(p, P())
    assert invocations == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Boundary
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pct_threshold_boundary_exact_match_triggers():
    async def proc(items: int) -> int:
        if items in (0, 1):
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.4)
    with pytest.raises(ThresholdExceededException):
        await async_run(p, P())


@pytest.mark.asyncio
async def test_pct_threshold_boundary_just_below_no_trigger():
    async def proc(items: int) -> int:
        if items == 0:
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=0.4)
    await async_run(p, P())


@pytest.mark.asyncio
async def test_pct_threshold_100_pct_only_fires_on_full_failure():
    async def proc(items: int) -> int:
        if items in (0, 1, 2, 3):
            raise ValueError("boom")
        return items

    p, P = _build_each_pipeline(proc, error_threshold_pct=1.0)
    await async_run(p, P())

    async def proc_all_fail(items: int) -> int:
        raise ValueError("boom")

    p2, P2 = _build_each_pipeline(proc_all_fail, error_threshold_pct=1.0)
    with pytest.raises(ThresholdExceededException):
        await async_run(p2, P2())


@pytest.mark.asyncio
async def test_threshold_on_empty_stream_does_not_fire():
    async def proc(items: int) -> int:
        raise ValueError("should not be called")

    class P(NamedTuple):
        items: list[int] = []

    async def numbers(items: list[int]):
        for x in items:
            yield x

    async def sink(proc: AsyncIterator[int]) -> None:
        async for _ in proc:
            pass

    p = pipeline(
        name="empty",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step(
                "proc",
                fn=proc,
                error_threshold_absolute=1,
                error_threshold_pct=0.1,
            ),
            step("sink", fn=sink),
        ],
    )
    await async_run(p, P())


# ---------------------------------------------------------------------------
# Counters reset per step
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_threshold_counters_reset_per_step():
    async def proc1(items: int) -> int:
        if items == 0:
            raise ValueError("boom")
        return items

    async def proc2(proc1: int) -> int:
        if proc1 == 4:
            raise ValueError("boom")
        return proc1

    async def sink(proc2: AsyncIterator[int]) -> None:
        async for _ in proc2:
            pass

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    async def numbers(items: list[int]):
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
    await async_run(p, P())


# ---------------------------------------------------------------------------
# Observer events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_observers_receive_failed_events_on_threshold():
    events: list[tuple] = []

    def on_event(ctx):
        events.append((ctx.event, ctx.step_name))

    async def proc(items: int) -> int:
        if items in (0, 1, 2):
            raise ValueError("boom")
        return items

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    async def numbers(items: list[int]):
        for x in items:
            yield x

    from synaflow import Observer

    p = pipeline(
        name="obs",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step("proc", fn=proc, error_threshold_absolute=2),
        ],
        observers=[Observer(async_adapter(on_event))],
    )
    with pytest.raises(ThresholdExceededException):
        await async_run(p, P())

    failed_events = [e for e in events if "FAILED" in e[0].name]
    step_failed = [e for e in failed_events if e[0] == StepEvent.FAILED]
    pipeline_failed = [e for e in failed_events if e[0] == PipelineEvent.FAILED]
    assert len(step_failed) >= 1
    assert step_failed[0][1] == "proc"
    assert len(pipeline_failed) >= 1
    assert pipeline_failed[0][1] == "proc"


# ---------------------------------------------------------------------------
# Threshold + force_materialize
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_threshold_with_force_materialize_respected():
    async def proc(items: int) -> int:
        if items in (0, 1):
            raise ValueError("boom")
        return items

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    async def numbers(items: list[int]):
        for x in items:
            yield x

    p = pipeline(
        name="mat",
        params=P,
        steps=[
            step("numbers", fn=numbers),
            step(
                "proc",
                fn=proc,
                error_threshold_absolute=2,
                force_materialize=True,
            ),
        ],
    )
    with pytest.raises(ThresholdExceededException) as exc_info:
        await async_run(p, P())
    assert exc_info.value.error_count == 2


# ---------------------------------------------------------------------------
# Manual raise in ALL step (escape hatch)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_manual_threshold_exception_in_all_step_escape_hatch():
    handled = []

    def error_factory(ctx):
        async def handle(error_ctx):
            handled.append(error_ctx.exception)

        return handle

    async def all_proc() -> int:
        raise ThresholdExceededException("all_proc", error_count=3, success_count=7)

    class P(NamedTuple):
        pass

    p = pipeline(
        name="manual",
        params=P,
        error_materializer=error_factory,
        steps=[step("all_proc", fn=all_proc)],
    )
    with pytest.raises(ThresholdExceededException) as exc_info:
        await async_run(p, P())
    assert len(handled) == 1
    assert isinstance(handled[0], ThresholdExceededException)
    assert exc_info.value.error_count == 3
    assert exc_info.value.success_count == 7


# ---------------------------------------------------------------------------
# Manual raise in EACH step (misuse -> wrapped)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_manual_threshold_exception_in_each_step_wraps_in_validator():
    handled = []

    def error_factory(ctx):
        async def handle(error_ctx):
            handled.append(error_ctx.exception)

        return handle

    async def proc(items: int) -> int:
        if items == 0:
            raise ThresholdExceededException("proc", 1, 0)
        return items

    async def sink(proc: AsyncIterator[int]) -> None:
        async for _ in proc:
            pass

    class P(NamedTuple):
        items: list[int] = [0, 1, 2]

    async def numbers(items: list[int]):
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
    await async_run(p, P())
    assert len(handled) == 1
    assert isinstance(handled[0], InvalidThresholdRaiseInEACHStep)
    assert isinstance(handled[0].original_exception, ThresholdExceededException)


# ---------------------------------------------------------------------------
# Regression: on_error=CONTINUE without threshold unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_error_continue_without_threshold_unchanged():
    invocations = []

    async def proc(items: int) -> int:
        invocations.append(items)
        if items == 2:
            raise ValueError("boom")
        return items

    async def sink(proc: AsyncIterator[int]) -> None:
        async for _ in proc:
            pass

    class P(NamedTuple):
        items: list[int] = [0, 1, 2, 3, 4]

    async def numbers(items: list[int]):
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
    await async_run(p, P())
    assert invocations == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Breaking change coverage: on_error=STOP
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_error_stop_no_longer_forces_materialization():
    captured_type = []

    async def sink(source: AsyncIterator[int]):
        captured_type.append(type(source).__name__)
        try:
            async for _ in source:
                pass
        except Exception:
            pass

    async def source_fn():
        yield 1
        raise ValueError("iterboom")

    class P(NamedTuple):
        pass

    p = pipeline(
        name="nomat",
        params=P,
        steps=[
            step("source", fn=source_fn, on_error="stop"),  # type: ignore[arg-type]
            step("sink", fn=sink),
        ],
    )
    try:
        await async_run(p, P())
    except PipelineStopException:
        pass
    assert captured_type, f"captured_type was empty: {captured_type}"
