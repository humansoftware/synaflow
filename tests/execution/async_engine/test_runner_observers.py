import asyncio
import logging
import pytest
from typing import NamedTuple, AsyncGenerator

from synaflow import (
    pipeline,
    step,
    async_run,
    OnError,
    PipelineEvent,
    StepEvent,
    MaterializationEvent,
    Observer,
)
from synaflow.core.exceptions import PipelineStopException


class ObserversParams(NamedTuple):
    count: int = 3


def test_given_pipeline_when_runs_then_emits_pipeline_started_completed():
    events = []

    async def on_started(ctx):
        events.append(("pipeline_started", ctx.pipeline_name))

    async def on_completed(ctx):
        events.append(("pipeline_completed", ctx.pipeline_name))

    p = pipeline(
        name="p1",
        params=ObserversParams,
        observers=[
            Observer(PipelineEvent.STARTED, on_started),
            Observer(PipelineEvent.COMPLETED, on_completed),
        ],
        steps=[step("s1", fn=lambda count: count * 2)],
    )

    async def main():
        await async_run(p, ObserversParams(count=5))

    asyncio.run(main())

    assert events == [
        ("pipeline_started", "p1"),
        ("pipeline_completed", "p1"),
    ]


def test_given_failing_pipeline_when_runs_then_emits_pipeline_failed():
    events = []

    async def on_failed(ctx):
        events.append(
            (
                "pipeline_failed",
                ctx.pipeline_name,
                ctx.step_name,
                type(ctx.exception).__name__,
            )
        )

    async def fail_step(count: int) -> int:
        raise ValueError("Oops")

    p = pipeline(
        name="p_fail",
        params=ObserversParams,
        observers=[Observer(PipelineEvent.FAILED, on_failed)],
        steps=[step("s1", fn=fail_step, on_error=OnError.STOP)],
    )

    async def main():
        with pytest.raises(PipelineStopException) as excinfo:
            await async_run(p, ObserversParams(count=5))
        assert "s1" in str(excinfo.value)

    asyncio.run(main())

    assert events == [
        ("pipeline_failed", "p_fail", "s1", "ValueError"),
    ]


def test_given_all_mode_step_when_runs_then_emits_step_started_completed():
    events = []

    async def on_step_started(ctx):
        events.append(("started", ctx.step_name, ctx.mode.value, ctx.on_error.value))

    async def on_step_completed(ctx):
        events.append(
            (
                "completed",
                ctx.step_name,
                ctx.success_count,
                ctx.error_count,
                ctx.completed_all_inputs,
            )
        )

    p = pipeline(
        name="p_step",
        params=ObserversParams,
        observers=[
            Observer(StepEvent.STARTED, on_step_started),
            Observer(StepEvent.COMPLETED, on_step_completed),
        ],
        steps=[step("s1", fn=lambda count: count + 10)],
    )

    async def main():
        await async_run(p, ObserversParams(count=2))

    asyncio.run(main())

    assert events == [
        ("started", "s1", "all", "continue"),
        ("completed", "s1", 1, 0, True),
    ]


def test_given_each_mode_step_with_continue_when_runs_then_emits_completed_with_counts():
    events = []

    async def on_completed(ctx):
        events.append(
            (
                ctx.step_name,
                ctx.success_count,
                ctx.error_count,
                ctx.completed_all_inputs,
            )
        )

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def process(gen: int) -> int:
        if gen == 1:
            raise ValueError("Failure on 1")
        return gen * 10

    p = pipeline(
        name="p_each_continue",
        params=ObserversParams,
        observers=[Observer(StepEvent.COMPLETED, on_completed)],
        steps=[
            step("gen", fn=gen),
            step("process", fn=process, on_error=OnError.CONTINUE),
        ],
    )

    async def main():
        await async_run(p, ObserversParams(count=3))

    asyncio.run(main())

    assert len(events) == 2
    gen_completed = [ev for ev in events if ev[0] == "gen"][0]
    proc_completed = [ev for ev in events if ev[0] == "process"][0]

    assert gen_completed == ("gen", 1, 0, True)
    assert proc_completed == ("process", 2, 1, True)


def test_given_each_mode_step_with_stop_when_runs_then_emits_failed_with_counts():
    events = []

    async def on_failed(ctx):
        events.append(
            (
                ctx.step_name,
                ctx.success_count,
                ctx.error_count,
                ctx.completed_all_inputs,
                type(ctx.exception).__name__,
            )
        )

    async def gen(count: int) -> AsyncGenerator[int, None]:
        yield 0
        yield 1
        yield 2

    async def process(gen: int) -> int:
        if gen == 1:
            raise ValueError("Failure on 1")
        return gen * 10

    p = pipeline(
        name="p_each_stop",
        params=ObserversParams,
        observers=[Observer(StepEvent.FAILED, on_failed)],
        steps=[
            step("gen", fn=gen),
            step("process", fn=process, on_error=OnError.STOP),
        ],
    )

    async def main():
        with pytest.raises(PipelineStopException):
            await async_run(p, ObserversParams(count=3))

    asyncio.run(main())

    assert len(events) == 1
    assert events[0] == ("process", 1, 1, False, "ValueError")


def test_given_materializer_when_runs_then_emits_materialization_events():
    events = []

    async def on_mat_started(ctx):
        events.append(("started", ctx.step_name, ctx.materializer_name))

    async def on_mat_completed(ctx):
        events.append(("completed", ctx.step_name, ctx.materializer_name))

    p = pipeline(
        name="p_mat",
        params=ObserversParams,
        observers=[
            Observer(MaterializationEvent.STARTED, on_mat_started),
            Observer(MaterializationEvent.COMPLETED, on_mat_completed),
        ],
        steps=[
            step(
                "s1",
                fn=lambda count: list(range(count)),
                force_materialize=True,
            )
        ],
    )

    async def main():
        await async_run(p, ObserversParams(count=2))

    asyncio.run(main())

    assert len(events) == 2
    assert events[0][0] == "started"
    assert events[0][1] == "s1"
    assert events[1][0] == "completed"
    assert events[1][1] == "s1"


def test_given_failing_observer_when_runs_then_swallows_exception_and_logs(
    caplog,
):
    async def bad_handler(ctx):
        raise RuntimeError("I am bad")

    p = pipeline(
        name="p_bad_obs",
        params=ObserversParams,
        observers=[Observer(PipelineEvent.STARTED, bad_handler)],
        steps=[step("s1", fn=lambda count: count * 3)],
    )

    async def main():
        with caplog.at_level(logging.WARNING, logger="synaflow"):
            await async_run(p, ObserversParams(count=2))

    asyncio.run(main())

    assert any(
        "Observer failed for event" in record.message for record in caplog.records
    )


def test_given_async_observer_handler_when_runs_then_awaited():
    events = []

    async def async_on_started(ctx):
        await asyncio.sleep(0.001)
        events.append(("async_started", ctx.pipeline_name))

    p = pipeline(
        name="async_p1",
        params=ObserversParams,
        observers=[Observer(PipelineEvent.STARTED, async_on_started)],
        steps=[step("s1", fn=lambda count: count * 2)],
    )

    async def main():
        await async_run(p, ObserversParams(count=5))

    asyncio.run(main())

    assert events == [
        ("async_started", "async_p1"),
    ]
