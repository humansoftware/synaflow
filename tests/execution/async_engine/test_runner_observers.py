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


async def async_dummy_step(count: int) -> int:
    return count * 2


async def async_dummy_step_add(count: int) -> int:
    return count + 10


async def async_dummy_step_mul(count: int) -> int:
    return count * 3


async def async_dummy_step_list(count: int) -> list[int]:
    return list(range(count))


def test_given_pipeline_when_runs_then_emits_pipeline_started_completed():
    events = []

    async def on_event(ctx):
        if ctx.event == PipelineEvent.STARTED:
            events.append(("pipeline_started", ctx.pipeline_name))
        elif ctx.event == PipelineEvent.COMPLETED:
            events.append(("pipeline_completed", ctx.pipeline_name))

    p = pipeline(
        name="p1",
        params=ObserversParams,
        observers=[Observer(on_event)],
        steps=[step("s1", fn=async_dummy_step)],
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

    async def on_event(ctx):
        if ctx.event == PipelineEvent.FAILED:
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
        observers=[Observer(on_event)],
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

    async def on_event(ctx):
        if ctx.event == StepEvent.STARTED:
            events.append(
                ("started", ctx.step_name, ctx.mode.value, ctx.on_error.value)
            )
        elif ctx.event == StepEvent.COMPLETED:
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
        observers=[Observer(on_event)],
        steps=[step("s1", fn=async_dummy_step_add)],
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

    async def on_event(ctx):
        if ctx.event == StepEvent.COMPLETED:
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
        observers=[Observer(on_event)],
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

    async def on_event(ctx):
        if ctx.event == StepEvent.FAILED:
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
        observers=[Observer(on_event)],
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

    async def on_event(ctx):
        if ctx.event == MaterializationEvent.STARTED:
            events.append(("started", ctx.step_name, ctx.materializer_name))
        elif ctx.event == MaterializationEvent.COMPLETED:
            events.append(("completed", ctx.step_name, ctx.materializer_name))

    p = pipeline(
        name="p_mat",
        params=ObserversParams,
        observers=[Observer(on_event)],
        steps=[
            step(
                "s1",
                fn=async_dummy_step_list,
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
        observers=[Observer(bad_handler)],
        steps=[step("s1", fn=async_dummy_step_mul)],
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

    async def async_on_event(ctx):
        if ctx.event == PipelineEvent.STARTED:
            await asyncio.sleep(0.001)
            events.append(("async_started", ctx.pipeline_name))

    p = pipeline(
        name="async_p1",
        params=ObserversParams,
        observers=[Observer(async_on_event)],
        steps=[step("s1", fn=async_dummy_step)],
    )

    async def main():
        await async_run(p, ObserversParams(count=5))

    asyncio.run(main())

    assert events == [
        ("async_started", "async_p1"),
    ]
