import functools
import logging
from collections.abc import Iterator as Iter
from typing import Iterator, NamedTuple

import pytest

from synaflow import (
    MaterializationEvent,
    Observer,
    PipelineEvent,
    StepEvent,
    async_run,
    pipeline,
    step,
)
from synaflow.core.observers import (
    MaterializationStartedContext,
    PipelineFailedContext,
    StepCompletedContext,
    StepFailedContext,
)
from synaflow.core.types import OnError, StepMode
from synaflow.execution.async_engine.executor import AsyncPipelineExecutor


class Params(NamedTuple):
    values: list[int]


# ---------------------------------------------------------------------------
# Helper: observer that records all events
# ---------------------------------------------------------------------------


class EventRecorder:
    def __init__(self):
        self.events: list[tuple] = []

    def record(self, ctx):
        self.events.append((type(ctx).__name__, ctx))

    async def async_record(self, ctx):
        self.events.append((type(ctx).__name__, ctx))


# ---------------------------------------------------------------------------
# Pipeline events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_given_pipeline_observer_when_run_completes_then_started_and_completed_emitted():
    rec = EventRecorder()

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def consumer(gen: Iterator[int]) -> list[int]:
        return list(gen)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen),
            step("consumer", fn=consumer),
        ],
        observers=[
            Observer(PipelineEvent.STARTED, rec.record),
            Observer(PipelineEvent.COMPLETED, rec.record),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2]))

    names = [e[0] for e in rec.events]
    assert "PipelineStartedContext" in names
    assert "PipelineCompletedContext" in names
    assert names.index("PipelineStartedContext") < names.index(
        "PipelineCompletedContext"
    )
    assert "PipelineFailedContext" not in names


@pytest.mark.asyncio
async def test_given_pipeline_observer_when_step_fails_stop_then_failed_emitted():
    rec = EventRecorder()

    def failing(values: list[int]) -> int:
        raise ValueError("boom")

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("failing", fn=failing, on_error=OnError.STOP)],
        observers=[Observer(PipelineEvent.FAILED, rec.record)],
    )
    with pytest.raises(Exception):
        await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))

    assert len(rec.events) == 1
    name, ctx = rec.events[0]
    assert name == "PipelineFailedContext"
    assert isinstance(ctx, PipelineFailedContext)
    assert ctx.step_name == "failing"
    assert isinstance(ctx.exception, ValueError)


@pytest.mark.asyncio
async def test_given_pipeline_failed_context_then_has_fields():
    rec = EventRecorder()

    def failing(values: list[int]) -> int:
        raise ValueError("boom")

    p = pipeline(
        name="my_pipe",
        params=Params,
        steps=[step("s", fn=failing, on_error=OnError.STOP)],
        observers=[Observer(PipelineEvent.FAILED, rec.record)],
    )
    with pytest.raises(Exception):
        await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))

    ctx = rec.events[0][1]
    assert ctx.pipeline_name == "my_pipe"
    assert ctx.event is PipelineEvent.FAILED


# ---------------------------------------------------------------------------
# Step events — ALL mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_given_all_mode_step_when_succeeds_then_started_and_completed_emitted():
    rec = EventRecorder()

    def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s",
                fn=identity,
                observers=[
                    Observer(StepEvent.STARTED, rec.record),
                    Observer(StepEvent.COMPLETED, rec.record),
                ],
            )
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[42]))

    names = [e[0] for e in rec.events]
    assert "StepStartedContext" in names
    assert "StepCompletedContext" in names
    assert names.index("StepStartedContext") < names.index("StepCompletedContext")


@pytest.mark.asyncio
async def test_given_all_mode_step_completed_then_counts_correct():
    rec = EventRecorder()

    def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s", fn=identity, observers=[Observer(StepEvent.COMPLETED, rec.record)]
            )
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[42]))

    ctx = rec.events[0][1]
    assert ctx.success_count == 1
    assert ctx.error_count == 0
    assert ctx.completed_all_inputs is True
    assert ctx.mode == StepMode.ALL


@pytest.mark.asyncio
async def test_given_all_mode_step_when_fails_stop_then_failed_emitted():
    rec = EventRecorder()

    def failing(values: list[int]) -> int:
        raise ValueError("boom")

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s",
                fn=failing,
                on_error=OnError.STOP,
                observers=[Observer(StepEvent.FAILED, rec.record)],
            )
        ],
    )
    with pytest.raises(Exception):
        await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))

    ctx = rec.events[0][1]
    assert isinstance(ctx, StepFailedContext)
    assert ctx.completed_all_inputs is False
    assert isinstance(ctx.exception, ValueError)


# ---------------------------------------------------------------------------
# Step events — EACH mode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_given_each_mode_step_when_all_items_succeed_then_completed_with_counts():
    rec = EventRecorder()

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def double(gen: int) -> int:
        return gen * 2

    def collect(double: list[int]) -> int:
        return len(double)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen),
            step(
                "double",
                fn=double,
                observers=[Observer(StepEvent.COMPLETED, rec.record)],
            ),
            step("collect", fn=collect),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2, 3]))

    ctx = rec.events[0][1]
    assert isinstance(ctx, StepCompletedContext)
    assert ctx.mode == StepMode.EACH
    assert ctx.success_count == 3
    assert ctx.error_count == 0
    assert ctx.completed_all_inputs is True


@pytest.mark.asyncio
async def test_given_each_mode_step_when_some_fail_continue_then_completed_not_failed():
    rec = EventRecorder()

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def maybe_fail(gen: int) -> int:
        if gen == 2:
            raise ValueError("skip")
        return gen

    def collect(maybe_fail: list[int]) -> int:
        return len(maybe_fail)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen),
            step(
                "maybe_fail",
                fn=maybe_fail,
                on_error=OnError.CONTINUE,
                observers=[
                    Observer(StepEvent.COMPLETED, rec.record),
                    Observer(StepEvent.FAILED, rec.record),
                ],
            ),
            step("collect", fn=collect),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2, 3]))

    assert len(rec.events) == 1
    ctx = rec.events[0][1]
    assert isinstance(ctx, StepCompletedContext)
    assert ctx.completed_all_inputs is True


@pytest.mark.asyncio
async def test_given_each_mode_step_when_item_fails_stop_then_failed_with_partial_counts():
    rec = EventRecorder()

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def fail_on_second(gen: int) -> int:
        if gen == 2:
            raise ValueError("stop")
        return gen

    def collect(fail_on_second: list[int]) -> int:
        return len(fail_on_second)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen),
            step(
                "fail_on_second",
                fn=fail_on_second,
                on_error=OnError.STOP,
                observers=[Observer(StepEvent.FAILED, rec.record)],
            ),
            step("collect", fn=collect),
        ],
    )
    with pytest.raises(Exception):
        await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2, 3]))

    ctx = rec.events[0][1]
    assert isinstance(ctx, StepFailedContext)
    assert ctx.completed_all_inputs is False
    assert isinstance(ctx.exception, ValueError)


# ---------------------------------------------------------------------------
# step_output_observers still work independently
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_given_step_output_observers_when_run_then_not_affected_by_lifecycle_observers():
    """step_output_observers (low-level) coexist with lifecycle observers."""
    output_records = []

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("gen", fn=gen)],
    )

    executor = AsyncPipelineExecutor(
        p.dag,
        step_output_observers=[lambda n, o: output_records.append((n, o))],
    )
    await executor.execute(Params(values=[1, 2]))

    assert len(output_records) == 1
    step_name, output = output_records[0]
    assert step_name == "gen"


# ---------------------------------------------------------------------------
# Materialization events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_given_step_with_list_consumer_when_materialized_then_events_emitted():
    rec = EventRecorder()

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def collect(gen: list[int]) -> int:
        return len(gen)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "gen",
                fn=gen,
                observers=[
                    Observer(MaterializationEvent.STARTED, rec.record),
                    Observer(MaterializationEvent.COMPLETED, rec.record),
                ],
            ),
            step("collect", fn=collect),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2]))

    names = [e[0] for e in rec.events]
    assert "MaterializationStartedContext" in names
    assert "MaterializationCompletedContext" in names
    assert names.index("MaterializationStartedContext") < names.index(
        "MaterializationCompletedContext"
    )


@pytest.mark.asyncio
async def test_given_materialization_context_then_has_fields():
    rec = EventRecorder()

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def collect(gen: list[int]) -> int:
        return len(gen)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "gen",
                fn=gen,
                observers=[Observer(MaterializationEvent.STARTED, rec.record)],
            ),
            step("collect", fn=collect),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))

    ctx = rec.events[0][1]
    assert isinstance(ctx, MaterializationStartedContext)
    assert ctx.step_name == "gen"
    assert ctx.dataset_name == "gen"
    assert ctx.materializer_name is not None


@pytest.mark.asyncio
async def test_given_materialization_when_fails_then_failed_emitted():
    rec = EventRecorder()

    def bad_mat(ctx):
        def fail(value):
            raise ValueError("mat failed")

        return fail

    bad_mat.__name__ = "bad_mat"

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def collect(gen: list[int]) -> int:
        return len(gen)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "gen",
                fn=gen,
                materializer=bad_mat,
                on_error=OnError.STOP,
                observers=[Observer(MaterializationEvent.FAILED, rec.record)],
            ),
            step("collect", fn=collect),
        ],
    )
    try:
        await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))
    except Exception:
        pass

    mat_failed = [e for e in rec.events if e[0] == "MaterializationFailedContext"]
    assert len(mat_failed) >= 1
    assert isinstance(mat_failed[0][1].exception, ValueError)


@pytest.mark.asyncio
async def test_given_lazy_consumer_when_no_materialization_then_no_materialization_events():
    rec = EventRecorder()

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def passthrough(gen: Iterator[int]) -> Iterator[int]:
        yield from gen

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "gen",
                fn=gen,
                observers=[
                    Observer(MaterializationEvent.STARTED, rec.record),
                    Observer(MaterializationEvent.COMPLETED, rec.record),
                ],
            ),
            step("passthrough", fn=passthrough),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))

    materialization_events = [
        e
        for e in rec.events
        if e[0] in ("MaterializationStartedContext", "MaterializationCompletedContext")
    ]
    assert len(materialization_events) == 0


# ---------------------------------------------------------------------------
# Observer failure isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_given_observer_raises_when_dispatched_then_step_still_succeeds(caplog):
    def bad_observer(ctx):
        raise RuntimeError("observer failure")

    def ok_step(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s", fn=ok_step, observers=[Observer(StepEvent.COMPLETED, bad_observer)]
            )
        ],
    )
    caplog.set_level(logging.DEBUG)
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))
    assert "observer failure" in caplog.text


@pytest.mark.asyncio
async def test_given_observer_raises_when_dispatched_then_other_observers_still_called():
    rec = EventRecorder()

    def bad_observer(ctx):
        raise RuntimeError("fail")

    def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s",
                fn=identity,
                observers=[
                    Observer(StepEvent.COMPLETED, bad_observer),
                    Observer(StepEvent.COMPLETED, rec.record),
                ],
            )
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))
    assert len(rec.events) == 1


# ---------------------------------------------------------------------------
# Laziness / materialization preservation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_given_observers_when_lazy_step_then_output_remains_iterator():
    def gen(values: list[int]) -> Iter[int]:
        yield from values

    def lazy_consumer(gen: Iter[int]) -> Iter[int]:
        yield from gen

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "gen",
                fn=gen,
                observers=[Observer(StepEvent.COMPLETED, lambda ctx: None)],
            ),
            step("lazy_consumer", fn=lazy_consumer),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2]))


@pytest.mark.asyncio
async def test_given_materialization_observer_when_lazy_step_then_materialization_not_triggered():
    rec = EventRecorder()

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def lazy_consumer(gen: Iterator[int]) -> Iterator[int]:
        yield from gen

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "gen",
                fn=gen,
                observers=[Observer(MaterializationEvent.STARTED, rec.record)],
            ),
            step("lazy_consumer", fn=lazy_consumer),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2]))
    assert len(rec.events) == 0


# ---------------------------------------------------------------------------
# Async handler support
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_given_async_def_handler_when_dispatched_then_awaited():
    rec = EventRecorder()

    async def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s",
                fn=identity,
                observers=[Observer(StepEvent.COMPLETED, rec.async_record)],
            )
        ],
    )
    await async_run(p, Params(values=[42]))
    assert len(rec.events) == 1
    assert rec.events[0][0] == "StepCompletedContext"


@pytest.mark.asyncio
async def test_given_partial_async_handler_when_dispatched_then_awaited():
    rec = EventRecorder()

    async def handler(ctx):
        rec.record(ctx)

    partial_handler = functools.partial(handler)

    async def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s",
                fn=identity,
                observers=[Observer(StepEvent.COMPLETED, partial_handler)],
            )
        ],
    )
    await async_run(p, Params(values=[42]))
    assert len(rec.events) == 1


@pytest.mark.asyncio
async def test_given_callable_object_with_async_call_when_dispatched_then_awaited():
    rec = EventRecorder()

    class AsyncCallable:
        async def __call__(self, ctx):
            rec.record(ctx)

    async def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s",
                fn=identity,
                observers=[Observer(StepEvent.COMPLETED, AsyncCallable())],
            )
        ],
    )
    await async_run(p, Params(values=[42]))
    assert len(rec.events) == 1
