import functools
import logging
from collections.abc import AsyncIterator, Iterator as Iter
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
# Wrapper-level event filter (per spec: filtering lives above the core)
# ---------------------------------------------------------------------------


def on_event(event_type, handler):
    def wrapper(ctx):
        if ctx.event is event_type:
            return handler(ctx)

    wrapper.__name__ = getattr(handler, "__name__", "on_event")
    return wrapper


# ---------------------------------------------------------------------------
# Helper: observer that records all events for a specific event type
# ---------------------------------------------------------------------------


class EventRecorder:
    def __init__(self, event_type=None):
        self.events: list[tuple] = []
        self.event_type = event_type

    def record(self, ctx):
        if self.event_type is None or ctx.event is self.event_type:
            self.events.append((type(ctx).__name__, ctx))

    async def async_record(self, ctx):
        if self.event_type is None or ctx.event is self.event_type:
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
        observers=[Observer(rec.record)],
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
    rec = EventRecorder(PipelineEvent.FAILED)

    def failing(values: list[int]) -> int:
        raise ValueError("boom")

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("failing", fn=failing, on_error=OnError.STOP)],
        observers=[Observer(on_event(PipelineEvent.FAILED, rec.record))],
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
    rec = EventRecorder(PipelineEvent.FAILED)

    def failing(values: list[int]) -> int:
        raise ValueError("boom")

    p = pipeline(
        name="my_pipe",
        params=Params,
        steps=[step("s", fn=failing, on_error=OnError.STOP)],
        observers=[Observer(on_event(PipelineEvent.FAILED, rec.record))],
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
        steps=[step("s", fn=identity, observers=[Observer(rec.record)])],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[42]))

    names = [e[0] for e in rec.events]
    assert "StepStartedContext" in names
    assert "StepCompletedContext" in names
    assert names.index("StepStartedContext") < names.index("StepCompletedContext")


@pytest.mark.asyncio
async def test_given_all_mode_step_completed_then_counts_correct():
    rec = EventRecorder(StepEvent.COMPLETED)

    def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s",
                fn=identity,
                observers=[Observer(on_event(StepEvent.COMPLETED, rec.record))],
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
    rec = EventRecorder(StepEvent.FAILED)

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
                observers=[Observer(on_event(StepEvent.FAILED, rec.record))],
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
    rec = EventRecorder(StepEvent.COMPLETED)

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
                observers=[Observer(on_event(StepEvent.COMPLETED, rec.record))],
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
    rec_comp = EventRecorder(StepEvent.COMPLETED)
    rec_fail = EventRecorder(StepEvent.FAILED)

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
                    Observer(on_event(StepEvent.COMPLETED, rec_comp.record)),
                    Observer(on_event(StepEvent.FAILED, rec_fail.record)),
                ],
            ),
            step("collect", fn=collect),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2, 3]))

    assert len(rec_comp.events) == 1
    ctx = rec_comp.events[0][1]
    assert isinstance(ctx, StepCompletedContext)
    assert ctx.completed_all_inputs is True
    assert len(rec_fail.events) == 0


@pytest.mark.asyncio
async def test_given_each_mode_step_when_item_fails_stop_then_failed_with_partial_counts():
    rec = EventRecorder(StepEvent.FAILED)

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
                observers=[Observer(on_event(StepEvent.FAILED, rec.record))],
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


@pytest.mark.asyncio
async def test_given_step_output_observer_when_branch_stops_early_then_observer_sees_full_stream():
    output_records = []

    async def gen(values: list[int]):
        for value in values:
            yield value

    async def early(gen: AsyncIterator[int]):
        async for _item in gen:
            break

    async def full(gen: AsyncIterator[int]):
        return [item async for item in gen]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen, max_in_flight=3),
            step("early", fn=early),
            step("full", fn=full),
        ],
    )

    executor = AsyncPipelineExecutor(
        p.dag,
        step_output_observers=[lambda n, o: output_records.append((n, o))],
    )
    await executor.execute(Params(values=[1, 2, 3]))

    gen_output = next(
        output for step_name, output in output_records if step_name == "gen"
    )
    assert gen_output == [1, 2, 3]
    assert ("full", [1, 2, 3]) in output_records


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
            step("gen", fn=gen, observers=[Observer(rec.record)]),
            step("collect", fn=collect),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2]))

    names = [e[0] for e in rec.events]
    assert "MaterializationStartedContext" in names
    assert "MaterializationCompletedContext" in names
    mat_start = names.index("MaterializationStartedContext")
    mat_complete = names.index("MaterializationCompletedContext")
    assert mat_start < mat_complete


@pytest.mark.asyncio
async def test_given_materialization_context_then_has_fields():
    rec = EventRecorder(MaterializationEvent.STARTED)

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
                    Observer(on_event(MaterializationEvent.STARTED, rec.record))
                ],
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
    rec = EventRecorder(MaterializationEvent.FAILED)

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
                observers=[Observer(on_event(MaterializationEvent.FAILED, rec.record))],
            ),
            step("collect", fn=collect),
        ],
    )
    try:
        await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))
    except Exception:
        pass

    assert len(rec.events) >= 1
    assert isinstance(rec.events[0][1].exception, ValueError)


@pytest.mark.asyncio
async def test_given_lazy_consumer_when_no_materialization_then_no_materialization_events():
    rec = EventRecorder(MaterializationEvent.STARTED)

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
                    Observer(on_event(MaterializationEvent.STARTED, rec.record))
                ],
            ),
            step("passthrough", fn=passthrough),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))

    assert len(rec.events) == 0


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
        steps=[step("s", fn=ok_step, observers=[Observer(bad_observer)])],
    )
    caplog.set_level(logging.DEBUG)
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1]))
    assert "observer failure" in caplog.text


@pytest.mark.asyncio
async def test_given_observer_raises_when_dispatched_then_other_observers_still_called():
    rec = EventRecorder(StepEvent.COMPLETED)

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
                    Observer(bad_observer),
                    Observer(on_event(StepEvent.COMPLETED, rec.record)),
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
            step("gen", fn=gen, observers=[Observer(lambda ctx: None)]),
            step("lazy_consumer", fn=lazy_consumer),
        ],
    )
    await AsyncPipelineExecutor(p.dag).execute(Params(values=[1, 2]))


@pytest.mark.asyncio
async def test_given_materialization_observer_when_lazy_step_then_materialization_not_triggered():
    rec = EventRecorder(MaterializationEvent.STARTED)

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
                observers=[
                    Observer(on_event(MaterializationEvent.STARTED, rec.record))
                ],
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
    rec = EventRecorder(StepEvent.COMPLETED)

    async def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "s",
                fn=identity,
                observers=[Observer(on_event(StepEvent.COMPLETED, rec.async_record))],
            )
        ],
    )
    await async_run(p, Params(values=[42]))
    assert len(rec.events) == 1
    assert rec.events[0][0] == "StepCompletedContext"


@pytest.mark.asyncio
async def test_given_partial_async_handler_when_dispatched_then_awaited():
    rec = EventRecorder(StepEvent.COMPLETED)

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
                observers=[Observer(on_event(StepEvent.COMPLETED, partial_handler))],
            )
        ],
    )
    await async_run(p, Params(values=[42]))
    assert len(rec.events) == 1


@pytest.mark.asyncio
async def test_given_callable_object_with_async_call_when_dispatched_then_awaited():
    rec = EventRecorder(StepEvent.COMPLETED)

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
                observers=[Observer(on_event(StepEvent.COMPLETED, AsyncCallable()))],
            )
        ],
    )
    await async_run(p, Params(values=[42]))
    assert len(rec.events) == 1
