from synaflow.core.dag_builder import build_dag
from synaflow.core.adapters import async_adapter
import functools
import logging
from collections.abc import AsyncIterator
from typing import NamedTuple
import pytest
from synaflow import (
    MaterializationEvent,
    Observer,
    PipelineEvent,
    StepEvent,
    async_run,
    include,
    pipeline,
    step,
)
from synaflow.core.observers import (
    MaterializationStartedContext,
    PipelineFailedContext,
    PipelineStartedContext,
    StepCompletedContext,
    StepFailedContext,
    StepStartedContext,
)
from synaflow.core.types import OnError, StepMode
from synaflow.execution.async_engine.executor import AsyncPipelineExecutor


class Params(NamedTuple):
    values: list[int]


class EmptyParams(NamedTuple):
    pass


def on_event(event_type, handler):
    from synaflow.core.adapters import async_adapter

    async_handler = async_adapter(handler)

    async def wrapper(ctx):
        if ctx.event is event_type:
            return await async_handler(ctx)

    wrapper.__name__ = getattr(handler, "__name__", "on_event")
    return wrapper


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


@pytest.mark.asyncio
async def test_given_pipeline_run_id_is_consistent_and_unique_per_run():
    rec = EventRecorder()

    async def dummy(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p1",
        params=Params,
        steps=[step("dummy", fn=dummy)],
        observers=[Observer(async_adapter(rec.record))],
    )
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[2]))
    run_ids = {ctx.run_id for _, ctx in rec.events}
    assert len(run_ids) == 2
    for r_id in run_ids:
        assert isinstance(r_id, str) and len(r_id) > 0


@pytest.mark.asyncio
async def test_given_pipeline_observer_when_run_completes_then_started_and_completed_emitted():
    rec = EventRecorder()

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def consumer(gen: AsyncIterator[int]) -> list[int]:
        return [x async for x in gen]

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("gen", fn=gen), step("consumer", fn=consumer)],
        observers=[Observer(async_adapter(rec.record))],
    )
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1, 2]))
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

    async def failing(values: list[int]) -> int:
        raise ValueError("boom")

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("failing", fn=failing, on_error=OnError.STOP)],
        observers=[Observer(on_event(PipelineEvent.FAILED, rec.record))],
    )
    with pytest.raises(Exception):
        await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    assert len(rec.events) == 1
    name, ctx = rec.events[0]
    assert name == "PipelineFailedContext"
    assert isinstance(ctx, PipelineFailedContext)
    assert ctx.step_name == "failing"
    assert isinstance(ctx.exception, ValueError)


@pytest.mark.asyncio
async def test_given_pipeline_failed_context_then_has_fields():
    rec = EventRecorder(PipelineEvent.FAILED)

    async def failing(values: list[int]) -> int:
        raise ValueError("boom")

    p = pipeline(
        name="my_pipe",
        params=Params,
        steps=[step("s", fn=failing, on_error=OnError.STOP)],
        observers=[Observer(on_event(PipelineEvent.FAILED, rec.record))],
    )
    with pytest.raises(Exception):
        await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    ctx = rec.events[0][1]
    assert ctx.pipeline_name == "my_pipe"
    assert ctx.event is PipelineEvent.FAILED


@pytest.mark.asyncio
async def test_given_all_mode_step_when_succeeds_then_started_and_completed_emitted():
    rec = EventRecorder()

    async def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("s", fn=identity, observers=[Observer(async_adapter(rec.record))])],
    )
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[42]))
    names = [e[0] for e in rec.events]
    assert "StepStartedContext" in names
    assert "StepCompletedContext" in names
    assert names.index("StepStartedContext") < names.index("StepCompletedContext")


@pytest.mark.asyncio
async def test_given_all_mode_step_completed_then_counts_correct():
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
                observers=[Observer(on_event(StepEvent.COMPLETED, rec.record))],
            )
        ],
    )
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[42]))
    ctx = rec.events[0][1]
    assert ctx.success_count == 1
    assert ctx.error_count == 0
    assert ctx.completed_all_inputs is True
    assert ctx.mode == StepMode.ALL


@pytest.mark.asyncio
async def test_given_all_mode_step_when_fails_stop_then_failed_emitted():
    rec = EventRecorder(StepEvent.FAILED)

    async def failing(values: list[int]) -> int:
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
        await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    ctx = rec.events[0][1]
    assert isinstance(ctx, StepFailedContext)
    assert ctx.completed_all_inputs is False
    assert isinstance(ctx.exception, ValueError)


@pytest.mark.asyncio
async def test_given_each_mode_step_when_all_items_succeed_then_completed_with_counts():
    rec = EventRecorder(StepEvent.COMPLETED)

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def double(gen: int) -> int:
        return gen * 2

    async def collect(double: list[int]) -> int:
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
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1, 2, 3]))
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

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def maybe_fail(gen: int) -> int:
        if gen == 2:
            raise ValueError("skip")
        return gen

    async def collect(maybe_fail: list[int]) -> int:
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
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1, 2, 3]))
    assert len(rec_comp.events) == 1
    ctx = rec_comp.events[0][1]
    assert isinstance(ctx, StepCompletedContext)
    assert ctx.completed_all_inputs is True
    assert len(rec_fail.events) == 0


@pytest.mark.asyncio
async def test_given_each_mode_step_when_item_fails_stop_then_failed_with_partial_counts():
    rec = EventRecorder(StepEvent.FAILED)

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def fail_on_second(gen: int) -> int:
        if gen == 2:
            raise ValueError("stop")
        return gen

    async def collect(fail_on_second: list[int]) -> int:
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
        await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1, 2, 3]))
    ctx = rec.events[0][1]
    assert isinstance(ctx, StepFailedContext)
    assert ctx.completed_all_inputs is False
    assert isinstance(ctx.exception, ValueError)


@pytest.mark.asyncio
async def test_given_step_with_list_consumer_when_materialized_then_events_emitted():
    rec = EventRecorder()

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def collect(gen: list[int]) -> int:
        return len(gen)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen, observers=[Observer(async_adapter(rec.record))]),
            step("collect", fn=collect),
        ],
    )
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1, 2]))
    names = [e[0] for e in rec.events]
    assert "MaterializationStartedContext" in names
    assert "MaterializationCompletedContext" in names
    mat_start = names.index("MaterializationStartedContext")
    mat_complete = names.index("MaterializationCompletedContext")
    assert mat_start < mat_complete


@pytest.mark.asyncio
async def test_given_materialization_context_then_has_fields():
    rec = EventRecorder(MaterializationEvent.STARTED)

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def collect(gen: list[int]) -> int:
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
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    ctx = rec.events[0][1]
    assert isinstance(ctx, MaterializationStartedContext)
    assert ctx.step_name == "gen"
    assert ctx.dataset_name == "gen"
    assert ctx.materializer_name is not None


@pytest.mark.asyncio
async def test_given_materialization_when_fails_then_failed_emitted():
    rec = EventRecorder(MaterializationEvent.FAILED)

    def bad_mat(ctx):

        async def fail(value):
            raise ValueError("mat failed")

        return fail

    bad_mat.__name__ = "bad_mat"

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def collect(gen: list[int]) -> int:
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
        await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    except Exception:
        pass
    assert len(rec.events) >= 1
    assert isinstance(rec.events[0][1].exception, ValueError)


@pytest.mark.asyncio
async def test_given_lazy_consumer_when_no_materialization_then_no_materialization_events():
    rec = EventRecorder(MaterializationEvent.STARTED)

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def passthrough(gen: AsyncIterator[int]) -> None:
        async for _item in gen:
            pass

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
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    assert len(rec.events) == 0


@pytest.mark.asyncio
async def test_given_observer_raises_when_dispatched_then_step_still_succeeds(caplog):

    async def bad_observer(ctx):
        raise RuntimeError("observer failure")

    async def ok_step(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("s", fn=ok_step, observers=[Observer(bad_observer)])],
    )
    caplog.set_level(logging.DEBUG)
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    assert "observer failure" in caplog.text


@pytest.mark.asyncio
async def test_given_observer_raises_when_dispatched_then_other_observers_still_called():
    rec = EventRecorder(StepEvent.COMPLETED)

    async def bad_observer(ctx):
        raise RuntimeError("fail")

    async def identity(values: list[int]) -> int:
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
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1]))
    assert len(rec.events) == 1


@pytest.mark.asyncio
async def test_given_observers_when_lazy_step_then_output_remains_iterator():

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def lazy_consumer(gen: AsyncIterator[int]) -> None:
        async for _item in gen:
            pass

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen, observers=[Observer(async_adapter(lambda ctx: None))]),
            step("lazy_consumer", fn=lazy_consumer),
        ],
    )
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1, 2]))


@pytest.mark.asyncio
async def test_given_materialization_observer_when_lazy_step_then_materialization_not_triggered():
    rec = EventRecorder(MaterializationEvent.STARTED)

    async def gen(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def lazy_consumer(gen: AsyncIterator[int]) -> None:
        async for _item in gen:
            pass

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
    await AsyncPipelineExecutor(build_dag(p)).execute(Params(values=[1, 2]))
    assert len(rec.events) == 0


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


@pytest.mark.asyncio
async def test_given_step_returning_list_when_observed_then_success_count_reflects_logical_item_count():
    rec = EventRecorder()

    async def producer(values: list[int]) -> AsyncIterator[int]:
        for v in values:
            yield v

    async def consumer(prod: list[int]) -> list[int]:
        return prod

    p = pipeline(
        name="test_p",
        params=Params,
        steps=[step("prod", fn=producer), step("cons", fn=consumer)],
        observers=[Observer(async_adapter(rec.record))],
    )
    await async_run(p, params=Params(values=[1, 2, 3]))
    cons_event = next(
        (
            ctx
            for name, ctx in rec.events
            if isinstance(ctx, StepCompletedContext) and ctx.step_name == "cons"
        )
    )
    assert cons_event.success_count == 3


@pytest.mark.asyncio
async def test_given_lazy_generator_step_when_observed_then_step_started_event_fires_on_first_input_consumption():
    state = {"generator_started": False, "step_started_event_fired": False}

    async def producer() -> AsyncIterator[int]:
        state["generator_started"] = True
        yield 1
        yield 2

    def observer(ctx):
        if (
            isinstance(ctx, StepStartedContext)
            and getattr(ctx, "step_name", None) == "prod"
        ):
            assert state["generator_started"] is True, (
                "StepStarted fired before generator actually started!"
            )
            state["step_started_event_fired"] = True

    async def consumer(prod: AsyncIterator[int]) -> list[int]:
        return [x async for x in prod]

    p = pipeline(
        name="test_p",
        params=Params,
        steps=[step("prod", fn=producer), step("cons", fn=consumer)],
        observers=[Observer(async_adapter(observer))],
    )
    await async_run(p, params=Params(values=[]))
    assert state["step_started_event_fired"] is True


@pytest.mark.asyncio
async def test_given_pipeline_started_context_exposes_scope_step_totals():
    """PipelineStartedContext.scope_step_totals exposes the dag-level
    dict (async parity of sync test)."""

    class _Scope(NamedTuple):
        values: list[int] = []

    rec = EventRecorder()

    async def fn_only(values: list[int]) -> int:
        return sum(values)

    sub = pipeline(
        name="Sub", params=_Scope, exports="only", steps=[step("only", fn=fn_only)]
    )

    async def adapt_sub(values: list[int]) -> _Scope:
        return _Scope(values=values)

    p = pipeline(
        name="pl_started",
        params=_Scope,
        steps=[
            include(name="first", pipeline=sub, fn=adapt_sub),
            step("solo", fn=fn_only),
        ],
        observers=[Observer(rec.async_record)],
    )
    await async_run(p, params=_Scope(values=[1, 2, 3]))
    started = next(
        (ctx for _, ctx in rec.events if isinstance(ctx, PipelineStartedContext))
    )
    assert started.scope_step_totals == {
        "pl_started": 2,
        "pl_started__first": 1,
    }


@pytest.mark.asyncio
async def test_given_pipeline_started_context_default_scope_step_totals_is_empty_dict():
    """Field always present (stable contract)."""

    class _Scope(NamedTuple):
        values: list[int] = []

    rec = EventRecorder()

    async def fn_only(values: list[int]) -> int:
        return sum(values)

    p = pipeline(
        name="trivial",
        params=_Scope,
        steps=[step("only", fn=fn_only)],
        observers=[Observer(rec.async_record)],
    )
    await async_run(p, params=_Scope(values=[1]))
    started = next(
        (ctx for _, ctx in rec.events if isinstance(ctx, PipelineStartedContext))
    )
    assert started.scope_step_totals == {"trivial": 1}
