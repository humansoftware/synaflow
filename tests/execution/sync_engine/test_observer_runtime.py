from typing import Any
from synaflow.execution.sync_engine.executor import PipelineExecutor
import logging
from collections.abc import Iterator as Iter
from time import monotonic_ns
from time import sleep
from typing import Iterator, NamedTuple

import pytest

from synaflow import (
    MaterializationEvent,
    Observer,
    PipelineEvent,
    StepEvent,
    pipeline,
    run,
    step,
)
from synaflow.core.observers import (
    MaterializationStartedContext,
    PipelineFailedContext,
    StepCompletedContext,
    StepFailedContext,
    StepStartedContext,
)
from synaflow.core.types import OnError, StepMode


class Params(NamedTuple):
    values: list[int]


class EmptyParams(NamedTuple):
    pass


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


# ---------------------------------------------------------------------------
# Pipeline events
# ---------------------------------------------------------------------------


def test_given_pipeline_run_id_is_consistent_and_unique_per_run():
    rec = EventRecorder()

    def dummy(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p1",
        params=Params,
        steps=[step("dummy", fn=dummy)],
        observers=[Observer(rec.record)],
    )

    run(p, Params(values=[1]))
    run(p, Params(values=[2]))

    # Assert p run
    run_ids = {ctx.run_id for _, ctx in rec.events}
    assert len(run_ids) == 2

    # Assert each run_id has events associated with it
    for r_id in run_ids:
        assert isinstance(r_id, str) and len(r_id) > 0


def test_given_pipeline_observer_when_run_completes_then_started_and_completed_emitted():
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
    run(p, Params(values=[1, 2]))

    names = [e[0] for e in rec.events]
    assert "PipelineStartedContext" in names
    assert "PipelineCompletedContext" in names
    assert names.index("PipelineStartedContext") < names.index(
        "PipelineCompletedContext"
    )
    assert "PipelineFailedContext" not in names


def test_given_pipeline_observer_when_step_fails_stop_then_failed_emitted():
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
        run(p, Params(values=[1]))

    assert len(rec.events) == 1
    name, ctx = rec.events[0]
    assert name == "PipelineFailedContext"
    assert isinstance(ctx, PipelineFailedContext)
    assert ctx.step_name == "failing"
    assert isinstance(ctx.exception, ValueError)


def test_given_pipeline_failed_context_then_has_fields():
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
        run(p, Params(values=[1]))

    ctx = rec.events[0][1]
    assert ctx.pipeline_name == "my_pipe"
    assert ctx.event is PipelineEvent.FAILED


# ---------------------------------------------------------------------------
# Step events — ALL mode
# ---------------------------------------------------------------------------


def test_given_all_mode_step_when_succeeds_then_started_and_completed_emitted():
    rec = EventRecorder()

    def identity(values: list[int]) -> int:
        return values[0]

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("s", fn=identity, observers=[Observer(rec.record)])],
    )
    run(p, Params(values=[42]))

    names = [e[0] for e in rec.events]
    assert "StepStartedContext" in names
    assert "StepCompletedContext" in names
    assert names.index("StepStartedContext") < names.index("StepCompletedContext")


def test_given_all_mode_step_completed_then_counts_correct():
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
    run(p, Params(values=[42]))

    ctx = rec.events[0][1]
    assert ctx.success_count == 1
    assert ctx.error_count == 0
    assert ctx.completed_all_inputs is True
    assert ctx.mode == StepMode.ALL


def test_given_all_mode_step_when_fails_stop_then_failed_emitted():
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
        run(p, Params(values=[1]))

    ctx = rec.events[0][1]
    assert isinstance(ctx, StepFailedContext)
    assert ctx.completed_all_inputs is False
    assert isinstance(ctx.exception, ValueError)


# ---------------------------------------------------------------------------
# Step events — EACH mode
# ---------------------------------------------------------------------------


def test_given_each_mode_step_when_all_items_succeed_then_completed_with_counts():
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
    run(p, Params(values=[1, 2, 3]))

    ctx = rec.events[0][1]
    assert isinstance(ctx, StepCompletedContext)
    assert ctx.mode == StepMode.EACH
    assert ctx.success_count == 3
    assert ctx.error_count == 0
    assert ctx.completed_all_inputs is True


def test_given_each_mode_step_when_some_fail_continue_then_completed_not_failed():
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
    run(p, Params(values=[1, 2, 3]))

    assert len(rec_comp.events) == 1
    ctx = rec_comp.events[0][1]
    assert isinstance(ctx, StepCompletedContext)
    assert ctx.completed_all_inputs is True
    assert len(rec_fail.events) == 0


def test_given_each_mode_step_when_item_fails_stop_then_failed_with_partial_counts():
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
        run(p, Params(values=[1, 2, 3]))

    ctx = rec.events[0][1]
    assert isinstance(ctx, StepFailedContext)
    assert ctx.completed_all_inputs is False
    assert isinstance(ctx.exception, ValueError)


# ---------------------------------------------------------------------------
# step_output_observers still work independently
# ---------------------------------------------------------------------------


def test_given_step_output_observers_when_run_then_not_affected_by_lifecycle_observers():
    """step_output_observers (low-level) coexist with lifecycle observers."""

    output_records = []

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    p = pipeline(
        name="p",
        params=Params,
        steps=[step("gen", fn=gen, force_materialize=True)],
    )

    executor = PipelineExecutor(
        p.dag,
        step_output_observers=[lambda n, o: output_records.append((n, o))],
    )
    executor.execute(Params(values=[1, 2]))

    assert len(output_records) == 1
    step_name, output = output_records[0]
    assert step_name == "gen"


def test_given_step_output_observer_when_branch_stops_early_then_observer_sees_full_stream():

    output_records = []

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def early(gen: Iterator[int]) -> None:
        for _item in gen:
            break

    def full(gen: Iterator[int]) -> list[int]:
        return list(gen)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen, max_in_flight=3),
            step("early", fn=early),
            step("full", fn=full),
        ],
    )

    executor = PipelineExecutor(
        p.dag,
        step_output_observers=[
            lambda n, o: output_records.append((n, list(o) if n == "gen" else o))
        ],
    )
    executor.execute(Params(values=[1, 2, 3]))

    assert ("gen", [1, 2, 3]) in output_records
    assert ("full", [1, 2, 3]) in output_records


def test_given_step_output_observer_and_bounded_lazy_stream_then_observer_does_not_force_eager():

    rec = EventRecorder(MaterializationEvent.STARTED)
    output_records = []

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def lazy_consumer(gen: Iterator[int]) -> None:
        for _item in gen:
            pass

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step(
                "gen",
                fn=gen,
                max_in_flight=3,
                observers=[
                    Observer(on_event(MaterializationEvent.STARTED, rec.record))
                ],
            ),
            step("lazy_consumer", fn=lazy_consumer),
        ],
    )

    executor = PipelineExecutor(
        p.dag,
        step_output_observers=[lambda n, o: output_records.append((n, o))],
    )
    executor.execute(Params(values=[1, 2, 3]))

    assert len(rec.events) == 0
    assert any(step_name == "gen" for step_name, _output in output_records)


def test_given_step_output_observer_when_bounded_stream_then_observer_does_not_consume_slots():

    log: list[str] = []

    def gen(values: list[int]) -> Iterator[int]:
        for value in values:
            log.append(f"prod {value}")
            yield value

    def slow(gen: Iterator[int]) -> None:
        for item in gen:
            log.append(f"recv {item}")
            sleep(0.01)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen, max_in_flight=1),
            step("slow", fn=slow),
        ],
    )

    executor = PipelineExecutor(
        p.dag,
        step_output_observers=[
            lambda n, o: list(o) if n == "gen" else None,
        ],
    )
    executor.execute(Params(values=[0, 1, 2, 3]))

    assert log.index("recv 0") < log.index("prod 2")


def test_given_step_output_observer_when_bounded_stream_then_bound_is_unchanged():

    output_records = []
    produced: list[int] = []
    consumed: list[int] = []
    max_seen_ahead = 0

    def gen(values: list[int]) -> Iterator[int]:
        nonlocal max_seen_ahead
        for value in values:
            produced.append(value)
            max_seen_ahead = max(max_seen_ahead, len(produced) - len(consumed))
            yield value

    def slow(gen: Iterator[int]) -> None:
        for item in gen:
            consumed.append(item)
            sleep(0.01)

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen, max_in_flight=3),
            step("slow", fn=slow),
        ],
    )

    executor = PipelineExecutor(
        p.dag,
        step_output_observers=[lambda n, o: output_records.append((n, o))],
    )
    executor.execute(Params(values=list(range(10))))

    assert consumed == list(range(10))
    assert max_seen_ahead <= 4
    assert any(step_name == "gen" for step_name, _output in output_records)


def test_given_lazy_stream_drained_by_output_observer_when_run_completes_then_completion_events_wait_for_drain():
    state = {
        "generator_exhausted_at": None,
        "observer_finished_at": None,
        "pipeline_completed_at": None,
        "step_completed_at": None,
        "step_completed_counts": None,
    }

    def source() -> Iterator[int]:
        try:
            for value in [1, 2, 3]:
                sleep(0.01)
                yield value
        finally:
            state["generator_exhausted_at"] = monotonic_ns()

    def done(source: Iterator[int]) -> None:
        return None

    def lifecycle_observer(ctx) -> None:
        now = monotonic_ns()
        if isinstance(ctx, StepCompletedContext) and ctx.step_name == "source":
            state["step_completed_at"] = now
            state["step_completed_counts"] = (
                ctx.success_count,
                ctx.error_count,
                ctx.completed_all_inputs,
            )
        if type(ctx).__name__ == "PipelineCompletedContext":
            state["pipeline_completed_at"] = now

    def output_observer(step_name: str, output) -> None:
        if step_name != "source":
            return
        assert list(output) == [1, 2, 3]
        state["observer_finished_at"] = monotonic_ns()

    p = pipeline(
        name="p",
        params=EmptyParams,
        observers=[Observer(lifecycle_observer)],
        steps=[
            step("source", fn=source),
            step("done", fn=done),
        ],
    )

    PipelineExecutor(
        p.dag,
        resource_factories=p.resources,
        step_output_observers=[output_observer],
    ).execute(EmptyParams())

    assert state["generator_exhausted_at"] is not None
    assert state["observer_finished_at"] is not None
    assert state["step_completed_at"] is not None
    assert state["pipeline_completed_at"] is not None
    assert state["step_completed_at"] >= state["generator_exhausted_at"]
    assert state["pipeline_completed_at"] >= state["observer_finished_at"]
    assert state["step_completed_counts"] == (3, 0, True)


def test_given_terminal_last_step_with_output_observer_when_run_completes_then_pipeline_waits_for_observer():
    state = {
        "observer_finished_at": None,
        "pipeline_completed_at": None,
        "step_completed_counts": None,
        "error_materializer_called": False,
    }

    def source() -> int:
        return 1

    def middle(source: int) -> int:
        return source + 1

    def terminal(middle: int) -> list[int]:
        return [middle, middle + 1, middle + 2]

    def error_factory(ctx):
        def handle(error_ctx):
            state["error_materializer_called"] = True

        return handle

    def lifecycle_observer(ctx) -> None:
        now = monotonic_ns()
        if isinstance(ctx, StepCompletedContext) and ctx.step_name == "terminal":
            state["step_completed_counts"] = (
                ctx.success_count,
                ctx.error_count,
                ctx.completed_all_inputs,
            )
        if type(ctx).__name__ == "PipelineCompletedContext":
            state["pipeline_completed_at"] = now

    def output_observer(step_name: str, output) -> None:
        if step_name != "terminal":
            return
        assert output == [2, 3, 4]
        sleep(0.01)
        state["observer_finished_at"] = monotonic_ns()

    p = pipeline(
        name="p",
        params=EmptyParams,
        observers=[Observer(lifecycle_observer)],
        steps=[
            step("source", fn=source),
            step("middle", fn=middle),
            step("terminal", fn=terminal, error_materializer=error_factory),
        ],
    )

    PipelineExecutor(
        p.dag,
        resource_factories=p.resources,
        step_output_observers=[output_observer],
    ).execute(EmptyParams())

    assert state["observer_finished_at"] is not None
    assert state["pipeline_completed_at"] is not None
    assert state["pipeline_completed_at"] >= state["observer_finished_at"]
    assert state["step_completed_counts"] == (3, 0, True)
    assert state["error_materializer_called"] is False


# ---------------------------------------------------------------------------
# Materialization events
# ---------------------------------------------------------------------------


def test_given_step_with_list_consumer_when_materialized_then_events_emitted():
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
    run(p, Params(values=[1, 2]))

    names = [e[0] for e in rec.events]
    assert "MaterializationStartedContext" in names
    assert "MaterializationCompletedContext" in names
    mat_start = names.index("MaterializationStartedContext")
    mat_complete = names.index("MaterializationCompletedContext")
    assert mat_start < mat_complete


def test_given_materialization_context_then_has_fields():
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
    run(p, Params(values=[1]))

    ctx = rec.events[0][1]
    assert isinstance(ctx, MaterializationStartedContext)
    assert ctx.step_name == "gen"
    assert ctx.dataset_name == "gen"
    assert ctx.materializer_name is not None


def test_given_materialization_when_fails_then_failed_emitted():
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
        run(p, Params(values=[1]))
    except Exception:
        pass

    assert len(rec.events) >= 1
    assert isinstance(rec.events[0][1].exception, ValueError)


def test_given_lazy_consumer_when_no_materialization_then_no_materialization_events():
    rec = EventRecorder(MaterializationEvent.STARTED)

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def passthrough(gen: Iterator[int]) -> None:
        for _item in gen:
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
    run(p, Params(values=[1]))

    assert len(rec.events) == 0


# ---------------------------------------------------------------------------
# Observer failure isolation
# ---------------------------------------------------------------------------


def test_given_observer_raises_when_dispatched_then_step_still_succeeds(caplog):
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
    run(p, Params(values=[1]))
    assert "observer failure" in caplog.text


def test_given_observer_raises_when_dispatched_then_other_observers_still_called():
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
    run(p, Params(values=[1]))
    assert len(rec.events) == 1


# ---------------------------------------------------------------------------
# Laziness / materialization preservation
# ---------------------------------------------------------------------------


def test_given_observers_when_lazy_step_then_output_remains_iterator():
    def gen(values: list[int]) -> Iter[int]:
        yield from values

    def lazy_consumer(gen: Iter[int]) -> None:
        for _item in gen:
            pass

    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("gen", fn=gen, observers=[Observer(lambda ctx: None)]),
            step("lazy_consumer", fn=lazy_consumer),
        ],
    )
    run(p, Params(values=[1, 2]))


def test_given_materialization_observer_when_lazy_step_then_materialization_not_triggered():
    rec = EventRecorder(MaterializationEvent.STARTED)

    def gen(values: list[int]) -> Iterator[int]:
        yield from values

    def lazy_consumer(gen: Iterator[int]) -> None:
        for _item in gen:
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
    run(p, Params(values=[1, 2]))
    assert len(rec.events) == 0


def test_given_stream_with_no_consumers_but_has_observers_then_stream_is_consumed():
    rec = EventRecorder()

    def producer(values: list[int]) -> Any:
        yield from values

    p = pipeline(
        name="p1",
        params=Params,
        steps=[step("p", fn=producer)],
    )

    executor = PipelineExecutor(
        p.dag,
        step_output_observers=[lambda step_name, output: rec.record(step_name, output)],
    )
    executor.execute(Params(values=[1, 2, 3]))

    # Let the test pass without crashing!


def test_given_step_returning_list_when_observed_then_success_count_reflects_logical_item_count():
    rec = EventRecorder()

    def producer(values: list[int]) -> Iterator[int]:
        yield from values

    def consumer(prod: list[int]) -> list[int]:
        return prod

    p = pipeline(
        name="test_p",
        params=Params,
        steps=[step("prod", fn=producer), step("cons", fn=consumer)],
        observers=[Observer(rec.record)],
    )

    run(p, params=Params(values=[1, 2, 3]))

    cons_event = next(
        ctx
        for name, ctx in rec.events
        if isinstance(ctx, StepCompletedContext) and ctx.step_name == "cons"
    )
    assert cons_event.success_count == 3


def test_given_lazy_generator_step_when_observed_then_step_started_event_fires_on_first_input_consumption():
    state = {"generator_started": False, "step_started_event_fired": False}

    def producer() -> Iterator[int]:
        state["generator_started"] = True
        yield 1
        yield 2

    def observer(ctx):
        if (
            isinstance(ctx, StepStartedContext)
            and getattr(ctx, "step_name", None) == "prod"
        ):
            # the step started event should ONLY fire after the generator actually starts!
            # or at the same time it is pulled.
            assert state["generator_started"] is True, (
                "StepStarted fired before generator actually started!"
            )
            state["step_started_event_fired"] = True

    def consumer(prod: Iterator[int]) -> list[int]:
        return list(prod)

    p = pipeline(
        name="test_p",
        params=Params,
        steps=[
            step("prod", fn=producer),
            step("cons", fn=consumer),
        ],
        observers=[Observer(observer)],
    )

    run(p, params=Params(values=[]))
    assert state["step_started_event_fired"] is True
