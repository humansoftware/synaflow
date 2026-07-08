import logging
from collections.abc import Iterator as Iter
from typing import Iterator, NamedTuple

import pytest

from synaflow import (
    MaterializationEvent,
    Observer,
    PipelineEvent,
    StepEvent,
    include,
    pipeline,
    run,
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


# ---------------------------------------------------------------------------
# issue #105: scope-stamped fields flow through to observer contexts
# ---------------------------------------------------------------------------


def test_given_step_started_context_carries_scope_index_and_total():
    """Scope fields stamped at DAG build time (issue #105) flow
    through to the StepStartedContext seen by observers."""
    rec = EventRecorder()

    def fn_a(values: list[int]) -> int:
        return sum(values)

    p = pipeline(
        name="scope_test",
        params=Params,
        steps=[step("a", fn=fn_a)],
        observers=[Observer(rec.record)],
    )
    run(p, params=Params(values=[1, 2, 3]))

    started = next(
        ctx
        for _, ctx in rec.events
        if isinstance(ctx, StepStartedContext) and ctx.step_name == "a"
    )
    assert started.pipeline_scope == "scope_test"
    assert started.step_index_in_scope == 1
    assert started.step_total_in_scope == 1


def test_given_step_completed_context_carries_scope_index_and_total():
    """Same fields fire when a step completes successfully."""
    rec = EventRecorder()

    def fn_a(values: list[int]) -> int:
        return sum(values)

    p = pipeline(
        name="scope_test_done",
        params=Params,
        steps=[step("a", fn=fn_a)],
        observers=[Observer(rec.record)],
    )
    run(p, params=Params(values=[1, 2, 3]))

    completed = next(
        ctx
        for _, ctx in rec.events
        if isinstance(ctx, StepCompletedContext) and ctx.step_name == "a"
    )
    assert completed.pipeline_scope == "scope_test_done"
    assert completed.step_index_in_scope == 1
    assert completed.step_total_in_scope == 1


def test_given_step_failed_context_carries_scope_index_and_total():
    """Same fields fire when a step fails."""
    rec = EventRecorder()

    def boom(values: list[int]) -> int:
        raise RuntimeError("kaboom")

    p = pipeline(
        name="scope_fail",
        params=Params,
        steps=[step("a", fn=boom, on_error="stop")],
        observers=[Observer(rec.record)],
    )
    try:
        run(p, params=Params(values=[1]))
    except RuntimeError:
        pass

    failed = next(
        ctx
        for _, ctx in rec.events
        if isinstance(ctx, StepFailedContext) and ctx.step_name == "a"
    )
    assert failed.pipeline_scope == "scope_fail"
    assert failed.step_index_in_scope == 1
    assert failed.step_total_in_scope == 1


def test_given_step_in_sub_pipeline_reports_sub_pipeline_scope():
    """A step expanded inside an include reports the *sub-pipeline*
    scope, not the top-level pipeline scope. The adapter step
    reports the caller's scope (mirrors the DagNode.pipeline stamp).
    This is the runtime counterpart to design-time assertion in
    test_dag_scope.py::test_single_sub_pipeline_step_scopes."""

    class InnerParams(NamedTuple):
        text: str = ""

    def fn_inner(text: str) -> int:
        return len(text)

    def fn_export(fn_inner: int) -> int:
        return fn_inner * 10

    def adapter_fn(raw_strings: list[str]) -> Iterator[InnerParams]:
        # All-mode include consumes the adapter's output once and
        # dispatches per-instance to the inner pipeline.
        for s in raw_strings:
            yield InnerParams(text=s)

    class OuterParams(NamedTuple):
        raw_strings: list[str]

    sub = pipeline(
        name="Inner",
        params=InnerParams,
        exports="fn_export",
        steps=[
            step("fn_inner", fn=fn_inner),
            step("fn_export", fn=fn_export),
        ],
    )
    rec = EventRecorder()
    p = pipeline(
        name="OuterTwoLevel",
        params=OuterParams,
        steps=[include("inner", pipeline=sub, fn=adapter_fn)],
        observers=[Observer(rec.record)],
    )
    run(p, params=OuterParams(raw_strings=["a", "bb"]))

    started_by_name = {
        ctx.step_name: ctx
        for _, ctx in rec.events
        if isinstance(ctx, StepStartedContext)
    }
    # Adapter reports the *caller's* scope.
    assert started_by_name["inner__adapter"].pipeline_scope == "OuterTwoLevel"
    # Inner sub-step reports the *sub-pipeline's* scope.
    assert started_by_name["inner__fn_inner"].pipeline_scope == "Inner"
    # Exported inner step collapses onto the include name ("inner").
    # NOTE: in an orphan include (no downstream consumer), the export
    # collapse is a dag node but may not actually execute in the run.
    # We test the dag-level assertion of its scope directly instead.
    inner_dag_node = p.dag.steps["inner"]
    assert inner_dag_node.pipeline == "Inner"
    assert inner_dag_node.step_total_in_scope == 2


def test_given_step_index_in_scope_starts_at_one():
    """Issue #105: indexing is 1-indexed (1..total), not 0..total-1.
    Verifies by reading the values emitted to a real observer."""
    rec = EventRecorder()

    def fn_a(values: list[int]) -> int:
        return values[0]

    def fn_b(fn_a: int) -> int:
        return fn_a + 1

    def fn_c(fn_b: int) -> int:
        return fn_b + 1

    p = pipeline(
        name="one_indexed",
        params=Params,
        steps=[
            step("fn_a", fn=fn_a),
            step("fn_b", fn=fn_b),
            step("fn_c", fn=fn_c),
        ],
        observers=[Observer(rec.record)],
    )
    run(p, params=Params(values=[7]))

    started = {
        ctx.step_name: ctx
        for _, ctx in rec.events
        if isinstance(ctx, StepStartedContext)
    }
    # First step: index=1 (not 0); all share same total (3).
    assert started["fn_a"].step_index_in_scope == 1
    assert started["fn_a"].step_total_in_scope == 3
    assert started["fn_b"].step_index_in_scope == 2
    assert started["fn_b"].step_total_in_scope == 3
    assert started["fn_c"].step_index_in_scope == 3
    assert started["fn_c"].step_total_in_scope == 3


def test_given_pipeline_started_context_does_not_carry_step_scope_fields():
    """PipelineStartedContext is intentionally unchanged: consumer
    derives per-scope totals from step_started events themselves."""

    class _Empty(NamedTuple):
        pass

    rec = EventRecorder()

    def fn_only(values: list[int]) -> int:
        return sum(values)

    p = pipeline(
        name="pl_started",
        params=Params,
        steps=[step("only", fn=fn_only)],
        observers=[Observer(rec.record)],
    )
    run(p, params=Params(values=[1, 2, 3]))

    started = next(
        ctx for _, ctx in rec.events if isinstance(ctx, PipelineStartedContext)
    )
    # Step-scope fields must NOT leak into pipeline-level contexts:
    assert not hasattr(started, "pipeline_scope") or started.pipeline_scope in (
        None,
        "",
    )
    assert (
        not hasattr(started, "step_index_in_scope") or started.step_index_in_scope == 0
    )
    assert (
        not hasattr(started, "step_total_in_scope") or started.step_total_in_scope == 0
    )
