import logging
import pytest
from typing import NamedTuple, Generator

from synaflow import (
    pipeline,
    step,
    run,
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

    def on_started(ctx):
        events.append(("pipeline_started", ctx.pipeline_name))

    def on_completed(ctx):
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

    run(p, ObserversParams(count=5))

    assert events == [
        ("pipeline_started", "p1"),
        ("pipeline_completed", "p1"),
    ]


def test_given_failing_pipeline_when_runs_then_emits_pipeline_failed():
    events = []

    def on_failed(ctx):
        events.append(
            (
                "pipeline_failed",
                ctx.pipeline_name,
                ctx.step_name,
                type(ctx.exception).__name__,
            )
        )

    def fail_step(count: int) -> int:
        raise ValueError("Oops")

    p = pipeline(
        name="p_fail",
        params=ObserversParams,
        observers=[Observer(PipelineEvent.FAILED, on_failed)],
        steps=[step("s1", fn=fail_step, on_error=OnError.STOP)],
    )

    with pytest.raises(PipelineStopException) as excinfo:
        run(p, ObserversParams(count=5))

    assert "s1" in str(excinfo.value)
    assert events == [
        ("pipeline_failed", "p_fail", "s1", "ValueError"),
    ]


def test_given_all_mode_step_when_runs_then_emits_step_started_completed():
    events = []

    def on_step_started(ctx):
        events.append(("started", ctx.step_name, ctx.mode.value, ctx.on_error.value))

    def on_step_completed(ctx):
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

    run(p, ObserversParams(count=2))

    assert events == [
        ("started", "s1", "all", "continue"),
        ("completed", "s1", 1, 0, True),
    ]


def test_given_each_mode_step_with_continue_when_runs_then_emits_completed_with_counts():
    events = []

    def on_completed(ctx):
        events.append(
            (
                ctx.step_name,
                ctx.success_count,
                ctx.error_count,
                ctx.completed_all_inputs,
            )
        )

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def process(gen: int) -> int:
        if gen == 1:
            raise ValueError("Failure on 1")
        return gen * 10

    # Gen is all-mode, process is each-mode because it accepts gen (Iterator) as individual item (int)
    p = pipeline(
        name="p_each_continue",
        params=ObserversParams,
        observers=[Observer(StepEvent.COMPLETED, on_completed)],
        steps=[
            step("gen", fn=gen),
            step("process", fn=process, on_error=OnError.CONTINUE),
        ],
    )

    run(p, ObserversParams(count=3))  # gen yields 0, 1, 2. item 1 will fail.

    # "gen" is all-mode returning Iterator (wrapped) -> success_count=1, completed_all_inputs=True
    # "process" is each-mode -> success_count=2 (for 0 and 2), error_count=1 (for 1), completed_all_inputs=True
    # Wait, the order of completion depends on execution. "gen" finishes yielding, then "process" finishes consuming.
    assert len(events) == 2
    # Ensure both are completed
    gen_completed = [ev for ev in events if ev[0] == "gen"][0]
    proc_completed = [ev for ev in events if ev[0] == "process"][0]

    assert gen_completed == ("gen", 1, 0, True)
    assert proc_completed == ("process", 2, 1, True)


def test_given_each_mode_step_with_stop_when_runs_then_emits_failed_with_counts():
    events = []

    def on_failed(ctx):
        events.append(
            (
                ctx.step_name,
                ctx.success_count,
                ctx.error_count,
                ctx.completed_all_inputs,
                type(ctx.exception).__name__,
            )
        )

    def gen(count: int) -> Generator[int, None, None]:
        yield 0
        yield 1
        yield 2

    def process(gen: int) -> int:
        if gen == 1:
            raise ValueError("Failure on 1")
        return gen * 10

    # Gen is all-mode, process is each-mode with STOP
    p = pipeline(
        name="p_each_stop",
        params=ObserversParams,
        observers=[Observer(StepEvent.FAILED, on_failed)],
        steps=[
            step("gen", fn=gen),
            step("process", fn=process, on_error=OnError.STOP),
        ],
    )

    with pytest.raises(PipelineStopException):
        run(p, ObserversParams(count=3))

    # "process" should fail with success_count=1 (for 0), error_count=1 (for 1), completed_all_inputs=False
    assert len(events) == 1
    assert events[0] == ("process", 1, 1, False, "ValueError")


def test_given_materializer_when_runs_then_emits_materialization_events():
    events = []

    def on_mat_started(ctx):
        events.append(("started", ctx.step_name, ctx.materializer_name))

    def on_mat_completed(ctx):
        events.append(("completed", ctx.step_name, ctx.materializer_name))

    p = pipeline(
        name="p_mat",
        params=ObserversParams,
        observers=[
            Observer(MaterializationEvent.STARTED, on_mat_started),
            Observer(MaterializationEvent.COMPLETED, on_mat_completed),
        ],
        steps=[step("s1", fn=lambda count: list(range(count)), force_materialize=True)],
    )

    run(p, ObserversParams(count=2))

    # Since force_materialize=True, it runs materialization
    assert len(events) == 2
    assert events[0][0] == "started"
    assert events[0][1] == "s1"
    assert events[1][0] == "completed"
    assert events[1][1] == "s1"


def test_given_failing_observer_when_runs_then_swallows_exception_and_logs(caplog):
    def bad_handler(ctx):
        raise RuntimeError("I am bad")

    p = pipeline(
        name="p_bad_obs",
        params=ObserversParams,
        observers=[Observer(PipelineEvent.STARTED, bad_handler)],
        steps=[step("s1", fn=lambda count: count * 3)],
    )

    # Execution must succeed even if the observer throws an error
    with caplog.at_level(logging.WARNING, logger="synaflow"):
        run(p, ObserversParams(count=2))

    # The exception must be logged as warning
    assert any(
        "Observer failed for event" in record.message for record in caplog.records
    )
