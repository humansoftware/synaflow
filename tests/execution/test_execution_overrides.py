from typing import AsyncGenerator, Iterator, NamedTuple

import pytest

from synaflow import (
    ExecutionOverrides,
    Observer,
    PIPELINE_SCOPE,
    PipelineEvent,
    StepEvent,
    async_run,
    pipeline,
    step,
)


def test_given_materializer_override_when_sync_run_then_override_is_used(
    run_pipeline,
):
    class Params(NamedTuple):
        count: int = 3

    def gen(count: int) -> Iterator[int]:
        yield from range(count)

    captured = []

    def consume(items: list[int]) -> None:
        captured.append(items)

    p = pipeline(
        name="sync_override",
        params=Params,
        steps=[
            step("items", fn=gen),
            step("consume", fn=consume),
        ],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.materializers["items"] = tuple

    run_pipeline(p, Params(), overrides=overrides)

    assert captured == [(0, 1, 2)]


async def test_given_materializer_override_when_async_run_then_override_is_used():
    class Params(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for item in range(count):
            yield item

    captured = []

    async def consume(items: list[int]) -> None:
        captured.append(items)

    p = pipeline(
        name="async_override",
        params=Params,
        steps=[
            step("items", fn=gen),
            step("consume", fn=consume),
        ],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.materializers["items"] = tuple

    await async_run(p, Params(), overrides=overrides)

    assert captured == [(0, 1, 2)]


def test_given_execution_overrides_from_production_when_materializer_requested_then_returns_compiled_callable():
    class Params(NamedTuple):
        count: int = 1

    def gen(count: int) -> Iterator[int]:
        yield from range(count)

    def consume(items: list[int]) -> None:
        return None

    p = pipeline(
        name="compiled_materializer_contract",
        params=Params,
        steps=[
            step("items", fn=gen),
            step("consume", fn=consume),
        ],
    )

    overrides = ExecutionOverrides.from_production(p)

    assert list(overrides.materializers) == ["items"]
    assert overrides.materializers["items"] is list


def test_given_unknown_materializer_override_key_when_assigned_then_raises():
    class Params(NamedTuple):
        value: int = 1

    def emit(value: int) -> int:
        return value

    p = pipeline(
        name="invalid_override_key",
        params=Params,
        steps=[step("emit", fn=emit)],
    )

    overrides = ExecutionOverrides.empty(p)

    with pytest.raises(KeyError, match="Unknown override key 'missing'"):
        overrides.materializers["missing"] = tuple


def test_given_non_callable_materializer_override_when_assigned_then_raises():
    class Params(NamedTuple):
        count: int = 1

    def gen(count: int) -> Iterator[int]:
        yield from range(count)

    def consume(items: list[int]) -> None:
        return None

    p = pipeline(
        name="invalid_override_value",
        params=Params,
        steps=[
            step("items", fn=gen),
            step("consume", fn=consume),
        ],
    )

    overrides = ExecutionOverrides.empty(p)

    with pytest.raises(TypeError, match="must be callable"):
        overrides.materializers["items"] = 123


def test_given_pipeline_observer_override_when_sync_run_then_pipeline_and_step_events_use_override(
    run_pipeline,
):
    class Params(NamedTuple):
        value: int = 1

    events = []

    def record(ctx):
        events.append(type(ctx).__name__)

    def emit(value: int) -> int:
        return value

    p = pipeline(
        name="observer_pipeline_override",
        params=Params,
        steps=[step("emit", fn=emit)],
        observers=[Observer(lambda ctx: None)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.observers[PIPELINE_SCOPE] = [Observer(record)]

    run_pipeline(p, Params(), overrides=overrides)

    assert "PipelineStartedContext" in events
    assert "PipelineCompletedContext" in events
    assert "StepStartedContext" in events
    assert "StepCompletedContext" in events


def test_given_step_observer_override_when_sync_run_then_only_step_events_use_override(
    run_pipeline,
):
    class Params(NamedTuple):
        value: int = 1

    pipeline_events = []
    step_events = []

    def record_pipeline(ctx):
        if ctx.event in (PipelineEvent.STARTED, PipelineEvent.COMPLETED):
            pipeline_events.append(type(ctx).__name__)

    def record_step(ctx):
        if ctx.event in (StepEvent.STARTED, StepEvent.COMPLETED):
            step_events.append(type(ctx).__name__)

    def emit(value: int) -> int:
        return value

    p = pipeline(
        name="observer_step_override",
        params=Params,
        steps=[step("emit", fn=emit, observers=[Observer(lambda ctx: None)])],
        observers=[Observer(record_pipeline)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.observers["emit"] = [Observer(record_step)]

    run_pipeline(p, Params(), overrides=overrides)

    assert pipeline_events == ["PipelineStartedContext", "PipelineCompletedContext"]
    assert step_events == ["StepStartedContext", "StepCompletedContext"]


async def test_given_pipeline_observer_override_when_async_run_then_pipeline_and_step_events_use_override():
    class Params(NamedTuple):
        value: int = 1

    events = []

    async def record(ctx):
        events.append(type(ctx).__name__)

    async def emit(value: int) -> int:
        return value

    p = pipeline(
        name="async_observer_pipeline_override",
        params=Params,
        steps=[step("emit", fn=emit)],
        observers=[Observer(lambda ctx: None)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.observers[PIPELINE_SCOPE] = [Observer(record)]

    await async_run(p, Params(), overrides=overrides)

    assert "PipelineStartedContext" in events
    assert "PipelineCompletedContext" in events
    assert "StepStartedContext" in events
    assert "StepCompletedContext" in events


def test_given_invalid_observer_override_key_when_assigned_then_raises():
    class Params(NamedTuple):
        value: int = 1

    def emit(value: int) -> int:
        return value

    p = pipeline(
        name="invalid_observer_key",
        params=Params,
        steps=[step("emit", fn=emit)],
        observers=[Observer(lambda ctx: None)],
    )

    overrides = ExecutionOverrides.empty(p)

    with pytest.raises(KeyError, match="Unknown override key 'missing'"):
        overrides.observers["missing"] = [Observer(lambda ctx: None)]


def test_given_invalid_observer_override_value_when_assigned_then_raises():
    class Params(NamedTuple):
        value: int = 1

    def emit(value: int) -> int:
        return value

    p = pipeline(
        name="invalid_observer_value",
        params=Params,
        steps=[step("emit", fn=emit)],
        observers=[Observer(lambda ctx: None)],
    )

    overrides = ExecutionOverrides.empty(p)

    record = lambda ctx: None

    with pytest.raises(TypeError, match="must be a list of observers"):
        overrides.observers[PIPELINE_SCOPE] = record

    with pytest.raises(
        TypeError,
        match="must contain only callables or Observer registrations",
    ):
        overrides.observers[PIPELINE_SCOPE] = [123]
