from synaflow.execution.adapters import async_adapter
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Iterator, NamedTuple

import pytest

from synaflow import (
    ExecutionOverrides,
    Observer,
    PIPELINE_SCOPE,
    PipelineEvent,
    ResourceRegistry,
    Scope,
    StepEvent,
    async_run,
    include,
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


def test_given_execution_overrides_empty_when_materializer_not_overridden_then_compiled_callable_is_kept():
    class Params(NamedTuple):
        count: int = 1

    def gen(count: int) -> Iterator[int]:
        yield from range(count)

    def consume(items: list[int]) -> None:
        return None

    p = pipeline(
        name="compiled_materializer_empty",
        params=Params,
        steps=[
            step("items", fn=gen),
            step("consume", fn=consume),
        ],
    )

    overrides = ExecutionOverrides.empty(p)

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


def test_given_execution_overrides_empty_when_observers_not_overridden_then_all_observers_are_silenced(
    run_pipeline,
):
    class Params(NamedTuple):
        value: int = 1

    events = []

    def record_pipeline(ctx):
        events.append(type(ctx).__name__)

    def record_step(ctx):
        events.append(type(ctx).__name__)

    def emit(value: int) -> int:
        return value

    p = pipeline(
        name="observer_step_override",
        params=Params,
        steps=[step("emit", fn=emit, observers=[Observer(lambda ctx: None)])],
        observers=[Observer(record_pipeline)],
    )

    overrides = ExecutionOverrides.empty(p)

    run_pipeline(p, Params(), overrides=overrides)

    assert events == []


def test_given_execution_overrides_from_production_when_observers_not_overridden_then_compiled_observers_are_kept(
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
        name="observer_step_from_production",
        params=Params,
        steps=[step("emit", fn=emit, observers=[Observer(record_step)])],
        observers=[Observer(record_pipeline)],
    )

    overrides = ExecutionOverrides.from_production(p)

    run_pipeline(p, Params(), overrides=overrides)

    assert pipeline_events == ["PipelineStartedContext", "PipelineCompletedContext"]
    assert step_events == ["StepStartedContext", "StepCompletedContext"]


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

    assert pipeline_events == []
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
        observers=[Observer(async_adapter(lambda ctx: None))],
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

    def record(ctx):
        return None

    with pytest.raises(TypeError, match="must be a list of observers"):
        overrides.observers[PIPELINE_SCOPE] = record

    with pytest.raises(
        TypeError,
        match="must contain only callables or Observer registrations",
    ):
        overrides.observers[PIPELINE_SCOPE] = [123]


def test_given_scope_key_when_materializer_overridden_in_sub_pipeline_then_override_is_used(
    run_pipeline,
):
    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 3

    def prepare(value: int) -> Iterator[int]:
        yield value
        yield value + 1

    def finish(prepare: list[int]) -> int:
        return sum(prepare)

    sub = pipeline(
        name="sub",
        params=SubParams,
        steps=[
            step("prepare", fn=prepare),
            step("finish", fn=finish),
        ],
        exports="finish",
    )

    def adapt(value: int) -> SubParams:
        return SubParams(value=value)

    captured = []

    def consume(incl: int) -> None:
        captured.append(incl)

    p = pipeline(
        name="scope_materializer_override",
        params=Params,
        steps=[
            include("incl", pipeline=sub, fn=adapt),
            step("consume", fn=consume),
        ],
    )

    overrides = ExecutionOverrides.empty(p)
    sub_scope = Scope("incl")
    overrides.materializers[sub_scope.scope("prepare")] = lambda items: [
        item + 1 for item in items
    ]

    run_pipeline(p, Params(), overrides=overrides)

    assert captured == [9]


def test_given_scope_key_when_observer_overridden_in_sub_pipeline_then_override_is_used(
    run_pipeline,
):
    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 3

    events = []

    def record(ctx):
        if ctx.event in (StepEvent.STARTED, StepEvent.COMPLETED):
            events.append((ctx.step_name, type(ctx).__name__))

    def prepare(value: int) -> int:
        return value * 2

    def finish(prepare: int) -> int:
        return prepare

    sub = pipeline(
        name="sub",
        params=SubParams,
        steps=[
            step("prepare", fn=prepare, observers=[Observer(lambda ctx: None)]),
            step("finish", fn=finish),
        ],
        exports="finish",
    )

    def adapt(value: int) -> SubParams:
        return SubParams(value=value)

    def consume(incl: int) -> None:
        return None

    p = pipeline(
        name="scope_observer_override",
        params=Params,
        steps=[
            include("incl", pipeline=sub, fn=adapt),
            step("consume", fn=consume),
        ],
    )

    overrides = ExecutionOverrides.empty(p)
    sub_scope = Scope("incl")
    overrides.observers[sub_scope.scope("prepare")] = [Observer(record)]

    run_pipeline(p, Params(), overrides=overrides)

    assert events == [
        ("incl__prepare", "StepStartedContext"),
        ("incl__prepare", "StepCompletedContext"),
    ]


def test_given_empty_resource_overrides_when_pipeline_resource_factory_exists_then_factory_is_used(
    run_pipeline,
):
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 1

    seen = []

    def use(db: DB, value: int) -> None:
        seen.append((db, value))

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="missing_resource",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)

    run_pipeline(p, Params(), overrides=overrides)

    assert len(seen) == 1
    assert isinstance(seen[0][0], DB)
    assert seen[0][1] == 1


def test_given_pipeline_resource_factory_when_run_without_override_then_resource_is_injected(
    run_pipeline,
):
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 1

    seen = []

    def use(db: DB, value: int) -> None:
        seen.append((db, value))

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="missing_resource_registry",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    run_pipeline(p, Params())

    assert len(seen) == 1
    assert isinstance(seen[0][0], DB)
    assert seen[0][1] == 1


def test_given_pipeline_resource_factory_used_by_multiple_steps_when_run_then_factory_is_called_per_step(
    run_pipeline,
):
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 1

    created = []
    seen = []

    def get_db() -> DB:
        db = DB()
        created.append(db)
        return db

    def first(db: DB, value: int) -> int:
        seen.append(("first", db, value))
        return value

    def second(db: DB, first: int) -> None:
        seen.append(("second", db, first))

    p = pipeline(
        name="resource_factory_per_step",
        params=Params,
        resources={"db": get_db},
        steps=[step("first", fn=first), step("second", fn=second)],
    )

    run_pipeline(p, Params())

    assert len(created) == 2
    assert seen[0] == ("first", created[0], 1)
    assert seen[1] == ("second", created[1], 1)


def test_given_resource_override_when_sync_run_then_resource_is_injected(run_pipeline):
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 3

    seen = []

    def use(db: DB, value: int) -> None:
        seen.append((db, value))

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="sync_resource_override",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)
    db = DB()
    overrides.resources["db"] = db

    run_pipeline(p, Params(), overrides=overrides)

    assert seen == [(db, 3)]


async def test_given_resource_override_when_async_run_then_resource_is_injected():
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 3

    seen = []

    async def use(db: DB, value: int) -> None:
        seen.append((db, value))

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="async_resource_override",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)
    db = DB()
    overrides.resources["db"] = db

    await async_run(p, Params(), overrides=overrides)

    assert seen == [(db, 3)]


def test_given_resource_override_when_pipeline_factory_exists_then_override_takes_precedence(
    run_pipeline,
):
    class DB:
        def __init__(self, source: str):
            self.source = source

    class Params(NamedTuple):
        value: int = 3

    seen = []

    def use(db: DB, value: int) -> None:
        seen.append((db.source, value))

    def get_db() -> DB:
        return DB("pipeline")

    p = pipeline(
        name="resource_override_precedence",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.resources["db"] = DB("override")

    run_pipeline(p, Params(), overrides=overrides)

    assert seen == [("override", 3)]


def test_given_resource_factory_override_when_sync_run_then_override_factory_is_used(
    run_pipeline,
):
    class DB:
        def __init__(self, source: str):
            self.source = source

    class Params(NamedTuple):
        value: int = 3

    seen = []

    def use(db: DB, value: int) -> None:
        seen.append((db.source, value))

    def get_db() -> DB:
        return DB("pipeline")

    p = pipeline(
        name="resource_override_factory_sync",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.resources["db"] = lambda: DB("override-factory")

    run_pipeline(p, Params(), overrides=overrides)

    assert seen == [("override-factory", 3)]


async def test_given_resource_factory_override_when_async_run_then_override_factory_is_used():
    class DB:
        def __init__(self, source: str):
            self.source = source

    class Params(NamedTuple):
        value: int = 3

    seen = []

    async def use(db: DB, value: int) -> None:
        seen.append((db.source, value))

    def get_db() -> DB:
        return DB("pipeline")

    p = pipeline(
        name="resource_override_factory_async",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.resources["db"] = lambda: DB("override-factory")

    await async_run(p, Params(), overrides=overrides)

    assert seen == [("override-factory", 3)]


def test_given_resource_context_manager_factory_when_sync_run_then_entered_value_is_injected(
    run_pipeline,
):
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 5

    entered = []
    exited = []
    seen = []

    @contextmanager
    def get_db() -> DB:
        db = DB()
        entered.append(db)
        try:
            yield db
        finally:
            exited.append(db)

    def use(db: DB, value: int) -> None:
        seen.append((db, value))

    p = pipeline(
        name="sync_resource_context_manager",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    run_pipeline(p, Params())

    assert len(entered) == 1
    assert seen == [(entered[0], 5)]
    assert exited == entered


def test_given_resource_context_manager_override_when_sync_run_then_entered_value_is_injected(
    run_pipeline,
):
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 5

    entered = []
    exited = []
    seen = []

    def get_db() -> DB:
        return DB()

    @contextmanager
    def override_db() -> DB:
        db = DB()
        entered.append(db)
        try:
            yield db
        finally:
            exited.append(db)

    def use(db: DB, value: int) -> None:
        seen.append((db, value))

    p = pipeline(
        name="sync_resource_context_manager_override",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.resources["db"] = override_db

    run_pipeline(p, Params(), overrides=overrides)

    assert len(entered) == 1
    assert seen == [(entered[0], 5)]
    assert exited == entered


async def test_given_async_resource_context_manager_factory_when_async_run_then_entered_value_is_injected():
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 5

    entered = []
    exited = []
    seen = []

    @asynccontextmanager
    async def get_db() -> DB:
        db = DB()
        entered.append(db)
        try:
            yield db
        finally:
            exited.append(db)

    async def use(db: DB, value: int) -> None:
        seen.append((db, value))

    p = pipeline(
        name="async_resource_context_manager",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    await async_run(p, Params())

    assert len(entered) == 1
    assert seen == [(entered[0], 5)]
    assert exited == entered


async def test_given_async_resource_context_manager_override_when_async_run_then_entered_value_is_injected():
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 5

    entered = []
    exited = []
    seen = []

    def get_db() -> DB:
        return DB()

    @asynccontextmanager
    async def override_db() -> DB:
        db = DB()
        entered.append(db)
        try:
            yield db
        finally:
            exited.append(db)

    async def use(db: DB, value: int) -> None:
        seen.append((db, value))

    p = pipeline(
        name="async_resource_context_manager_override",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.resources["db"] = override_db

    await async_run(p, Params(), overrides=overrides)

    assert len(entered) == 1
    assert seen == [(entered[0], 5)]
    assert exited == entered


def test_given_async_resource_context_manager_factory_when_sync_run_then_raises_clear_error(
    run_pipeline,
):
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 1

    @asynccontextmanager
    async def get_db() -> DB:
        yield DB()

    def use(db: DB, value: int) -> None:
        return None

    p = pipeline(
        name="sync_rejects_async_resource_context_manager",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    with pytest.raises(TypeError, match="produced an async context manager"):
        run_pipeline(p, Params())


def test_given_async_resource_context_manager_override_when_sync_run_then_raises_clear_error(
    run_pipeline,
):
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 1

    def get_db() -> DB:
        return DB()

    @asynccontextmanager
    async def override_db() -> DB:
        yield DB()

    def use(db: DB, value: int) -> None:
        return None

    p = pipeline(
        name="sync_rejects_async_resource_context_manager_override",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)
    overrides.resources["db"] = override_db

    with pytest.raises(TypeError, match="produced an async context manager"):
        run_pipeline(p, Params(), overrides=overrides)


def test_given_execution_overrides_from_production_when_resources_requested_then_registry_is_empty_but_keyed():
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 1

    def use(db: DB, value: int) -> None:
        return None

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="resource_contract",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.from_production(p)

    assert isinstance(overrides.resources, ResourceRegistry)
    assert list(overrides.resources) == ["db"]
    with pytest.raises(KeyError):
        _ = overrides.resources["db"]


def test_given_unknown_resource_override_key_when_assigned_then_raises():
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 1

    def use(db: DB, value: int) -> None:
        return None

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="invalid_resource_key",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)

    with pytest.raises(KeyError, match="Unknown override key 'missing'"):
        overrides.resources["missing"] = DB()


def test_given_none_resource_override_value_when_assigned_then_raises():
    class DB:
        pass

    class Params(NamedTuple):
        value: int = 1

    def use(db: DB, value: int) -> None:
        return None

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="invalid_resource_value",
        params=Params,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
    )

    overrides = ExecutionOverrides.empty(p)

    with pytest.raises(TypeError, match="cannot be None"):
        overrides.resources["db"] = None


def test_given_sub_pipeline_resource_when_overridden_then_resource_is_injected_into_included_step(
    run_pipeline,
):
    class DB:
        pass

    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 3

    seen = []

    def use(db: DB, value: int) -> int:
        seen.append((db, value))
        return value

    def get_db() -> DB:
        return DB()

    sub = pipeline(
        name="sub",
        params=SubParams,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
        exports="use",
    )

    def adapt(value: int) -> SubParams:
        return SubParams(value=value)

    def consume(incl: int) -> None:
        return None

    p = pipeline(
        name="parent",
        params=Params,
        steps=[
            include("incl", pipeline=sub, fn=adapt),
            step("consume", fn=consume),
        ],
    )

    overrides = ExecutionOverrides.empty(p)
    db = DB()
    overrides.resources["db"] = db

    run_pipeline(p, Params(), overrides=overrides)

    assert seen == [(db, 3)]
