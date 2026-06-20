from typing import AsyncGenerator, Iterator, NamedTuple

import pytest

from synaflow import ExecutionOverrides, async_run, pipeline, step


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
