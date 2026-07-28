from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import NamedTuple

import pytest

from synaflow import PipelineRegistry, pipeline, async_run, step
from synaflow.core.exceptions import PipelineStopException
from synaflow.core.types import OnError


class Params(NamedTuple):
    count: int = 3


@pytest.mark.asyncio
async def test_cm_resource_entered_and_exited_per_item_in_each_mode():
    events = []

    @asynccontextmanager
    async def tracking_cm(name: str) -> AsyncGenerator[str, None]:
        events.append(f"{name}: enter")
        try:
            yield name
        finally:
            events.append(f"{name}: exit")

    def cm_factory() -> str:
        return tracking_cm("write_db")  # type: ignore

    async def numbers(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def each_step(numbers: int, cm: str) -> str:
        events.append(f"each_step: processing {numbers} with cm={cm}")
        return f"processed-{numbers}"

    p = pipeline(
        name="test_async_each_resource",
        params=Params,
        resources={"cm": cm_factory},
        steps=[
            step("numbers", fn=numbers),
            step("each_step", fn=each_step),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    await async_run(catalog.get_dag("test_async_each_resource"), Params(count=3))

    expected = [
        "write_db: enter",
        "each_step: processing 0 with cm=write_db",
        "write_db: exit",
        "write_db: enter",
        "each_step: processing 1 with cm=write_db",
        "write_db: exit",
        "write_db: enter",
        "each_step: processing 2 with cm=write_db",
        "write_db: exit",
    ]
    assert events == expected


@pytest.mark.asyncio
async def test_plain_resource_resolved_once_in_each_mode():
    factory_calls = 0

    def plain_factory() -> str:
        nonlocal factory_calls
        factory_calls += 1
        return "plain_res"

    async def numbers(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def each_step(numbers: int, res: str) -> str:
        return f"{res}-{numbers}"

    p = pipeline(
        name="test_async_each_plain_resource",
        params=Params,
        resources={"res": plain_factory},
        steps=[
            step("numbers", fn=numbers),
            step("each_step", fn=each_step),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    await async_run(catalog.get_dag("test_async_each_plain_resource"), Params(count=3))

    assert factory_calls == 1


@pytest.mark.asyncio
async def test_cm_resource_in_all_mode_entered_once():
    events = []

    @asynccontextmanager
    async def tracking_cm(name: str) -> AsyncGenerator[str, None]:
        events.append(f"{name}: enter")
        try:
            yield name
        finally:
            events.append(f"{name}: exit")

    def cm_factory() -> str:
        return tracking_cm("db")  # type: ignore

    async def all_step(cm: str, count: int) -> str:
        events.append(f"all_step: cm={cm}")
        return f"done-{count}"

    p = pipeline(
        name="test_async_all_resource",
        params=Params,
        resources={"cm": cm_factory},
        steps=[
            step("all_step", fn=all_step),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    await async_run(catalog.get_dag("test_async_all_resource"), Params(count=3))

    expected = [
        "db: enter",
        "all_step: cm=db",
        "db: exit",
    ]
    assert events == expected


@pytest.mark.asyncio
async def test_cm_resource_exited_before_downstream_consumer_receives_item():
    events = []

    @asynccontextmanager
    async def db_transaction() -> AsyncGenerator[dict, None]:
        tx = {"committed": False}
        events.append("TX ENTER")
        try:
            yield tx
        finally:
            tx["committed"] = True
            events.append("TX COMMIT")

    def db_factory() -> dict:
        return db_transaction()  # type: ignore

    async def items(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def writer_step(items: int, db: dict) -> int:
        events.append(f"writer_step: processing {items}, committed={db['committed']}")
        return items

    async def reader_step(writer_step: int, db: dict) -> None:
        events.append(
            f"reader_step: processing {writer_step}, committed={db['committed']}"
        )

    p = pipeline(
        name="test_async_downstream_commit_order",
        params=Params,
        resources={"db": db_factory},
        steps=[
            step("items", fn=items),
            step("writer_step", fn=writer_step),
            step("reader_step", fn=reader_step),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    await async_run(
        catalog.get_dag("test_async_downstream_commit_order"), Params(count=2)
    )

    expected = [
        "TX ENTER",
        "writer_step: processing 0, committed=False",
        "TX COMMIT",
        "TX ENTER",
        "reader_step: processing 0, committed=False",
        "TX COMMIT",
        "TX ENTER",
        "writer_step: processing 1, committed=False",
        "TX COMMIT",
        "TX ENTER",
        "reader_step: processing 1, committed=False",
        "TX COMMIT",
    ]
    assert events == expected


@pytest.mark.asyncio
async def test_cm_factory_error_in_each_mode_with_on_error_continue_skips_failed_items():
    call_count = 0

    @asynccontextmanager
    async def good_cm() -> AsyncGenerator[str, None]:
        yield "resource"

    def failing_cm_factory() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("factory boom")
        return good_cm()  # type: ignore

    async def numbers(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    results: list[str] = []

    async def each_step(numbers: int, res: str) -> str:
        result = f"{res}-{numbers}"
        results.append(result)
        return result

    p = pipeline(
        name="test_async_factory_error_continue",
        params=Params,
        resources={"res": failing_cm_factory},
        steps=[
            step("numbers", fn=numbers),
            step("each_step", fn=each_step, on_error=OnError.CONTINUE),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    await async_run(
        catalog.get_dag("test_async_factory_error_continue"), Params(count=3)
    )

    assert results == ["resource-1", "resource-2"]


@pytest.mark.asyncio
async def test_cm_factory_error_in_each_mode_with_on_error_stop_raises():
    call_count = 0

    @asynccontextmanager
    async def good_cm() -> AsyncGenerator[str, None]:
        yield "resource"

    def failing_cm_factory() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("factory boom")
        return good_cm()  # type: ignore

    async def numbers(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def each_step(numbers: int, res: str) -> str:
        return f"{res}-{numbers}"

    p = pipeline(
        name="test_async_factory_error_stop",
        params=Params,
        resources={"res": failing_cm_factory},
        steps=[
            step("numbers", fn=numbers),
            step("each_step", fn=each_step, on_error=OnError.STOP),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    with pytest.raises(PipelineStopException, match="each_step"):
        await async_run(
            catalog.get_dag("test_async_factory_error_stop"), Params(count=3)
        )
