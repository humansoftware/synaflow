from collections.abc import AsyncGenerator, Generator
from typing import Any
import pytest
from synaflow.execution.lifecycle_stream import LifecycleStream, AsyncLifecycleStream


def test_sync_lifecycle_stream() -> None:
    events: list[str] = []

    def on_start() -> None:
        events.append("start")

    def on_item(x: Any) -> None:
        events.append(f"item:{x}")

    def on_end(count: int) -> None:
        events.append(f"end:{count}")

    def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{count}")

    # Test success path
    source = iter([1, 2])
    stream = LifecycleStream(source, on_start, on_item, on_end, on_error)
    assert next(stream) == 1
    assert next(stream) == 2
    with pytest.raises(StopIteration):
        next(stream)

    assert events == ["start", "item:1", "item:2", "end:2"]

    # Test error path
    events.clear()

    def failing_gen() -> Generator[int, None, None]:
        yield 10
        raise ValueError("Boom")

    stream2 = LifecycleStream(failing_gen(), on_start, on_item, on_end, on_error)
    assert next(stream2) == 10
    with pytest.raises(ValueError, match="Boom"):
        next(stream2)
    assert events == ["start", "item:10", "error:1"]


@pytest.mark.asyncio
async def test_async_lifecycle_stream() -> None:
    events: list[str] = []

    async def on_start() -> None:
        events.append("start")

    async def on_item(x: Any) -> None:
        events.append(f"item:{x}")

    async def on_end(count: int) -> None:
        events.append(f"end:{count}")

    async def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{count}")

    # Test success path (async iterator source)
    async def async_source() -> AsyncGenerator[int, None]:
        yield 1
        yield 2

    stream = AsyncLifecycleStream(async_source(), on_start, on_item, on_end, on_error)
    assert await anext(stream) == 1
    assert await anext(stream) == 2
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["start", "item:1", "item:2", "end:2"]

    # Test error path (sync iterator source in async wrapper)
    events.clear()

    def failing_gen() -> Generator[int, None, None]:
        yield 10
        raise ValueError("Boom")

    stream2 = AsyncLifecycleStream(failing_gen(), on_start, on_item, on_end, on_error)
    assert await anext(stream2) == 10
    with pytest.raises(ValueError, match="Boom"):
        await anext(stream2)
    assert events == ["start", "item:10", "error:1"]


def test_sync_lifecycle_stream_multiple_calls_after_terminal_state() -> None:
    events: list[str] = []

    def on_start() -> None:
        events.append("start")

    def on_item(x: Any) -> None:
        events.append(f"item:{x}")

    def on_end(count: int) -> None:
        events.append(f"end:{count}")

    def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{count}")

    # Case 1: After normal completion (StopIteration)
    source = iter([1])
    stream = LifecycleStream(source, on_start, on_item, on_end, on_error)
    assert next(stream) == 1
    with pytest.raises(StopIteration):
        next(stream)

    # Verify events
    assert events == ["start", "item:1", "end:1"]

    # Call next again
    with pytest.raises(StopIteration):
        next(stream)

    # Events should remain unchanged (no on_end called again)
    assert events == ["start", "item:1", "end:1"]

    # Case 2: After failure (error callback)
    events.clear()

    def failing_gen() -> Generator[int, None, None]:
        yield 10
        raise ValueError("Boom")

    stream2 = LifecycleStream(failing_gen(), on_start, on_item, on_end, on_error)
    assert next(stream2) == 10
    with pytest.raises(ValueError, match="Boom"):
        next(stream2)

    assert events == ["start", "item:10", "error:1"]

    # Call next again - should raise StopIteration without invoking on_error/on_end again!
    with pytest.raises(StopIteration):
        next(stream2)

    assert events == ["start", "item:10", "error:1"]


@pytest.mark.asyncio
async def test_async_lifecycle_stream_multiple_calls_after_terminal_state() -> None:
    events: list[str] = []

    async def on_start() -> None:
        events.append("start")

    async def on_item(x: Any) -> None:
        events.append(f"item:{x}")

    async def on_end(count: int) -> None:
        events.append(f"end:{count}")

    async def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{count}")

    # Case 1: After normal completion
    async def async_source() -> AsyncGenerator[int, None]:
        yield 1

    stream = AsyncLifecycleStream(async_source(), on_start, on_item, on_end, on_error)
    assert await anext(stream) == 1
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["start", "item:1", "end:1"]

    # Call anext again
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["start", "item:1", "end:1"]

    # Case 2: After failure
    events.clear()

    def failing_gen() -> Generator[int, None, None]:
        yield 10
        raise ValueError("Boom")

    stream2 = AsyncLifecycleStream(failing_gen(), on_start, on_item, on_end, on_error)
    assert await anext(stream2) == 10
    with pytest.raises(ValueError, match="Boom"):
        await anext(stream2)

    assert events == ["start", "item:10", "error:1"]

    # Call anext again
    with pytest.raises(StopAsyncIteration):
        await anext(stream2)

    assert events == ["start", "item:10", "error:1"]
