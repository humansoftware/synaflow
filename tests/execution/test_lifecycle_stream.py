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


def test_sync_lifecycle_stream_on_start_fails() -> None:
    events: list[str] = []

    def on_start() -> None:
        events.append("start_fail")
        raise RuntimeError("Start error")

    def on_item(x: Any) -> None:
        events.append(f"item:{x}")

    def on_end(count: int) -> None:
        events.append(f"end:{count}")

    def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{type(exc).__name__}:{count}")

    source = iter([1, 2])
    stream = LifecycleStream(source, on_start, on_item, on_end, on_error)

    # 1. Verify original exception is propagated
    with pytest.raises(RuntimeError, match="Start error"):
        next(stream)

    # 2. Verify on_error was triggered (with count 0 since no items were yielded)
    assert events == ["start_fail", "error:RuntimeError:0"]

    # 3. Verify the stream is marked completed: subsequent calls raise StopIteration
    with pytest.raises(StopIteration):
        next(stream)

    # Verify no additional events were recorded
    assert events == ["start_fail", "error:RuntimeError:0"]


@pytest.mark.asyncio
async def test_async_lifecycle_stream_on_start_fails_async_callback() -> None:
    events: list[str] = []

    async def on_start() -> None:
        events.append("start_fail")
        raise RuntimeError("Start async error")

    async def on_item(x: Any) -> None:
        events.append(f"item:{x}")

    async def on_end(count: int) -> None:
        events.append(f"end:{count}")

    async def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{type(exc).__name__}:{count}")

    async def async_source() -> AsyncGenerator[int, None]:
        yield 1

    stream = AsyncLifecycleStream(async_source(), on_start, on_item, on_end, on_error)

    # 1. Verify original exception is propagated
    with pytest.raises(RuntimeError, match="Start async error"):
        await anext(stream)

    # 2. Verify on_error was triggered
    assert events == ["start_fail", "error:RuntimeError:0"]

    # 3. Verify the stream is marked completed
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["start_fail", "error:RuntimeError:0"]


@pytest.mark.asyncio
async def test_async_lifecycle_stream_on_start_fails_sync_callback() -> None:
    events: list[str] = []

    def on_start() -> None:
        events.append("start_fail")
        raise RuntimeError("Start sync error")

    async def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{type(exc).__name__}:{count}")

    async def async_source() -> AsyncGenerator[int, None]:
        yield 1

    stream = AsyncLifecycleStream(async_source(), on_start, on_error=on_error)

    # 1. Verify original exception is propagated
    with pytest.raises(RuntimeError, match="Start sync error"):
        await anext(stream)

    # 2. Verify on_error was triggered
    assert events == ["start_fail", "error:RuntimeError:0"]

    # 3. Verify the stream is marked completed
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    assert events == ["start_fail", "error:RuntimeError:0"]


def test_sync_lifecycle_stream_empty() -> None:
    events: list[str] = []

    def on_start() -> None:
        events.append("start")

    def on_end(count: int) -> None:
        events.append(f"end:{count}")

    stream = LifecycleStream(iter([]), on_start=on_start, on_end=on_end)
    with pytest.raises(StopIteration):
        next(stream)
    assert events == ["start", "end:0"]


def test_sync_lifecycle_stream_immediate_error() -> None:
    events: list[str] = []

    def on_start() -> None:
        events.append("start")

    def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{type(exc).__name__}:{count}")

    def failing_gen() -> Generator[int, None, None]:
        if False:
            yield 1
        raise ValueError("immediate boom")

    stream = LifecycleStream(failing_gen(), on_start=on_start, on_error=on_error)
    with pytest.raises(ValueError, match="immediate boom"):
        next(stream)
    assert events == ["start", "error:ValueError:0"]


@pytest.mark.asyncio
async def test_async_lifecycle_stream_empty_async() -> None:
    events: list[str] = []

    async def on_start() -> None:
        events.append("start")

    async def on_end(count: int) -> None:
        events.append(f"end:{count}")

    async def empty_async() -> AsyncGenerator[int, None]:
        if False:
            yield 1

    stream = AsyncLifecycleStream(empty_async(), on_start=on_start, on_end=on_end)
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert events == ["start", "end:0"]


@pytest.mark.asyncio
async def test_async_lifecycle_stream_immediate_error_async() -> None:
    events: list[str] = []

    async def on_start() -> None:
        events.append("start")

    async def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{type(exc).__name__}:{count}")

    async def failing_async() -> AsyncGenerator[int, None]:
        if False:
            yield 1
        raise ValueError("immediate async boom")

    stream = AsyncLifecycleStream(failing_async(), on_start=on_start, on_error=on_error)
    with pytest.raises(ValueError, match="immediate async boom"):
        await anext(stream)
    assert events == ["start", "error:ValueError:0"]


@pytest.mark.asyncio
async def test_async_lifecycle_stream_empty_sync() -> None:
    events: list[str] = []

    async def on_start() -> None:
        events.append("start")

    async def on_end(count: int) -> None:
        events.append(f"end:{count}")

    stream = AsyncLifecycleStream(iter([]), on_start=on_start, on_end=on_end)
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert events == ["start", "end:0"]


@pytest.mark.asyncio
async def test_async_lifecycle_stream_immediate_error_sync() -> None:
    events: list[str] = []

    async def on_start() -> None:
        events.append("start")

    async def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{type(exc).__name__}:{count}")

    def failing_gen() -> Generator[int, None, None]:
        if False:
            yield 1
        raise ValueError("immediate sync boom")

    stream = AsyncLifecycleStream(failing_gen(), on_start=on_start, on_error=on_error)
    with pytest.raises(ValueError, match="immediate sync boom"):
        await anext(stream)
    assert events == ["start", "error:ValueError:0"]
