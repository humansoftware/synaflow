from collections.abc import AsyncGenerator, Generator
from typing import Any
import pytest
from synaflow.execution.async_engine.lifecycle_stream import AsyncLifecycleStream


@pytest.mark.asyncio
async def test_lifecycle_stream() -> None:
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


@pytest.mark.asyncio
async def test_lifecycle_stream_multiple_calls_after_terminal_state() -> None:
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


@pytest.mark.asyncio
async def test_lifecycle_stream_on_start_fails() -> None:
    # 1. Async callback
    events: list[str] = []

    async def on_start_async() -> None:
        events.append("start_fail")
        raise RuntimeError("Start async error")

    async def on_error(exc: BaseException, count: int) -> None:
        events.append(f"error:{type(exc).__name__}:{count}")

    async def async_source() -> AsyncGenerator[int, None]:
        yield 1

    stream = AsyncLifecycleStream(async_source(), on_start_async, on_error=on_error)
    with pytest.raises(RuntimeError, match="Start async error"):
        await anext(stream)
    assert events == ["start_fail", "error:RuntimeError:0"]
    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    # 2. Sync callback
    events.clear()

    def on_start_sync() -> None:
        events.append("start_fail")
        raise RuntimeError("Start sync error")

    stream2 = AsyncLifecycleStream(async_source(), on_start_sync, on_error=on_error)
    with pytest.raises(RuntimeError, match="Start sync error"):
        await anext(stream2)
    assert events == ["start_fail", "error:RuntimeError:0"]
    with pytest.raises(StopAsyncIteration):
        await anext(stream2)


@pytest.mark.asyncio
async def test_lifecycle_stream_empty() -> None:
    # 1. Async source
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

    # 2. Sync source
    events.clear()
    stream2 = AsyncLifecycleStream(iter([]), on_start=on_start, on_end=on_end)
    with pytest.raises(StopAsyncIteration):
        await anext(stream2)
    assert events == ["start", "end:0"]


@pytest.mark.asyncio
async def test_lifecycle_stream_immediate_error() -> None:
    # 1. Async source
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

    # 2. Sync source
    events.clear()

    def failing_gen() -> Generator[int, None, None]:
        if False:
            yield 1
        raise ValueError("immediate sync boom")

    stream2 = AsyncLifecycleStream(failing_gen(), on_start=on_start, on_error=on_error)
    with pytest.raises(ValueError, match="immediate sync boom"):
        await anext(stream2)
    assert events == ["start", "error:ValueError:0"]


def test_step_run_stats() -> None:
    from synaflow.execution.stats import StepRunStats

    stats = StepRunStats()
    assert stats.success_count == 0
    assert stats.error_count == 0
    assert stats.invocation_count == 0
    stats.record_success(2)
    assert stats.success_count == 2
    assert stats.invocation_count == 2
    stats.record_error(1)
    assert stats.error_count == 1
    assert stats.invocation_count == 3


@pytest.mark.asyncio
async def test_step_runner_simple() -> None:
    from contextlib import AsyncExitStack
    from unittest.mock import AsyncMock, MagicMock
    from synaflow.core.dag import DagNode
    from synaflow.execution.async_engine.step_runner import (
        AsyncStepRunner,
        AsyncStepRuntimeConfig,
    )
    from synaflow.execution.stats import StepRunStats
    from synaflow.core.types import OnError, StepMode

    stats = StepRunStats()
    ran = []

    async def fn(x: int) -> int:
        ran.append(x)
        return x * 2

    outputs = []
    mock_events = AsyncMock()

    mock_events.step_started = AsyncMock()
    mock_events.step_completed = AsyncMock()

    runtime_config = AsyncStepRuntimeConfig(
        dag_node=DagNode(
            fn=fn,
            mode=StepMode.ALL,
            on_error=OnError.STOP,
            observers=[],
        )
    )
    runner = AsyncStepRunner(
        step_name="s1",
        fn=fn,
        on_error=OnError.STOP,
        max_in_flight=1,
        dataset_param_names={},
        arguments={"x": 5},
        resource_stack=AsyncExitStack(),
        is_each_mode=False,
        should_drain=False,
        publisher=outputs.append,
        state=MagicMock(),
        events=mock_events,
        stats=stats,
        step_runtime_config=runtime_config,
    )
    await runner.run()

    assert runner.step_name == "s1"
    assert runner.fn == fn
    assert ran == [5]
    assert outputs == [10]
    assert mock_events.step_started.called
    assert mock_events.step_completed.called
