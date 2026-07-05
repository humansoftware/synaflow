from collections.abc import Generator
from typing import Any
import pytest
from synaflow.execution.sync_engine.lifecycle_stream import LifecycleStream


def test_lifecycle_stream() -> None:
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


def test_lifecycle_stream_multiple_calls_after_terminal_state() -> None:
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


def test_lifecycle_stream_on_start_fails() -> None:
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


def test_lifecycle_stream_empty() -> None:
    events: list[str] = []

    def on_start() -> None:
        events.append("start")

    def on_end(count: int) -> None:
        events.append(f"end:{count}")

    stream = LifecycleStream(iter([]), on_start=on_start, on_end=on_end)
    with pytest.raises(StopIteration):
        next(stream)
    assert events == ["start", "end:0"]


def test_lifecycle_stream_immediate_error() -> None:
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
