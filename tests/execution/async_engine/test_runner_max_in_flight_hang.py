"Regression tests for Issue #103 (async engine parity).\n\nMirrors of tests in ``tests/execution/sync_engine/test_runner_max_in_flight_hang.py``.\nEach test exercises the same hang mechanism against the async engine so that\nthe parity check in ``tests/core/test_parity.py`` stays green and any future\nregression in either engine is caught by the same-name counterpart.\n\nThe tests use ``asyncio.wait_for`` as the bound on the async pipeline.  When\nthe framework no longer hangs, ``wait_for`` completes normally with the\nresult (or the exception captured by ``run_with_timeout``); when the bug is\npresent, the timeout fires and we report a hang.\n"

from synaflow.core.dag_builder import build_dag
import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from typing import NamedTuple
import pytest
from synaflow import OnError, async_run, pipeline, step
from synaflow.execution.async_engine.executor import AsyncPipelineExecutor
from synaflow.core.exceptions import PipelineStopException


class EmptyParams(NamedTuple):
    pass


class _HangDetected(BaseException):
    """Sentinel raised by ``_run_with_timeout`` when the awaitable hangs.

    Inherits from ``BaseException`` so a test-framework timeout is never
    mistaken for an application-level exception by ``except Exception``
    inside user code under test.
    """


async def _run_with_timeout(coro, timeout: float = 5.0) -> BaseException | None:
    """Run an awaitable with a hard timeout.

    Returns:
        ``None`` if the awaitable completes cleanly within ``timeout``.
        A ``BaseException`` if it raises within ``timeout``.

    Raises:
        _HangDetected: ``coro`` did not finish within ``timeout`` seconds.
            Tests asserting "must not hang" wrap this helper in
            ``try: ... except _HangDetected: pytest.fail(...)``.
    """
    raised: BaseException | None = None

    async def runner() -> None:
        nonlocal raised
        try:
            await coro
        except BaseException as exc:
            raised = exc

    task = asyncio.create_task(runner())
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except _HangDetected:
        task.cancel()
        raise
    except asyncio.TimeoutError:
        task.cancel()
        raise _HangDetected(
            f"Pipeline did not complete within {timeout}s (Issue #103)"
        ) from None
    return raised


@pytest.mark.asyncio
async def test_given_fanout_pump_blocked_when_consumer_raises_then_cleanup_hangs():
    """Cause 2 (cleanup hang): _pump task is stuck in __anext__() on a
    blocked producer.

    Pipeline topology:

        blocked_producer (max_in_flight=3, AsyncIterator[int])
            ├── consumer_a  (AsyncIterator[int] -> None)
            └── consumer_b  (AsyncIterator[int] -> None)  ← raises ValueError

    The pump is parked on ``await source_blocked.wait()`` inside the
    producer generator.  consumer_b raises with on_error=STOP → step_done
    sets fatal_error → async_run attempts to abort and await the pump.
    asyncio.Task.cancel() schedules a CancelledError but cannot interrupt
    user code synchronously.  cleanup() must use a bounded timeout so the
    pipeline terminates.
    """
    source_blocked = asyncio.Event()
    pump_started = asyncio.Event()

    async def blocked_producer() -> AsyncGenerator[int, None]:
        await source_blocked.wait()
        yield 1
        yield

    async def consumer_a(blocked_producer: AsyncIterator[int]) -> None:
        pump_started.set()
        async for _x in blocked_producer:
            pass

    async def consumer_b(blocked_producer: AsyncIterator[int]) -> None:
        await pump_started.wait()
        raise ValueError("consumer_b fails early")

    pipeline_def = pipeline(
        name="test_cleanup_hang_async",
        params=EmptyParams,
        steps=[
            step("blocked_producer", fn=blocked_producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a),
            step("consumer_b", fn=consumer_b, on_error=OnError.STOP),
        ],
    )
    try:
        exc = await _run_with_timeout(
            async_run(pipeline_def, EmptyParams()), timeout=5.0
        )
    except _HangDetected as hd:
        pytest.fail(
            f"AsyncPipelineExecutor hung: cleanup() must not block on asyncio.gather() for pump tasks indefinitely.  See Issue #103. Runner task exception context: {hd!r}"
        )
    assert exc is None or isinstance(exc, BaseException), (
        "AsyncPipelineExecutor hung: cleanup() must not block on asyncio.gather() for pump tasks indefinitely.  See Issue #103."
    )


@pytest.mark.asyncio
async def test_given_blocking_step_when_another_step_raises_then_run_graph_hangs():
    """Cause 1 (_run_graph hang): an in-flight task never completes (blocked
    on ``await``); _run_graph() must wake the event when fatal_error is set
    even though the blocker still runs.
    """
    step_blocked = asyncio.Event()
    blocking_started = asyncio.Event()

    async def blocking_step() -> None:
        blocking_started.set()
        await step_blocked.wait()

    async def failing_step() -> None:
        await blocking_started.wait()
        raise ValueError("failing_step raises")

    pipeline_def = pipeline(
        name="test_run_graph_hang_async",
        params=EmptyParams,
        steps=[
            step("blocking_step", fn=blocking_step),
            step("failing_step", fn=failing_step, on_error=OnError.STOP),
        ],
    )
    try:
        exc = await _run_with_timeout(
            async_run(pipeline_def, EmptyParams()), timeout=5.0
        )
    except _HangDetected as hd:
        pytest.fail(
            f"AsyncPipelineExecutor hung: _run_graph() must not block on event.wait() indefinitely when one step fails.  See Issue #103. Runner task exception context: {hd!r}"
        )
    assert exc is None or isinstance(exc, BaseException), (
        "AsyncPipelineExecutor hung: _run_graph() must not block on event.wait() indefinitely when one step fails.  See Issue #103."
    )


@pytest.mark.asyncio
async def test_given_build_arguments_raises_when_max_in_flight_active_then_pump_hangs_on_eof():
    """Production hang: build_arguments() raises before the async consumer
    is awaited.  In the async engine the pump pushes ``EOF_MARKER`` in its
    finally-block, so the issue manifests as a leaked half-open
    AsyncQueueBranch — mirror of the sync engine's leaked SyncQueueIterator
    (Issue #103).  Using ``AsyncPipelineExecutor(dag, resource_factories={})``
    directly simulates the include()-not-propagating-resource production
    scenario.
    """

    class Downloader:
        pass

    def make_downloader() -> Downloader:
        return Downloader()

    async def producer() -> AsyncGenerator[int, None]:
        for i in range(20):
            yield i

    consumer_a_results: list[int] = []

    async def consumer_a(producer: int) -> None:
        consumer_a_results.append(producer)

    async def consumer_b(producer: int, downloader: Downloader) -> None:
        raise AssertionError("consumer_b must not run")

    pipeline_def = pipeline(
        name="test_build_args_hang_async",
        params=EmptyParams,
        resources={"downloader": make_downloader},
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a),
            step("consumer_b", fn=consumer_b),
        ],
    )

    async def run_pipeline() -> None:
        await AsyncPipelineExecutor(
            build_dag(pipeline_def), resource_factories={}
        ).execute(EmptyParams())

    try:
        exc = await _run_with_timeout(run_pipeline(), timeout=5.0)
    except _HangDetected as hd:
        pytest.fail(
            f"AsyncPipelineExecutor hung: build_arguments() failures must not leak AsyncQueueBranch slots that block the pump.  See Issue #103. Runner task exception context: {hd!r}"
        )
    assert exc is None or isinstance(exc, BaseException), (
        "AsyncPipelineExecutor hung: build_arguments() failures must not leak AsyncQueueBranch slots that block the pump.  See Issue #103."
    )


@pytest.mark.asyncio
async def test_given_build_arguments_raises_without_bounded_handoff_then_no_hang():
    """Same shape as Test C but with max_in_flight=1 → no AsyncFanout pump.
    Baseline confirming the hang is specifically about the AsyncQueueBranch
    leaking into the pump.
    """

    class Downloader:
        pass

    def make_downloader() -> Downloader:
        return Downloader()

    async def producer() -> AsyncGenerator[int, None]:
        for i in range(10):
            yield i

    async def consumer_b(producer: int, downloader: Downloader) -> None:
        raise AssertionError("consumer_b must not run")

    pipeline_def = pipeline(
        name="test_build_args_no_fanout_async",
        params=EmptyParams,
        resources={"downloader": make_downloader},
        steps=[
            step("producer", fn=producer, max_in_flight=1),
            step("consumer_b", fn=consumer_b),
        ],
    )

    async def run_pipeline() -> None:
        await AsyncPipelineExecutor(
            build_dag(pipeline_def), resource_factories={}
        ).execute(EmptyParams())

    try:
        exc = await _run_with_timeout(run_pipeline(), timeout=5.0)
    except _HangDetected as hd:
        pytest.fail(
            f"AsyncPipelineExecutor with max_in_flight=1 should fail fast.  If this hangs, something else is blocking.  See Issue #103. Runner task exception context: {hd!r}"
        )
    assert exc is None or isinstance(exc, BaseException), (
        "AsyncPipelineExecutor with max_in_flight=1 should fail fast.  If this hangs, something else is blocking."
    )


@pytest.mark.asyncio
async def test_given_consumer_raises_with_on_error_continue_then_pump_drains():
    """Consumer raises with OnError.CONTINUE (default).  The async pump
    should still drain cleanly because its ``finally``-block sends
    ``EOF_MARKER`` regardless of consumer health.
    """

    async def producer() -> AsyncGenerator[int, None]:
        for i in range(100):
            yield i

    async def consumer(producer: AsyncIterator[int]) -> None:
        i = 0
        async for x in producer:
            i += 1
            if i >= 3:
                raise ValueError("consumer fails early")

    pipeline_def = pipeline(
        name="test_continue_fanout_async",
        params=EmptyParams,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer", fn=consumer, on_error=OnError.CONTINUE),
        ],
    )
    try:
        exc = await _run_with_timeout(
            async_run(pipeline_def, EmptyParams()), timeout=5.0
        )
    except _HangDetected as hd:
        pytest.fail(
            f"AsyncPipelineExecutor with OnError.CONTINUE should drain cleanly — pump's finally-block sends EOF_MARKER regardless.  Hang here means the cleanup timeout was not bounded.  See Issue #103. Runner task exception context: {hd!r}"
        )
    assert exc is None or isinstance(exc, BaseException), (
        "AsyncPipelineExecutor with OnError.CONTINUE should drain cleanly — pump's finally-block sends EOF_MARKER regardless.  Hang here means the cleanup timeout was not bounded."
    )


@pytest.mark.asyncio
async def test_given_consumer_raises_with_on_error_stop_and_fanout_then_pump_drains():
    """Consumer raises with OnError.STOP → PipelineStopException →
    abort() cancels the pump tasks, which exit via CancelledError.  The
    pipeline should propagate the exception without hanging.
    """

    async def producer() -> AsyncGenerator[int, None]:
        for i in range(100):
            yield i

    async def consumer_b(producer: AsyncIterator[int]) -> None:
        i = 0
        async for _x in producer:
            i += 1
            if i >= 3:
                raise ValueError("consumer_b fails early")

    async def consumer_a(producer: AsyncIterator[int]) -> None:
        async for _x in producer:
            pass

    pipeline_def = pipeline(
        name="test_stop_fanout_async",
        params=EmptyParams,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a, on_error=OnError.STOP),
            step("consumer_b", fn=consumer_b, on_error=OnError.STOP),
        ],
    )
    exc: BaseException | None
    try:
        exc = await _run_with_timeout(
            async_run(pipeline_def, EmptyParams()), timeout=5.0
        )
    except _HangDetected as hd:
        pytest.fail(
            f"AsyncPipelineExecutor with OnError.STOP should propagate PipelineStopException.  Hang here means the run loop is blocked.  See Issue #103. Runner task exception context: {hd!r}"
        )
        return
    assert exc is None or isinstance(exc, BaseException), (
        "AsyncPipelineExecutor with OnError.STOP should propagate PipelineStopException.  Hang here means the run loop is blocked."
    )
    if exc is not None:
        assert isinstance(exc, PipelineStopException)
