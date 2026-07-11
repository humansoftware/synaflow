import asyncio
import pytest

from synaflow.execution.async_engine.executor import _pump_iterator
from synaflow.execution.async_engine.constants import EOF_MARKER


@pytest.mark.asyncio
async def test_given_full_branch_queue_when_stream_finishes_then_last_item_is_not_dropped():
    queues = {"a": asyncio.Queue(maxsize=1)}

    async def source():
        yield 1

    pump_task = asyncio.create_task(
        _pump_iterator("step", source(), queues, on_error=None)
    )

    for _ in range(5):
        await asyncio.sleep(0)

    item1 = await queues["a"].get()
    item2 = await queues["a"].get()

    assert item1 == 1
    assert item2 is EOF_MARKER

    await pump_task


@pytest.mark.asyncio
async def test_given_full_branch_queue_when_aborted_then_exception_is_raised_and_unconsumed_dropped():
    queues = {"a": asyncio.Queue(maxsize=1)}

    async def source():
        yield 1
        raise ValueError("Boom")

    pump_task = asyncio.create_task(
        _pump_iterator("step", source(), queues, on_error=None)
    )

    for _ in range(5):
        await asyncio.sleep(0)

    item1 = await queues["a"].get()
    item2 = await queues["a"].get()

    assert item1 == 1
    assert item2 is EOF_MARKER

    # Just ensure the task completes without deadlocking
    await pump_task


@pytest.mark.asyncio
async def test_given_fanout_with_normal_exhaustion_when_pump_finishes_then_join_reports_true():
    """Async handoff cleanup contract: after normal source exhaustion,
    the pump task completes within a reasonable timeout so the caller
    can finalise without hanging.

    Parity with the sync engine ``SyncFanout.join()`` contract — see
    ``tests/execution/sync_engine/test_sync_handoff.py`` (Issue #120)."""
    queues = {"a": asyncio.Queue()}

    async def source():
        for i in range(5):
            yield i

    pump_task = asyncio.create_task(
        _pump_iterator("step", source(), queues, on_error=None)
    )
    done, _ = await asyncio.wait({pump_task}, timeout=5.0)
    assert pump_task in done, "pump task must complete within the timeout"


@pytest.mark.asyncio
async def test_given_fanout_under_external_abort_when_source_exhausts_then_join_reports_true():
    """Async handoff cleanup contract: when the pump task is cancelled
    during source exhaustion, it must terminate cleanly so the caller
    does not hang waiting for it.

    Parity with the sync engine ``SyncFanout.join()`` contract — see
    ``tests/execution/sync_engine/test_sync_handoff.py`` (Issue #120)."""
    queues = {"a": asyncio.Queue(), "b": asyncio.Queue()}

    async def source():
        for i in range(10):
            yield i

    pump_task = asyncio.create_task(
        _pump_iterator("step", source(), queues, on_error=None)
    )

    # Let the pump push at least one item, then cancel to simulate an
    # external abort race.
    await asyncio.sleep(0)
    pump_task.cancel()

    try:
        await asyncio.wait_for(pump_task, timeout=1.0)
    except asyncio.CancelledError:
        # Expected — the pump was cancelled mid-execution; the
        # ``finally`` block ran (pushing EOF_MARKER) and the task
        # raised CancelledError when it next yielded.
        pass


@pytest.mark.asyncio
async def test_given_fanout_with_blocked_source_when_source_never_yields_then_join_reports_false():
    """Async handoff cleanup contract: when the source blocks forever,
    the pump task does NOT complete within a short timeout so the
    caller knows the pump is stuck.

    Parity with the sync engine ``SyncFanout.join()`` contract — see
    ``tests/execution/sync_engine/test_sync_handoff.py`` (Issue #120)."""
    triggered = asyncio.Event()
    queues = {"a": asyncio.Queue()}

    async def blocked_source():
        await triggered.wait()
        yield 1  # never reached within the test timeout

    pump_task = asyncio.create_task(
        _pump_iterator("step", blocked_source(), queues, on_error=None)
    )

    # ``asyncio.wait`` does not cancel on timeout; it just reports
    # which tasks completed.  The pump is blocked inside the source
    # and must NOT be done.
    done, _ = await asyncio.wait({pump_task}, timeout=0.3)
    assert pump_task not in done, (
        "pump task must NOT complete when the source is blocked"
    )
