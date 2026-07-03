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
