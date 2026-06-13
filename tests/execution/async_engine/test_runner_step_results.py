import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any

import pytest

from synaflow.execution.async_engine.pipeline import AsyncPipelineExecutor
from synaflow.execution.async_engine.topology import AsyncStreamManager, AsyncTeeWrapper
from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS

ASYNC_PACK_NAMES = (
    "async_linear",
    "async_diamond",
    "async_complex_parallel",
    "async_fibonacci",
    "async_complex_parallel_mixed",
    "async_sub_pipelines",
    "async_deep_sub_pipelines",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("pack_name", ASYNC_PACK_NAMES)
async def test_step_results(pack_name):
    pack = ASYNC_PACKS[pack_name]

    class TestAsyncStreamManager(AsyncStreamManager):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.recorded_queues = {}
            self.recorded_scalars = {}

        def store_output(
            self, name: str, value: Any, needs_materialize: bool = False
        ) -> None:
            consumers = [
                c for c, cnode in self.dag.items() if name in cnode.get("deps", {})
            ]

            if isinstance(value, (AsyncIterator, AsyncGenerator)):
                queues = {c: asyncio.Queue(maxsize=100) for c in consumers}

                rec_queue = asyncio.Queue(maxsize=100)
                queues["__test_recorder"] = rec_queue
                self.recorded_queues[name] = rec_queue

                self.context[name] = AsyncTeeWrapper(queues)
                node = self.dag.get(name, {})
                on_error = node.get("on_error")
                task = asyncio.create_task(
                    self.pump_iterator(name, value, queues, needs_materialize, on_error)
                )
                self.pump_tasks.append(task)
            else:
                self.recorded_scalars[name] = value
                self.context[name] = value

    class TestAsyncPipelineExecutor(AsyncPipelineExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.stream_manager = TestAsyncStreamManager(
                self.pipeline, self.context, self.pump_tasks
            )
            self.runner.stream_manager = self.stream_manager

    executor = TestAsyncPipelineExecutor(pack.pipeline)

    if pack.exception_match:
        with pytest.raises(Exception, match=pack.exception_match):
            await executor.execute(pack.input_params)
        return

    await executor.execute(pack.input_params)

    from synaflow.execution.async_engine.constants import EOF_MARKER

    final_results = dict(executor.stream_manager.recorded_scalars)
    for key, q in executor.stream_manager.recorded_queues.items():
        items = []
        while True:
            item = await q.get()
            if item is EOF_MARKER:
                break
            items.append(item)
        final_results[key] = items

    for key, expected_val in pack.step_results.items():
        assert final_results.get(key) == expected_val
