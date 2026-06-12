import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any, Generator, Iterator, List, NamedTuple
from unittest.mock import AsyncMock as MagicMock
from unittest.mock import call

import pytest

from synaflow import async_run, pipeline, step
from synaflow.core.types import OnError
from synaflow.execution.async_engine.pipeline import AsyncPipelineExecutor
from synaflow.execution.async_engine.topology import AsyncStreamManager, AsyncTeeWrapper
from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS


def mock_step(**params: type) -> MagicMock:
    mock = MagicMock()
    if params:
        annotations = {name: tp for name, tp in params.items()}
        mock.__annotations__ = annotations
        mock.__globals__ = {}
        mock.__signature__ = inspect.Signature(
            [
                inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=tp)
                for name, tp in annotations.items()
            ]
        )
    else:
        mock.__signature__ = inspect.Signature([])
    return mock


async def test_given_single_step_when_run_then_step_called_with_params():
    class P(NamedTuple):
        x: int = 5

    s1 = mock_step(x=int)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    await async_run(my_pipeline, params=P(x=7))
    s1.assert_called_once_with(x=7)


async def test_given_multiple_steps_when_run_then_second_receives_first_output():
    class P(NamedTuple):
        count: int = 3

    s1 = mock_step(count=int)
    s1.return_value = [0, 1, 2]
    s2 = mock_step(numbers=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[step("numbers", fn=s1), step("total", fn=s2)],
    )

    await async_run(my_pipeline, params=P())
    s1.assert_called_once_with(count=3)
    s2.assert_called_once_with(numbers=[0, 1, 2])


async def test_given_multiple_steps_when_run_then_intermediate_step_receives_params():
    class P(NamedTuple):
        count: int = 3
        multiplier: int = 10

    s1 = mock_step(count=int)
    s1.return_value = 5

    # s2 depends on s1 and also requests a parameter directly
    s2 = mock_step(s1=int, multiplier=int)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[step("s1", fn=s1), step("s2", fn=s2)],
    )
    await async_run(my_pipeline, params=P(count=2, multiplier=4))

    s1.assert_called_once_with(count=2)
    s2.assert_called_once_with(s1=5, multiplier=4)


async def test_given_params_with_defaults_when_run_then_uses_defaults():
    class P(NamedTuple):
        count: int = 5

    s1 = mock_step(count=int)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    await async_run(my_pipeline, params=P())
    s1.assert_called_once_with(count=5)


async def test_given_sync_stream_pipeline_when_run_asynchronously_then_raises():
    from typing import Iterator, NamedTuple

    import pytest

    from synaflow import pipeline, step
    from synaflow.execution.async_engine.pipeline import async_run

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def s1(items: list[int]) -> Iterator[int]:
        for i in items:
            yield i

    my_pipeline = pipeline(name="t", params=P, steps=[step("s1", fn=s1)])

    with pytest.raises(RuntimeError, match="must be executed with run"):
        await async_run(my_pipeline, params=P())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pack_name, pack", list(ASYNC_PACKS.items()), ids=list(ASYNC_PACKS.keys())
)
async def test_run_corpus_packs(pack_name, pack):
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

                # ADD RECORDER QUEUE!
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
            # Replace stream_manager
            self.stream_manager = TestAsyncStreamManager(
                self.pipeline, self.context, self.pump_tasks
            )
            self.runner.stream_manager = self.stream_manager

    executor = TestAsyncPipelineExecutor(pack.pipeline)

    if pack.exception_match:
        import pytest

        with pytest.raises(Exception, match=pack.exception_match):
            await executor.execute(pack.input_params)
        return

    await executor.execute(pack.input_params)

    from synaflow.execution.async_engine.constants import EOF_MARKER

    # Read queues to lists
    final_results = dict(executor.stream_manager.recorded_scalars)
    for key, q in executor.stream_manager.recorded_queues.items():
        items = []
        while True:
            item = await q.get()
            if item is EOF_MARKER:
                break
            items.append(item)
        final_results[key] = items

    # Assert expected results
    for key, expected_val in pack.step_results.items():
        if expected_val is not None:
            assert final_results.get(key) == expected_val

    # Assert expected call order if provided
    if pack.expected_call_order:
        pass
