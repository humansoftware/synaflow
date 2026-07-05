import asyncio
import inspect
from time import monotonic_ns
from typing import NamedTuple
from unittest.mock import AsyncMock as MagicMock

import pytest

from synaflow import async_run, pipeline, step
from synaflow.core.exceptions import PipelineStopException
from synaflow.core.types import OnError


def mock_step(return_annotation=inspect.Parameter.empty, **params: type) -> MagicMock:
    mock = MagicMock()
    if params:
        annotations = {name: tp for name, tp in params.items()}
        if return_annotation is not inspect.Parameter.empty:
            annotations["return"] = return_annotation
        mock.__annotations__ = annotations
        mock.__globals__ = {}
        mock.__signature__ = inspect.Signature(
            [
                inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=tp)
                for name, tp in annotations.items()
                if name != "return"
            ],
            return_annotation=return_annotation,
        )
    else:
        mock.__signature__ = inspect.Signature([], return_annotation=return_annotation)
    return mock


async def test_given_on_error_stop_when_item_fails_then_pipeline_stops():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def fail_on_2(items: int):
        if items == 2:
            raise ValueError("boom")

    s1 = mock_step(return_annotation=int, items=int)
    s1.side_effect = fail_on_2
    s2 = mock_step(return_annotation=list, s1=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", fn=s1, on_error=OnError.STOP),
            step("s2", fn=s2),
        ],
    )

    with pytest.raises(PipelineStopException, match="s1"):
        await async_run(my_pipeline, params=P())
    assert s1.call_count == 2
    s2.assert_not_called()


async def test_given_on_error_continue_when_item_fails_then_continues_next():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def skip_on_2(items: int):
        if items == 2:
            raise ValueError("skip")
        return items * 10

    s1 = mock_step(return_annotation=int, items=int)
    s1.side_effect = skip_on_2
    s2 = mock_step(return_annotation=list, s1=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[step("s1", fn=s1, on_error=OnError.CONTINUE), step("s2", fn=s2)],
    )

    await async_run(my_pipeline, params=P())
    assert s1.call_count == 3
    s2.assert_called_once_with(s1=[10, 30])


async def test_given_on_error_stop_when_all_mode_fails_then_pipeline_stops():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    s1 = mock_step(items=list)
    s1.side_effect = ValueError("boom")
    s2 = mock_step(s1=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", fn=s1, on_error=OnError.STOP),
            step("s2", fn=s2),
        ],
    )

    with pytest.raises(PipelineStopException, match="s1"):
        await async_run(my_pipeline, params=P())
    s2.assert_not_called()


async def test_given_on_error_stop_with_downstream_when_item_fails_then_downstream_never_called():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def fail_on_2(items: int) -> int:
        if items == 2:
            raise ValueError("boom")
        return items

    s1 = mock_step(return_annotation=int, items=int)
    s1.side_effect = fail_on_2

    # downstream depends on s1
    s2 = mock_step(return_annotation=int, s1=int)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step(
                "s1",
                fn=s1,
                on_error=OnError.STOP,
                force_materialize=True,
            ),
            step("s2", fn=s2),
        ],
    )

    with pytest.raises(PipelineStopException, match="s1"):
        await async_run(my_pipeline, params=P())
    s2.assert_not_called()


async def test_given_on_error_continue_when_step_fails_then_error_materializer_is_called_for_each_error():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    handled = []

    def error_factory(ctx):
        async def handle(error_ctx):
            handled.append(
                (
                    ctx.dataset_name,
                    type(error_ctx.exception).__name__,
                    str(error_ctx.exception),
                )
            )

        return handle

    async def fail_on_2(items: int):
        if items == 2:
            raise ValueError("skip")
        return items * 10

    s2 = mock_step(return_annotation=list, s1=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        error_materializer=error_factory,
        steps=[
            step("s1", fn=fail_on_2, on_error=OnError.CONTINUE),
            step("s2", fn=s2),
        ],
    )

    await async_run(my_pipeline, params=P())

    assert handled == [("s1", "ValueError", "skip")]
    s2.assert_called_once_with(s1=[10, 30])


async def test_given_error_context_when_step_fails_then_materializer_receives_runtime_fields():
    class P(NamedTuple):
        items: list[int] = [1]

    handled = []

    def error_factory(ctx):
        async def handle(error_ctx):
            handled.append(
                (
                    ctx.dataset_name,
                    error_ctx.step_name,
                    error_ctx.run_id,
                    error_ctx.success_count,
                    error_ctx.error_count,
                    error_ctx.completed_all_inputs,
                    str(error_ctx.exception),
                )
            )

        return handle

    async def fail(items: int):
        raise ValueError("boom")

    my_pipeline = pipeline(
        name="test_async_runtime_error_ctx",
        params=P,
        error_materializer=error_factory,
        steps=[step("s1", fn=fail, on_error=OnError.CONTINUE)],
    )

    await async_run(my_pipeline, params=P())

    assert len(handled) == 1
    dataset_name, step_name, run_id, success_count, error_count, completed, message = (
        handled[0]
    )
    assert dataset_name == "s1"
    assert step_name == "s1"
    assert run_id
    assert success_count == 0
    assert error_count == 1
    assert completed is False
    assert message == "boom"


async def test_given_on_error_stop_when_step_fails_then_error_materializer_is_called_before_pipeline_stops():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    handled = []

    def error_factory(ctx):
        async def handle(error_ctx):
            handled.append(
                (
                    ctx.dataset_name,
                    type(error_ctx.exception).__name__,
                    str(error_ctx.exception),
                )
            )

        return handle

    async def fail_on_2(items: int):
        if items == 2:
            raise ValueError("boom")
        return items

    my_pipeline = pipeline(
        name="test",
        params=P,
        error_materializer=error_factory,
        steps=[step("s1", fn=fail_on_2, on_error=OnError.STOP)],
    )

    with pytest.raises(PipelineStopException, match="s1"):
        await async_run(my_pipeline, params=P())

    assert handled == [("s1", "ValueError", "boom")]


async def test_given_on_error_continue_when_stream_iteration_fails_then_previous_items_are_preserved_and_error_materializer_is_called():
    class P(NamedTuple):
        pass

    handled = []

    def error_factory(ctx):
        async def handle(error_ctx):
            handled.append((ctx.dataset_name, type(error_ctx.exception).__name__))

        return handle

    async def source():
        yield 1
        raise ValueError("iterboom")

    sink = mock_step(return_annotation=list, source=list)

    my_pipeline = pipeline(
        name="test_async_iter_continue",
        params=P,
        error_materializer=error_factory,
        steps=[
            step("source", fn=source, on_error=OnError.CONTINUE),
            step("sink", fn=sink),
        ],
    )

    await async_run(my_pipeline, params=P())

    sink.assert_called_once_with(source=[1])
    assert handled == [("source", "ValueError")]


async def test_given_on_error_stop_when_stream_iteration_fails_then_pipeline_stops_and_error_materializer_is_called():
    class P(NamedTuple):
        pass

    handled = []

    def error_factory(ctx):
        async def handle(error_ctx):
            handled.append((ctx.dataset_name, type(error_ctx.exception).__name__))

        return handle

    async def source():
        yield 1
        raise ValueError("iterboom")

    sink = mock_step(return_annotation=list, source=list)

    my_pipeline = pipeline(
        name="test_async_iter_stop",
        params=P,
        error_materializer=error_factory,
        steps=[
            step("source", fn=source, on_error=OnError.STOP),
            step("sink", fn=sink),
        ],
    )

    with pytest.raises(PipelineStopException, match="source"):
        await async_run(my_pipeline, params=P())

    sink.assert_not_called()
    assert handled == [("source", "ValueError")]


async def test_given_terminal_last_step_with_error_materializer_when_fails_then_executor_waits_for_handler():
    class P(NamedTuple):
        pass

    state = {
        "error_materializer_finished_at": None,
        "returned_to_caller_at": None,
    }

    def error_factory(ctx):
        async def handle(error_ctx):
            assert ctx.dataset_name == "terminal"
            assert str(error_ctx.exception) == "boom"
            await asyncio.sleep(0.01)
            state["error_materializer_finished_at"] = monotonic_ns()

        return handle

    async def source() -> int:
        return 1

    async def middle(source: int) -> int:
        return source + 1

    async def terminal(middle: int) -> int:
        raise ValueError("boom")

    my_pipeline = pipeline(
        name="terminal_async_error_stop",
        params=P,
        error_materializer=error_factory,
        steps=[
            step("source", fn=source),
            step("middle", fn=middle),
            step("terminal", fn=terminal, on_error=OnError.STOP),
        ],
    )

    with pytest.raises(PipelineStopException, match="terminal"):
        await async_run(my_pipeline, params=P())
    state["returned_to_caller_at"] = monotonic_ns()

    assert state["error_materializer_finished_at"] is not None
    assert state["returned_to_caller_at"] >= state["error_materializer_finished_at"]
