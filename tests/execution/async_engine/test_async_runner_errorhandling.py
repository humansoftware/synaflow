import inspect
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
            step("s1", fn=s1, on_error=OnError.STOP),
            step("s2", fn=s2),
        ],
    )

    with pytest.raises(PipelineStopException, match="s1"):
        await async_run(my_pipeline, params=P())
    s2.assert_not_called()
