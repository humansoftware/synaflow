from typing import AsyncGenerator, AsyncIterator
import inspect
from typing import Generator, Iterator, List, NamedTuple
from unittest.mock import MagicMock, call

import pytest

from synaflow.pipeline import pipeline
from synaflow.step import step
from synaflow.types import OnError


async def mock_step(**params: type) -> MagicMock:
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


@pytest.mark.asyncio
async def test_given_on_error_stop_when_item_fails_then_pipeline_stops():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def fail_on_2(items: int):
        if items == 2:
            raise ValueError("boom")

    s1 = mock_step(items=int)
    s1.side_effect = fail_on_2
    s2 = mock_step(items=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", fn=s1, on_error=OnError.STOP),
            step("s2", fn=s2),
        ],
    )

    await async_run(my_pipeline, params=P())
    assert s1.call_count == 2
    s2.assert_not_called()


@pytest.mark.asyncio
async def test_given_on_error_continue_when_item_fails_then_continues_next():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def skip_on_2(items: int):
        if items == 2:
            raise ValueError("skip")

    s1 = mock_step(items=int)
    s1.side_effect = skip_on_2

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[step("s1", fn=s1, on_error=OnError.CONTINUE)],
    )

    await async_run(my_pipeline, params=P())
    assert s1.call_count == 3


@pytest.mark.asyncio
async def test_given_on_error_stop_when_all_mode_fails_then_pipeline_stops():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    s1 = mock_step(items=list)
    s1.side_effect = ValueError("boom")
    s2 = mock_step(items=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", fn=s1, on_error=OnError.STOP),
            step("s2", fn=s2),
        ],
    )

    await async_run(my_pipeline, params=P())
    s2.assert_not_called()
