from synaflow import async_run
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
async def test_given_single_step_when_run_then_step_called_with_params():
    class P(NamedTuple):
        x: int = 5

    s1 = mock_step(x=int)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    await async_run(my_pipeline, params=P(x=7))
    s1.assert_called_once_with(x=7)


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_given_params_with_defaults_when_run_then_uses_defaults():
    class P(NamedTuple):
        count: int = 5

    s1 = mock_step(count=int)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    await async_run(my_pipeline, params=P())
    s1.assert_called_once_with(count=5)
