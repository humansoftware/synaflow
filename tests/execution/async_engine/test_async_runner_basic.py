import inspect
from typing import AsyncGenerator, AsyncIterator, Generator, Iterator, List, NamedTuple
from unittest.mock import AsyncMock as MagicMock
from unittest.mock import call

import pytest

from synaflow import async_run, pipeline, step
from synaflow.core.types import OnError


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
    from synaflow.execution.async_engine.executor import async_run

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def s1(items: list[int]) -> Iterator[int]:
        for i in items:
            yield i

    my_pipeline = pipeline(name="t", params=P, steps=[step("s1", fn=s1)])

    with pytest.raises(RuntimeError, match="must be executed with run"):
        await async_run(my_pipeline, params=P())
