import inspect
from typing import Generator, Iterator, List, NamedTuple
from unittest.mock import MagicMock, call

import pytest

from synaflow import pipeline, step
from synaflow.core.exceptions import PipelineStopException
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


def test_given_on_error_stop_when_item_fails_then_pipeline_stops(run_pipeline):
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def fail_on_2(items: int):
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

    with pytest.raises(PipelineStopException, match="s1"):
        run_pipeline(my_pipeline, params=P())
    assert s1.call_count == 2
    s2.assert_not_called()


def test_given_on_error_continue_when_item_fails_then_continues_next(run_pipeline):
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def skip_on_2(items: int):
        if items == 2:
            raise ValueError("skip")
        return items * 10

    s1 = mock_step(items=int)
    s1.side_effect = skip_on_2
    s2 = mock_step(s1=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[step("s1", fn=s1, on_error=OnError.CONTINUE), step("s2", fn=s2)],
    )

    run_pipeline(my_pipeline, params=P())
    assert s1.call_count == 3
    s2.assert_called_once_with(s1=[10, 30])


def test_given_on_error_stop_when_all_mode_fails_then_pipeline_stops(run_pipeline):
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

    with pytest.raises(PipelineStopException, match="s1"):
        run_pipeline(my_pipeline, params=P())
    s2.assert_not_called()


def test_given_on_error_stop_with_downstream_when_item_fails_then_downstream_never_called(
    run_pipeline,
):
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def fail_on_2(items: int) -> int:
        if items == 2:
            raise ValueError("boom")
        return items

    s1 = mock_step(items=int)
    s1.side_effect = fail_on_2

    # downstream depends on s1
    s2 = mock_step(s1=int)

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", fn=s1, on_error=OnError.STOP),
            step("s2", fn=s2),
        ],
    )

    with pytest.raises(PipelineStopException, match="s1"):
        run_pipeline(my_pipeline, params=P())
    s2.assert_not_called()
