import inspect
from typing import Generator, Iterator, List, NamedTuple
from unittest.mock import MagicMock, call

import pytest

from synaflow.pipeline import pipeline
from synaflow.step import step
from synaflow.types import OnError


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


def test_given_scalar_param_and_list_in_context_when_run_then_iterates(run_pipeline):
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    s1 = mock_step(items=int)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    run_pipeline(my_pipeline, params=P())

    assert s1.call_count == 3
    s1.assert_has_calls([call(items=1), call(items=2), call(items=3)])


def test_given_iterable_param_and_list_in_context_when_run_then_passes_whole_list(run_pipeline):
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    s1 = mock_step(items=list)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    run_pipeline(my_pipeline, params=P())

    s1.assert_called_once_with(items=[1, 2, 3])
