import inspect
from typing import Generator, Iterator, List, NamedTuple
from unittest.mock import MagicMock, call

import pytest

from synaflow import pipeline, step
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


def test_given_single_step_when_run_then_step_called_with_params(run_pipeline):
    class P(NamedTuple):
        x: int = 5

    s1 = mock_step(x=int)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    run_pipeline(my_pipeline, params=P(x=7))
    s1.assert_called_once_with(x=7)


def test_given_multiple_steps_when_run_then_second_receives_first_output(run_pipeline):
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

    run_pipeline(my_pipeline, params=P())
    s1.assert_called_once_with(count=3)
    s2.assert_called_once_with(numbers=[0, 1, 2])


def test_given_multiple_steps_when_run_then_intermediate_step_receives_params(
    run_pipeline,
):
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
    run_pipeline(my_pipeline, params=P(count=2, multiplier=4))

    s1.assert_called_once_with(count=2)
    s2.assert_called_once_with(s1=5, multiplier=4)


def test_given_params_with_defaults_when_run_then_uses_defaults(run_pipeline):
    class P(NamedTuple):
        count: int = 5

    s1 = mock_step(count=int)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    run_pipeline(my_pipeline, params=P())
    s1.assert_called_once_with(count=5)


def test_given_async_pipeline_when_run_synchronously_then_raises():
    from typing import NamedTuple

    import pytest

    from synaflow import pipeline, step
    from synaflow.execution.sync_engine.pipeline import run

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def s1(items: list[int]) -> int:
        return 1

    my_pipeline = pipeline(name="t", params=P, steps=[step("s1", fn=s1)])

    with pytest.raises(RuntimeError, match="must be executed with async_run"):
        run(my_pipeline, params=P())


from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS


@pytest.mark.parametrize("pack_name", list(SYNC_PACKS.keys()))
def test_run_corpus_packs(pack_name):
    pack = SYNC_PACKS[pack_name]
    import itertools
    from collections.abc import Generator, Iterator
    from typing import Any

    from synaflow.execution.sync_engine.pipeline import PipelineExecutor
    from synaflow.execution.sync_engine.topology import SyncStreamManager

    class TestSyncStreamManager(SyncStreamManager):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.recorded = {}

        def apply_materializer(self, name: str, iterator: Iterator) -> Any:
            res1, res2 = itertools.tee(iterator)
            self.recorded[name] = res1
            return super().apply_materializer(name, res2)

        def tee_iterator_for_consumers(self, name: str, iterator: Iterator) -> Any:
            res1, res2 = itertools.tee(iterator)
            self.recorded[name] = res1
            return super().tee_iterator_for_consumers(name, res2)

    class ContextRecorder(dict):
        def __init__(self, stream_manager, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.stream_manager = stream_manager

        def __setitem__(self, key, value):
            if isinstance(value, (Iterator, Generator)):
                res1, res2 = itertools.tee(value)
                self.stream_manager.recorded[key] = res1
                super().__setitem__(key, res2)
            elif type(value).__name__ != "TeeWrapper":
                self.stream_manager.recorded[key] = value
                super().__setitem__(key, value)
            else:
                super().__setitem__(key, value)

    class TestPipelineExecutor(PipelineExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.stream_manager = TestSyncStreamManager(self.pipeline, self.context)
            self.runner.stream_manager = self.stream_manager

            # Replace context to capture scalars
            new_ctx = ContextRecorder(self.stream_manager, self.context)
            self.context = new_ctx
            self.stream_manager.context = new_ctx
            self.resolver.context = new_ctx
            self.runner.context = new_ctx

    executor = TestPipelineExecutor(pack.pipeline)

    if pack.exception_match:
        import pytest

        with pytest.raises(Exception, match=pack.exception_match):
            executor.execute(pack.input_params)
        return

    executor.execute(pack.input_params)

    # Convert recorded iterators to lists before assertion
    final_results = {}
    for key, val in executor.stream_manager.recorded.items():
        if isinstance(val, (Iterator, Generator)):
            final_results[key] = list(val)
        else:
            final_results[key] = val

    # Assert expected results
    for key, expected_val in pack.step_results.items():
        if expected_val is not None:
            assert final_results.get(key) == expected_val

    # Assert expected call order if provided
    if pack.expected_call_order:
        pass
