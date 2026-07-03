import pytest
import inspect
from typing import NamedTuple
from dataclasses import dataclass
from unittest.mock import MagicMock

from synaflow.core.dag import Dag, DagNode
from synaflow.core.types import OnError, StepMode

from synaflow import pipeline, step


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


def test_given_namedtuple_param_field_when_run_then_injected_as_object(run_pipeline):
    class MyNamedTuple(NamedTuple):
        a: int
        b: int

    class P(NamedTuple):
        obj: MyNamedTuple

    s1 = mock_step(obj=MyNamedTuple)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    run_pipeline(my_pipeline, params=P(obj=MyNamedTuple(a=1, b=2)))
    s1.assert_called_once_with(obj=MyNamedTuple(a=1, b=2))


def test_given_dataclass_param_field_when_run_then_injected_as_object(run_pipeline):
    @dataclass
    class MyDataclass:
        a: int
        b: int

    @dataclass
    class P:
        obj: MyDataclass

    s1 = mock_step(obj=MyDataclass)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    run_pipeline(my_pipeline, params=P(obj=MyDataclass(a=1, b=2)))
    s1.assert_called_once_with(obj=MyDataclass(a=1, b=2))


def test_given_frozen_dataclass_param_field_when_run_then_injected_as_object(
    run_pipeline,
):
    @dataclass(frozen=True)
    class MyFrozenDataclass:
        a: int
        b: int

    @dataclass(frozen=True)
    class P:
        obj: MyFrozenDataclass

    s1 = mock_step(obj=MyFrozenDataclass)

    my_pipeline = pipeline(name="test", params=P, steps=[step("s1", fn=s1)])
    run_pipeline(my_pipeline, params=P(obj=MyFrozenDataclass(a=1, b=2)))
    s1.assert_called_once_with(obj=MyFrozenDataclass(a=1, b=2))


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

    from synaflow import pipeline, step
    from synaflow.execution.sync_engine.executor import run

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def s1(items: list[int]) -> int:
        return 1

    my_pipeline = pipeline(name="t", params=P, steps=[step("s1", fn=s1)])

    with pytest.raises(RuntimeError, match="must be executed with async_run"):
        run(my_pipeline, params=P())


def test_given_runtime_dag_with_all_mode_when_types_look_like_each_then_executor_obeys_dag_mode():

    from synaflow.execution.sync_engine.executor import PipelineExecutor

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    calls = []

    def consumer(items: int):
        calls.append(items)

    dag = Dag(
        name="manual",
        params={"items": list[int]},
        steps={
            "consumer": DagNode(
                fn=consumer,
                deps={"items": int},
                output=None,
                on_error=OnError.CONTINUE,
                mode=StepMode.EACH,
                each_mode_deps=["items"],
            ),
        },
    )

    PipelineExecutor(dag).execute(P())

    assert calls == [1, 2, 3]
