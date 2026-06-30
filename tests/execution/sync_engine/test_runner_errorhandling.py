import inspect
from typing import NamedTuple
from unittest.mock import MagicMock

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
        run_pipeline(my_pipeline, params=P())
    s2.assert_not_called()


def test_given_on_error_continue_when_step_fails_then_error_materializer_is_called_for_each_error(
    run_pipeline,
):
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    handled = []

    def error_factory(ctx):
        def handle(error_ctx):
            handled.append(
                (
                    ctx.dataset_name,
                    type(error_ctx.exception).__name__,
                    str(error_ctx.exception),
                )
            )

        return handle

    def fail_on_2(items: int):
        if items == 2:
            raise ValueError("skip")
        return items * 10

    s2 = mock_step(s1=list)

    my_pipeline = pipeline(
        name="test",
        params=P,
        error_materializer=error_factory,
        steps=[
            step("s1", fn=fail_on_2, on_error=OnError.CONTINUE),
            step("s2", fn=s2),
        ],
    )

    run_pipeline(my_pipeline, params=P())

    assert handled == [("s1", "ValueError", "skip")]
    s2.assert_called_once_with(s1=[10, 30])


def test_given_error_context_when_step_fails_then_materializer_receives_runtime_fields(
    run_pipeline,
):
    class P(NamedTuple):
        items: list[int] = [1]

    handled = []

    def error_factory(ctx):
        def handle(error_ctx):
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

    def fail(items: int):
        raise ValueError("boom")

    my_pipeline = pipeline(
        name="test_runtime_error_ctx",
        params=P,
        error_materializer=error_factory,
        steps=[step("s1", fn=fail, on_error=OnError.CONTINUE)],
    )

    run_pipeline(my_pipeline, params=P())

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


def test_given_on_error_stop_when_step_fails_then_error_materializer_is_called_before_pipeline_stops(
    run_pipeline,
):
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    handled = []

    def error_factory(ctx):
        def handle(error_ctx):
            handled.append(
                (
                    ctx.dataset_name,
                    type(error_ctx.exception).__name__,
                    str(error_ctx.exception),
                )
            )

        return handle

    def fail_on_2(items: int):
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
        run_pipeline(my_pipeline, params=P())

    assert handled == [("s1", "ValueError", "boom")]


def test_given_on_error_continue_when_stream_iteration_fails_then_previous_items_are_preserved_and_error_materializer_is_called(
    run_pipeline,
):
    class P(NamedTuple):
        pass

    handled = []

    def error_factory(ctx):
        def handle(error_ctx):
            handled.append((ctx.dataset_name, type(error_ctx.exception).__name__))

        return handle

    def source():
        yield 1
        raise ValueError("iterboom")

    sink = mock_step(source=list)

    my_pipeline = pipeline(
        name="test_iter_continue",
        params=P,
        error_materializer=error_factory,
        steps=[
            step("source", fn=source, on_error=OnError.CONTINUE),
            step("sink", fn=sink),
        ],
    )

    run_pipeline(my_pipeline, params=P())

    sink.assert_called_once_with(source=[1])
    assert handled == [("source", "ValueError")]


def test_given_on_error_stop_when_stream_iteration_fails_then_pipeline_stops_and_error_materializer_is_called(
    run_pipeline,
):
    class P(NamedTuple):
        pass

    handled = []

    def error_factory(ctx):
        def handle(error_ctx):
            handled.append((ctx.dataset_name, type(error_ctx.exception).__name__))

        return handle

    def source():
        yield 1
        raise ValueError("iterboom")

    sink = mock_step(source=list)

    my_pipeline = pipeline(
        name="test_iter_stop",
        params=P,
        error_materializer=error_factory,
        steps=[
            step("source", fn=source, on_error=OnError.STOP),
            step("sink", fn=sink),
        ],
    )

    with pytest.raises(PipelineStopException, match="source"):
        run_pipeline(my_pipeline, params=P())

    sink.assert_not_called()
    assert handled == [("source", "ValueError")]


def test_given_non_callable_error_materializer_when_step_fails_then_raises_type_error(
    run_pipeline,
):
    def producer() -> list[int]:
        raise ValueError("Oops")

    class P(NamedTuple):
        pass

    my_pipeline = pipeline(
        name="test",
        params=P,
        steps=[
            step(
                "producer",
                fn=producer,
                on_error=OnError.CONTINUE,
                error_materializer="not a callable string",
            )
        ],
    )

    with pytest.raises(
        TypeError, match="Error materializer for step 'producer' is not callable"
    ):
        run_pipeline(my_pipeline, params=P())
