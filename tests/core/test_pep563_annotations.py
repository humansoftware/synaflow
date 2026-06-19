from __future__ import annotations

from collections.abc import Iterator
from typing import NamedTuple
from synaflow import pipeline, step


def test_given_future_annotations_when_pipeline_built_then_types_resolve_correctly():
    class Params(NamedTuple):
        name: str = ""

    def my_step(name: str) -> str:
        return name

    p = pipeline(
        name="test_future_annotations",
        params=Params,
        steps=[
            step("my_step", fn=my_step),
        ],
    )
    assert p.dag is not None


def test_given_undefined_type_annotation_when_get_safe_type_hints_called_then_returns_empty_dict():
    def fn_with_undefined(x: "SomeUndefinedType") -> None:
        pass

    from synaflow.core.dag_dependencies import get_safe_type_hints

    assert get_safe_type_hints(fn_with_undefined) == {}


def test_given_undefined_type_annotation_in_params_when_initialize_parameters_called_then_falls_back():
    class ParamsWithUndefined(NamedTuple):
        x: "SomeUndefinedType"

    import typing

    try:
        print("GET HINTS:", typing.get_type_hints(ParamsWithUndefined))
    except Exception as e:
        print("GET HINTS EXCEPTION:", type(e), e)

    from synaflow.core.dag_dependencies import initialize_parameters

    nodes = initialize_parameters(ParamsWithUndefined)
    assert "x" in nodes
    from typing import ForwardRef

    print("OUTPUT TYPE:", type(nodes["x"].output), nodes["x"].output)
    assert (
        isinstance(nodes["x"].output, ForwardRef)
        or nodes["x"].output == "SomeUndefinedType"
    )


def test_given_future_annotations_when_run_then_executes_successfully(run_pipeline):
    class Params(NamedTuple):
        name: str = ""

    captured = None

    def my_step(name: str) -> str:
        nonlocal captured
        captured = name
        return name

    p = pipeline(
        name="test_future_annotations_run",
        params=Params,
        steps=[
            step("my_step", fn=my_step),
        ],
    )
    run_pipeline(p, Params(name="hello"))
    assert captured == "hello"


def test_given_future_annotations_when_custom_materializer_executed_then_receives_type_object(
    run_pipeline,
):
    resolved_item_type = None

    def my_factory(ctx):
        nonlocal resolved_item_type
        resolved_item_type = ctx.item_type
        return lambda it: list(it)

    class Params(NamedTuple):
        pass

    def my_step() -> Iterator[str]:
        yield "a"
        yield "b"

    captured = None

    def sink(my_step: list[str]) -> list[str]:
        nonlocal captured
        captured = my_step
        return my_step

    p = pipeline(
        name="test_future_annotations_mat",
        params=Params,
        steps=[
            step("my_step", fn=my_step, materializer=my_factory),
            step("sink", fn=sink),
        ],
    )
    run_pipeline(p, Params())
    assert captured == ["a", "b"]
    assert resolved_item_type == Iterator[str]
