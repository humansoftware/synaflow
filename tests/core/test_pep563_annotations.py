from __future__ import annotations

from typing import ForwardRef, NamedTuple

from synaflow import pipeline, step
from synaflow.core.dag import get_safe_type_hints
from synaflow.core.dag_builder import build_dag
from synaflow.core.dag_dependencies import initialize_parameters


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
    assert build_dag(p) is not None


def test_given_undefined_type_annotation_when_get_safe_type_hints_called_then_returns_empty_dict():

    def fn_with_undefined(x: "SomeUndefinedType") -> None:  # noqa: F821
        pass

    assert get_safe_type_hints(fn_with_undefined) == {}


def test_given_undefined_type_annotation_in_params_when_initialize_parameters_called_then_falls_back():

    class ParamsWithUndefined(NamedTuple):
        x: "SomeUndefinedType"  # noqa: F821

    nodes = initialize_parameters(ParamsWithUndefined)
    assert "x" in nodes

    assert (
        isinstance(nodes["x"].output, ForwardRef)
        or nodes["x"].output == "SomeUndefinedType"
    )
