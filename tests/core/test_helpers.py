import pytest
from synaflow import to_materializer, to_error_materializer
from synaflow.core.types import MaterializeContext, ErrorMaterializeContext
from typing import Any


def test_to_materializer_non_callable():
    with pytest.raises(TypeError, match="to_materializer expects a callable argument"):
        to_materializer(123)  # type: ignore


def test_to_materializer_preserves_callable():
    def my_fn(val):
        return val

    factory = to_materializer(my_fn)
    ctx = MaterializeContext(pipeline_name="test", dataset_name="step", item_type=Any)
    concrete = factory(ctx)
    assert concrete is my_fn
    assert concrete(42) == 42


def test_to_materializer_with_builtin_list():
    factory = to_materializer(list)
    ctx = MaterializeContext(pipeline_name="test", dataset_name="step", item_type=Any)
    concrete = factory(ctx)
    assert concrete is list
    assert concrete([1, 2]) == [1, 2]


def test_to_error_materializer_non_callable():
    with pytest.raises(
        TypeError, match="to_error_materializer expects a callable argument"
    ):
        to_error_materializer(123)  # type: ignore


def test_to_error_materializer_preserves_callable():
    def my_handler(exc):
        pass

    factory = to_error_materializer(my_handler)
    ctx = ErrorMaterializeContext(
        pipeline_name="test", dataset_name="step", exception_type=ValueError
    )
    concrete = factory(ctx)
    assert concrete is my_handler
