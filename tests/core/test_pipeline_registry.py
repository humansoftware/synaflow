from __future__ import annotations

import sys
import types
from typing import NamedTuple
from unittest import mock

import pytest

from synaflow import PipelineRegistry, include, pipeline, step
from synaflow.core.dag_builder import build_dag


def _make_pipeline(name: str = "p"):
    """Build a tiny sync pipeline for use in registry tests."""

    class P(NamedTuple):
        x: int = 0

    def fn(x: int) -> int:
        return x

    return pipeline(name=name, params=P, steps=[step("s", fn=fn)])


def _make_root_with_child():
    class ChildParams(NamedTuple):
        x: int

    def child_step(x: int) -> int:
        return x

    child_step.__annotations__ = {"x": int, "return": int}

    child = pipeline(
        name="child",
        params=ChildParams,
        steps=[step("child_step", fn=child_step)],
        exports="child_step",
    )

    class RootParams(NamedTuple):
        x: int

    def adapt(x: int) -> ChildParams:
        return ChildParams(x)

    adapt.__annotations__ = {"x": int, "return": ChildParams}

    root = pipeline(
        name="root",
        params=RootParams,
        steps=[include("child", pipeline=child, fn=adapt)],
    )
    return root, child


def test_given_add_then_get_returns_pipeline_def_and_precompiled_dag():
    p = _make_pipeline("a")
    reg = PipelineRegistry()

    reg.add(p)

    assert reg["a"] is p
    assert reg.get_dag("a").name == "a"


def test_given_add_then_get_dag_does_not_recompile():
    p = _make_pipeline("a")
    reg = PipelineRegistry()

    with mock.patch(
        "synaflow.core.pipeline_registry.build_dag", wraps=build_dag
    ) as compile_dag:
        reg.add(p)
        dag = reg.get_dag("a")

    assert dag.name == "a"
    assert compile_dag.call_count == 1


def test_given_add_same_instance_twice_then_it_is_a_noop():
    p = _make_pipeline("a")
    reg = PipelineRegistry()

    with mock.patch(
        "synaflow.core.pipeline_registry.build_dag", wraps=build_dag
    ) as compile_dag:
        reg.add(p)
        reg.add(p)

    assert compile_dag.call_count == 1


def test_given_add_different_instance_with_same_name_then_raises_value_error():
    reg = PipelineRegistry()
    reg.add(_make_pipeline("a"))

    with pytest.raises(ValueError, match="already registered"):
        reg.add(_make_pipeline("a"))


def test_given_add_invalid_pipeline_then_registry_is_unchanged():
    class P(NamedTuple):
        x: int = 0

    bad = pipeline(name="bad", params=P, steps=[step("bad", fn="not callable")])
    reg = PipelineRegistry()

    with pytest.raises(ValueError, match="must have a callable"):
        reg.add(bad)

    assert len(reg) == 0
    with pytest.raises(KeyError):
        reg.get_dag("bad")


def test_given_add_root_then_registers_its_nested_pipelines():
    root, child = _make_root_with_child()
    reg = PipelineRegistry()

    reg.add(root)

    assert set(reg) == {"root", "child"}
    assert reg["root"] is root
    assert reg["child"] is child
    assert reg.get_dag("root").name == "root"
    assert reg.get_dag("child").name == "child"


def test_given_nested_pipeline_name_collides_with_other_instance_then_add_is_atomic():
    root, _ = _make_root_with_child()
    reg = PipelineRegistry()
    reg.add(_make_pipeline("child"))

    with pytest.raises(ValueError, match="already registered"):
        reg.add(root)

    assert set(reg) == {"child"}


def test_given_unknown_name_then_raises_key_error():
    reg = PipelineRegistry()
    with pytest.raises(KeyError):
        reg["missing"]
    with pytest.raises(KeyError):
        reg.get_dag("missing")


def test_given_non_pipeline_def_then_add_raises_type_error():
    reg = PipelineRegistry()
    with pytest.raises(TypeError, match="PipelineDef"):
        reg.add("not a pipeline")  # type: ignore[arg-type]


def test_given_item_assignment_then_raises_type_error():
    reg = PipelineRegistry()

    with pytest.raises(TypeError):
        reg["a"] = _make_pipeline("a")  # type: ignore[index]


def test_given_include_cycle_then_add_raises_value_error_without_registration():
    class P(NamedTuple):
        x: int

    def adapt(x: int) -> P:
        return P(x)

    adapt.__annotations__ = {"x": int, "return": P}
    root = pipeline(name="root", params=P, steps=[], exports="root")
    root.steps.append(include("self", pipeline=root, fn=adapt))
    reg = PipelineRegistry()

    with pytest.raises(ValueError, match="include cycle"):
        reg.add(root)

    assert len(reg) == 0


def test_given_from_module_with_valid_catalog_then_loads_registry():
    mod = types.ModuleType("test_catalog_mod_valid")
    sys.modules["test_catalog_mod_valid"] = mod
    try:
        p = _make_pipeline("a")
        reg_inner = PipelineRegistry()
        reg_inner.add(p)
        mod.catalog = reg_inner
        loaded = PipelineRegistry.from_module("test_catalog_mod_valid")
        assert loaded is reg_inner
        assert loaded["a"] is p
    finally:
        del sys.modules["test_catalog_mod_valid"]


def test_given_from_module_with_wrong_attribute_type_then_raises_type_error():
    mod = types.ModuleType("test_catalog_mod_wrong")
    sys.modules["test_catalog_mod_wrong"] = mod
    try:
        mod.catalog = "not a registry"  # type: ignore[attr-defined]
        with pytest.raises(TypeError, match="PipelineRegistry"):
            PipelineRegistry.from_module("test_catalog_mod_wrong")
    finally:
        del sys.modules["test_catalog_mod_wrong"]
