from __future__ import annotations

import sys
import types
from typing import NamedTuple

import pytest

from synaflow import PipelineRegistry, pipeline, step


def _make_pipeline(name: str = "p") -> object:
    """Build a tiny sync pipeline for use in registry tests."""

    class P(NamedTuple):
        x: int = 0

    def fn(x: int) -> int:
        return x

    return pipeline(name=name, params=P, steps=[step("s", fn=fn)])


def test_given_register_then_get_returns_pipeline_def():
    p = _make_pipeline("a")
    reg = PipelineRegistry()
    reg["a"] = p
    assert reg["a"] is p


def test_given_register_then_get_dag_returns_compiled_dag():
    p = _make_pipeline("a")
    reg = PipelineRegistry()
    reg["a"] = p
    dag = reg.get_dag("a")
    assert dag.name == "a"
    assert "s" in dag.steps


def test_given_get_dag_called_twice_then_dag_is_cached():
    p = _make_pipeline("a")
    reg = PipelineRegistry()
    reg["a"] = p
    dag1 = reg.get_dag("a")
    dag2 = reg.get_dag("a")
    assert dag1 is dag2


def test_given_re_register_then_cached_dag_is_invalidated():
    p1 = _make_pipeline("a")
    p2 = _make_pipeline("a")
    reg = PipelineRegistry()
    reg["a"] = p1
    dag1 = reg.get_dag("a")
    reg["a"] = p2
    dag2 = reg.get_dag("a")
    assert dag1 is not dag2


def test_given_unknown_name_then_raises_key_error():
    reg = PipelineRegistry()
    with pytest.raises(KeyError):
        reg["missing"]
    with pytest.raises(KeyError):
        reg.get_dag("missing")


def test_given_non_pipeline_def_then_setitem_raises_type_error():
    reg = PipelineRegistry()
    with pytest.raises(TypeError, match="PipelineDef"):
        reg["a"] = "not a pipeline"  # type: ignore[assignment]


def test_given_key_mismatch_then_setitem_raises_value_error():
    p = _make_pipeline("a")
    reg = PipelineRegistry()
    with pytest.raises(ValueError, match="must match"):
        reg["b"] = p


def test_given_invalidate_then_next_get_dag_rebuilds():
    p = _make_pipeline("a")
    reg = PipelineRegistry()
    reg["a"] = p
    dag1 = reg.get_dag("a")
    reg.invalidate("a")
    dag2 = reg.get_dag("a")
    assert dag1 is not dag2


def test_given_invalidate_unknown_name_then_raises_key_error():
    reg = PipelineRegistry()
    with pytest.raises(KeyError):
        reg.invalidate("missing")


def test_given_clear_then_registry_is_empty_and_dags_dropped():
    p = _make_pipeline("a")
    reg = PipelineRegistry()
    reg["a"] = p
    reg.get_dag("a")
    reg.clear()
    assert len(reg) == 0
    with pytest.raises(KeyError):
        reg.get_dag("a")
    assert list(reg) == []


def test_given_from_module_with_valid_catalog_then_loads_registry():
    mod = types.ModuleType("test_catalog_mod_valid")
    sys.modules["test_catalog_mod_valid"] = mod
    try:
        p = _make_pipeline("a")
        reg_inner = PipelineRegistry()
        reg_inner["a"] = p
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
