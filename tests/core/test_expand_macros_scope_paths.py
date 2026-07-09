"""Tests for the scope-path plumbing inside ``expand_macros``.

The scope_id emitted per step must identify the *instance* of the
include, not the underlying ``PipelineDef`` name — repeated and nested
includes therefore produce distinct scope_ids. The path is built with
``__`` separators, mirroring step-name conventions.

The data flow is verified by inspecting the tuple returned by
``expand_macros`` directly: ``(scope_id, Step)``. By construction the
``Step`` instances are not mutated — scope_path is transient data
threaded through expansion.
"""

from typing import NamedTuple

from synaflow import include, pipeline, step
from synaflow.core.dag_expansion import expand_macros


class Params(NamedTuple):
    x: int = 0


def fn(x: int) -> int:
    return x


def adapt(x: int) -> Params:
    return Params(x=x)


def test_given_flat_pipeline_when_expand_macros_then_root_steps_get_root_scope_id():
    p = pipeline(
        name="R",
        params=Params,
        steps=[step("a", fn=fn), step("b", fn=fn)],
    )
    result = expand_macros(p.steps, current_pipeline_name="R")
    assert [sid for sid, _ in result] == ["R", "R"]
    assert [s.name for _, s in result] == ["a", "b"]


def test_given_include_when_expand_then_adapter_keeps_callers_scope():
    sub = pipeline(
        name="Sub", params=Params, exports="only", steps=[step("only", fn=fn)]
    )
    incl = include(name="first", pipeline=sub, fn=adapt)
    p = pipeline(
        name="R",
        params=Params,
        steps=[step("before", fn=fn), incl, step("after", fn=fn)],
    )
    result = expand_macros(p.steps, current_pipeline_name="R")
    # The exported step collapses onto the include prefix: see
    # ``_build_expanded_step_name`` — when sub_step.name == exports,
    # the expanded name equals the include prefix.
    names = [s.name for _, s in result]
    assert names == ["before", "first__adapter", "first", "after"]
    scopes = {s.name: sid for sid, s in result}
    assert scopes["before"] == "R"
    assert scopes["first__adapter"] == "R"
    assert scopes["first"] == "R__first"
    assert scopes["after"] == "R"


def test_given_nested_include_when_expand_then_cumulative_scope_path():
    inner = pipeline(name="I", params=Params, exports="only", steps=[step("i1", fn=fn)])
    outer = pipeline(
        name="O",
        params=Params,
        exports="only",
        steps=[include(name="inner", pipeline=inner, fn=adapt)],
    )
    p = pipeline(
        name="R",
        params=Params,
        steps=[include(name="outer", pipeline=outer, fn=adapt)],
    )
    result = expand_macros(p.steps, current_pipeline_name="R")
    scopes = {s.name: sid for sid, s in result}
    assert scopes["outer__adapter"] == "R"
    assert scopes["outer__inner__adapter"] == "R__outer"
    assert scopes["outer__inner__i1"] == "R__outer__inner"


def test_given_repeated_includes_of_same_sub_when_expand_then_distinct_scope_ids():
    sub = pipeline(
        name="Sub", params=Params, exports="only", steps=[step("only", fn=fn)]
    )
    p = pipeline(
        name="R",
        params=Params,
        steps=[
            include(name="first", pipeline=sub, fn=adapt),
            include(name="second", pipeline=sub, fn=adapt),
        ],
    )
    result = expand_macros(p.steps, current_pipeline_name="R")
    scopes = {s.name: sid for sid, s in result}
    assert scopes["first__adapter"] == "R"
    assert scopes["first"] == "R__first"
    assert scopes["second__adapter"] == "R"
    assert scopes["second"] == "R__second"


def test_given_repeated_includes_of_same_sub_when_expand_then_inner_steps_do_not_collide():
    """The two inner steps share ``pipeline == 'Sub'`` but live in
    distinct scopes — the bug fixed by issue #105 v2.

    No two step names collide either; both are distinguishable."""
    sub = pipeline(
        name="Sub", params=Params, exports="only", steps=[step("only", fn=fn)]
    )
    p = pipeline(
        name="R",
        params=Params,
        steps=[
            include(name="first", pipeline=sub, fn=adapt),
            include(name="second", pipeline=sub, fn=adapt),
        ],
    )
    result = expand_macros(p.steps, current_pipeline_name="R")
    scope_per_name = {s.name: sid for sid, s in result}
    assert scope_per_name["first"] != scope_per_name["second"]


def test_given_pipeline_with_no_name_when_expand_macros_then_scope_falls_back_to_empty():
    """PipelineDef without a current_pipeline_name can still be expanded
    for tests that don't care about a name (e.g., schema-only)."""
    p = pipeline(
        name="",
        params=Params,
        steps=[step("a", fn=fn)],
    )
    result = expand_macros(p.steps, current_pipeline_name="")
    assert [sid for sid, _ in result] == [""]
