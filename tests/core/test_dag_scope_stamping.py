"""Tests for scope stamping inside ``build_dag`` (issue #105 v2).

``build_dag`` populates ``DagNode.pipeline_scope``, ``step_index_in_scope``,
``step_total_in_scope`` and the dag-level ``scope_step_totals`` dict.
Index is **0-based** and **topological** within scope; the topology
is taken from ``Dag.get_execution_levels`` flattened in returned order
(no second topo algorithm).
"""

from typing import NamedTuple

from synaflow import include, pipeline, step
from synaflow.core.dag_builder import build_dag


class Params(NamedTuple):
    x: int = 0


def fn(x: int) -> int:
    return x


def adapt(x: int) -> Params:
    return Params(x=x)


def test_root_scope_stamps_each_direct_step_with_root_pipeline_name():
    p = pipeline(
        name="R",
        params=Params,
        steps=[step("a", fn=fn), step("b", fn=fn), step("c", fn=fn)],
    )
    dag = build_dag(p)
    nodes = {name: dag.steps[name] for name in ("a", "b", "c")}
    assert all(node.pipeline_scope == "R" for node in nodes.values())


def test_include_adapter_keeps_callers_scope_on_pipeline_scope():
    sub = pipeline(
        name="Sub", params=Params, exports="only", steps=[step("only", fn=fn)]
    )
    incl = include(name="first", pipeline=sub, fn=adapt)
    p = pipeline(
        name="R",
        params=Params,
        steps=[step("before", fn=fn), incl, step("after", fn=fn)],
    )
    dag = build_dag(p)
    assert dag.steps["before"].pipeline_scope == "R"
    assert dag.steps["first__adapter"].pipeline_scope == "R"
    # inner step collapses to include prefix when sub_step.name == exports
    assert dag.steps["first"].pipeline_scope == "R__first"
    assert dag.steps["after"].pipeline_scope == "R"


def test_nested_include_pipeline_scope_is_cumulative_path():
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
    dag = build_dag(p)
    assert dag.steps["outer__adapter"].pipeline_scope == "R"
    assert dag.steps["outer__inner__adapter"].pipeline_scope == "R__outer"
    assert dag.steps["outer__inner__i1"].pipeline_scope == "R__outer__inner"


def test_repeated_includes_produce_distinct_pipeline_scopes():
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
    dag = build_dag(p)
    assert dag.steps["first"].pipeline_scope == "R__first"
    assert dag.steps["second"].pipeline_scope == "R__second"


def test_repeated_includes_have_independent_step_total_per_scope():
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
    dag = build_dag(p)
    first = dag.steps["first"]
    second = dag.steps["second"]
    assert first.pipeline_scope == "R__first"
    assert second.pipeline_scope == "R__second"
    assert first.step_total_in_scope == 1
    assert second.step_total_in_scope == 1
    # both scope_ids unique
    assert dag.scope_step_totals == {
        "R": 2,  # adapters only (one adapter per include)
        "R__first": 1,
        "R__second": 1,
    }


def test_step_index_in_scope_is_zero_based():
    p = pipeline(
        name="R",
        params=Params,
        steps=[step("a", fn=fn), step("b", fn=fn), step("c", fn=fn)],
    )
    dag = build_dag(p)
    flat = [name for level in dag.get_execution_levels() for name in level]
    expected = {name: idx for idx, name in enumerate(flat)}
    for name, node in dag.steps.items():
        assert node.step_index_in_scope == expected[name]
        assert node.step_total_in_scope == len(dag.steps)


def test_step_index_is_topological_within_scope():
    """Diamond shape: a -> b, a -> c, b -> d, c -> d. Indices must
    reflect execution order, not declaration order."""

    class DiamondParams(NamedTuple):
        pass

    def fn_a() -> int:
        return 0

    def fn_b(a: int) -> int:
        return a

    def fn_c(a: int) -> int:
        return a

    def fn_d(b: int, c: int) -> int:
        return b + c

    p = pipeline(
        name="diamond",
        params=DiamondParams,
        steps=[
            step("a", fn=fn_a),
            step("b", fn=fn_b),
            step("c", fn=fn_c),
            step("d", fn=fn_d),
        ],
    )
    dag = build_dag(p)
    flat = [name for level in dag.get_execution_levels() for name in level]
    expected = {name: idx for idx, name in enumerate(flat)}
    for name, node in dag.steps.items():
        assert node.step_index_in_scope == expected[name]


def test_dag_exposes_scope_step_totals_dict():
    p = pipeline(
        name="R",
        params=Params,
        steps=[step("a", fn=fn), step("b", fn=fn)],
    )
    dag = build_dag(p)
    assert dag.scope_step_totals == {"R": 2}


def test_dag_node_to_serializable_includes_scope_fields():
    p = pipeline(name="R", params=Params, steps=[step("only", fn=fn)])
    dag = build_dag(p)
    serialized = dag.steps["only"].to_serializable()
    assert serialized["pipeline_scope"] == "R"
    assert serialized["step_index_in_scope"] == 0
    assert serialized["step_total_in_scope"] == 1


def test_dag_to_dict_serializes_scope_step_totals():
    p = pipeline(name="R", params=Params, steps=[step("only", fn=fn)])
    dag = build_dag(p)
    serialized = dag.to_dict()
    assert "scope_step_totals" in serialized
    assert serialized["scope_step_totals"] == {"R": 1}


def test_scope_step_totals_aggregate_by_scope():
    """Cumulative totals per scope in a tree of includes."""
    sub = pipeline(
        name="Sub", params=Params, exports="only", steps=[step("only", fn=fn)]
    )
    p = pipeline(
        name="R",
        params=Params,
        steps=[
            include(name="first", pipeline=sub, fn=adapt),
            include(name="second", pipeline=sub, fn=adapt),
            step("solo", fn=fn),
        ],
    )
    dag = build_dag(p)
    # Each include contributes 1 adapter in caller scope and 1 inner
    # in sub-scope. Plus the root has the "solo" direct step.
    assert dag.scope_step_totals == {
        "R": 3,  # first__adapter, second__adapter, solo
        "R__first": 1,
        "R__second": 1,
    }


def test_pipeline_step_is_excluded_from_scope_step_totals_to_dict():
    """Empty scope means the dag has only params + resources — no
    scope_step_totals key. Defensive: keeps existing serialization
    shape stable."""
    from synaflow.core.dag import Dag

    empty_dag = Dag(steps={})
    assert empty_dag.to_dict().get("scope_step_totals", {}) == {}
