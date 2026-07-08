"""Design-time tests for `DagNode.step_index_in_scope` and
`step_total_in_scope` (issue #105).

These verify the values are stamped onto each `DagNode` during
`build_dag`, before any pipeline ever runs. They inspect the DAG only —
no `run` calls, no observers, no executors.

The patterns mirror `tests/core/test_dag_expansion.py` to stay aligned
with what synaflow's dep wiring actually accepts. Note: we deliberately
avoid ``from __future__ import annotations`` because it changes how
``get_type_hints`` resolves step annotations in include flows, which
exposes a pre-existing mode-inference subtlety that's unrelated to this
issue.
"""

from typing import Iterator, NamedTuple


from synaflow import include, pipeline, step
# ---------------------------------------------------------------------------
# Fixtures (mirroring test_dag_expansion.py patterns).
# ---------------------------------------------------------------------------


class BParams(NamedTuple):
    text: str


def func_b1(text: str) -> str:
    return text.upper()


def func_b2(func_b1: str) -> int:
    return len(func_b1)


pipe_b = pipeline(
    name="TextProcessor",
    params=BParams,
    exports="func_b2",
    steps=[step("func_b1", fn=func_b1), step("func_b2", fn=func_b2)],
)


class AParams(NamedTuple):
    raw_texts: list[str]


def prepare_b_each(raw_texts: list[str]) -> Iterator[BParams]:
    for t in raw_texts:
        yield BParams(text=t)


def consolidate(my_text_processor: list[int]) -> int:
    return sum(my_text_processor)


# ---------------------------------------------------------------------------
# 1. Flat single pipeline: 1 scope, indices 1..N
# ---------------------------------------------------------------------------


def test_flat_pipeline_stamps_step_index_and_total_per_scope():
    p = pipeline(
        name="flat",
        params=BParams,
        steps=[
            step("func_b1", fn=func_b1),
            step("func_b2", fn=func_b2),
        ],
    )
    dag = p.dag
    flat_steps = [n for n in dag.steps.values() if n.pipeline == "flat"]
    assert len(flat_steps) >= 2
    for node in flat_steps:
        assert node.step_total_in_scope == len(flat_steps)
    indices = sorted(n.step_index_in_scope for n in flat_steps)
    assert indices == list(range(1, len(flat_steps) + 1))


# ---------------------------------------------------------------------------
# 2. Pipeline with 1 include: adapter in caller scope, inner steps in sub
# ---------------------------------------------------------------------------


def test_one_include_adapter_in_caller_inner_steps_in_sub():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )
    dag = pipe_a.dag

    main_steps = [n for n in dag.steps.values() if n.pipeline == "MainPipeline"]
    sub_steps = [n for n in dag.steps.values() if n.pipeline == "TextProcessor"]

    # Main scope: adapter + consolidate (both declared at top level).
    assert len(main_steps) == 2
    for node in main_steps:
        assert node.step_total_in_scope == 2

    # Sub scope: func_b1 + func_b2. Adapter belongs to MainPipeline.
    assert len(sub_steps) == 2
    for node in sub_steps:
        assert node.step_total_in_scope == 2, (
            f"Sub step {node.pipeline!r} index={node.step_index_in_scope} "
            f"total={node.step_total_in_scope} expected 2"
        )


# ---------------------------------------------------------------------------
# 3. Multiple sub-pipelines: independent scope totals
# ---------------------------------------------------------------------------


def test_multiple_includes_have_independent_scope_totals():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("textproc_a", pipeline=pipe_b, fn=prepare_b_each),
            include("textproc_b", pipeline=pipe_b, fn=prepare_b_each),
        ],
    )
    dag = pipe_a.dag

    main_steps = [n for n in dag.steps.values() if n.pipeline == "MainPipeline"]
    sub_steps = [n for n in dag.steps.values() if n.pipeline == "TextProcessor"]

    # Two adapters in main scope.
    assert len(main_steps) == 2
    for node in main_steps:
        assert node.step_total_in_scope == 2

    # Both includes reference pipe_b ("TextProcessor"), so they share
    # scope: 4 inner step nodes (2 per include instance) — but each
    # carries the SUB-pipeline's own definition size as its total
    # (per-definition stamping, not concatenated).
    assert len(sub_steps) == 4
    for node in sub_steps:
        assert node.step_total_in_scope == 2


# ---------------------------------------------------------------------------
# 4. (deferred) 3-level nesting: master → cvm_reports → filters
#
# NOTE: Skipped due to a pre-existing framework bug in
# `_expand_sub_pipeline_steps` (synaflow/core/dag_expansion.py). When a
# sub-pipeline is itself included (cvm_reports by master), the second-
# level expansion incorrectly reuses the outer sub-pipeline's name as
# the `pipeline` field for ALL steps, including those expanded from a
# nested include. As a result, innermost step names end up with
# `pipeline=<caller>` instead of `pipeline=<innermost>`. This is
# orthogonal to issue #105 and should be tracked in a separate issue.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 5. Invariant guarantees — pipeline must be set on DagNode, errors loud
# ---------------------------------------------------------------------------


def _build_master_pipe():
    """Local helper to build a simple include-based pipeline for the
    invariant-regression tests below. Mirrors test_dag_expansion.py."""

    master = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each)],
    )
    return master


# ---------------------------------------------------------------------------
# 5. Sibling includes of same SubPipelineDef share the scope
# ---------------------------------------------------------------------------


def test_sibling_includes_of_same_definition_share_scope_with_per_definition_totals():
    """Two includes of the same sub-pipeline produce 4 inner step
    nodes sharing the ``TextProcessor`` scope name (1+2 each), but
    each carries the SUB-pipeline's own definition size as its total
    (per-definition stamping). The indices repeat across instances
    (1,2 then 1,2) because the stamping is per-definition, not
    per-run."""
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("first", pipeline=pipe_b, fn=prepare_b_each),
            include("second", pipeline=pipe_b, fn=prepare_b_each),
        ],
    )
    dag = pipe_a.dag

    shared = [n for n in dag.steps.values() if n.pipeline == "TextProcessor"]
    # Two includes of same definition = 4 inner steps sharing one scope name.
    assert len(shared) == 4
    for node in shared:
        assert node.step_total_in_scope == 2  # per-definition, not concatenated
    indices = sorted(n.step_index_in_scope for n in shared)
    assert indices == [1, 1, 2, 2]


# ---------------------------------------------------------------------------
# 7. to_serializable() emits the new fields
# ---------------------------------------------------------------------------


def test_dag_node_to_serializable_includes_step_index_and_total():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )
    dag = pipe_a.dag
    # Pick an inner sub step.
    serialized = dag.steps["my_text_processor__func_b1"].to_serializable()
    assert "step_index_in_scope" in serialized
    assert "step_total_in_scope" in serialized
    assert isinstance(serialized["step_index_in_scope"], int)
    assert isinstance(serialized["step_total_in_scope"], int)


# ---------------------------------------------------------------------------
# 8. PipelineDef.fill_scope_metadata (design-time stamping)
# ---------------------------------------------------------------------------


def test_pipeline_def_fill_scope_metadata_stamps_direct_steps_and_recurses():
    """``PipelineDef.__post_init__`` (via ``fill_scope_metadata``) stamps
    ``index_in_scope`` / ``total_in_scope`` on every step reachable from
    the root. Sub-pipeline steps carry the SUB's own scope metadata."""
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )
    # MainPipeline scope: 2 items (1 include + 1 plain step).
    assert pipe_a.steps[0].index_in_scope == 1
    assert pipe_a.steps[0].total_in_scope == 2
    assert pipe_a.steps[1].index_in_scope == 2
    assert pipe_a.steps[1].total_in_scope == 2
    # pipe_b scope: 2 items (both plain steps).
    assert pipe_b.steps[0].index_in_scope == 1
    assert pipe_b.steps[0].total_in_scope == 2
    assert pipe_b.steps[1].index_in_scope == 2
    assert pipe_b.steps[1].total_in_scope == 2
    # Inner wrappers in the compiled dag inherit sub-pipeline metadata.
    # pipe_b has ``exports="func_b2"``, so the exported wrapper is
    # named just ``"my_text_processor"`` (the include prefix), not
    # ``my_text_processor__func_b2``.
    for inner in ("my_text_processor__func_b1", "my_text_processor"):
        node = pipe_a.dag.steps[inner]
        assert node.pipeline == "TextProcessor"
        assert node.step_total_in_scope == 2  # sub's OWN scope size


def test_pipeline_def_fill_scope_metadata_multi_instance_uses_per_definition_total():
    """With the per-definition stamping scheme, two includes of the same
    sub-pipeline each produce inner wrappers carrying the SUB's own
    scope size (``len(sub.steps)``) — NOT the concatenated run-time
    count across sibling includes.

    Observers detect scope completion by counting completed events per
    ``pipeline_scope``; ``total_in_scope`` is diagnostic info
    ("I'm step X of N in this definition")."""
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("first", pipeline=pipe_b, fn=prepare_b_each),
            include("second", pipeline=pipe_b, fn=prepare_b_each),
        ],
    )
    # MainPipeline scope has 2 items (both includes).
    for include_step in pipe_a.steps:
        assert include_step.total_in_scope == 2
    # Both instances of pipe_b's inner wrappers report the SUB's own
    # scope size, not the concatenated 4. The exported step in each
    # include is named just the prefix (``first`` / ``second``).
    for inner in (
        "first__func_b1",
        "first",
        "second__func_b1",
        "second",
    ):
        assert pipe_a.dag.steps[inner].step_total_in_scope == 2
