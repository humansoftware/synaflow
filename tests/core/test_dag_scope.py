"Design-time tests for `DagNode.step_index_in_scope` and\n`step_total_in_scope` (issue #105).\n\nThese verify the values are stamped onto each `DagNode` during\n`build_dag`, before any pipeline ever runs. They inspect the DAG only —\nno `run` calls, no observers, no executors.\n\nThe patterns mirror `tests/core/test_dag_expansion.py` to stay aligned\nwith what synaflow's dep wiring actually accepts. Note: we deliberately\navoid ``from __future__ import annotations`` because it changes how\n``get_type_hints`` resolves step annotations in include flows, which\nexposes a pre-existing mode-inference subtlety that's unrelated to this\nissue.\n"

from synaflow.core.dag_builder import build_dag
from typing import Iterator, NamedTuple
from synaflow import include, pipeline, step


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


def test_flat_pipeline_stamps_step_index_and_total_per_scope():
    p = pipeline(
        name="flat",
        params=BParams,
        steps=[step("func_b1", fn=func_b1), step("func_b2", fn=func_b2)],
    )
    dag = build_dag(p)
    flat_steps = [n for n in dag.steps.values() if n.pipeline == "flat"]
    assert len(flat_steps) >= 2
    for node in flat_steps:
        assert node.step_total_in_scope == len(flat_steps)
    indices = sorted((n.step_index_in_scope for n in flat_steps))
    assert indices == list(range(1, len(flat_steps) + 1))


def test_one_include_adapter_in_caller_inner_steps_in_sub():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )
    dag = build_dag(pipe_a)
    main_steps = [n for n in dag.steps.values() if n.pipeline == "MainPipeline"]
    sub_steps = [n for n in dag.steps.values() if n.pipeline == "TextProcessor"]
    assert len(main_steps) == 2
    for node in main_steps:
        assert node.step_total_in_scope == 2
    assert len(sub_steps) == 2
    for node in sub_steps:
        assert node.step_total_in_scope == 2, (
            f"Sub step {node.pipeline!r} index={node.step_index_in_scope} total={node.step_total_in_scope} expected 2"
        )


def test_multiple_includes_have_independent_scope_totals():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("textproc_a", pipeline=pipe_b, fn=prepare_b_each),
            include("textproc_b", pipeline=pipe_b, fn=prepare_b_each),
        ],
    )
    dag = build_dag(pipe_a)
    main_steps = [n for n in dag.steps.values() if n.pipeline == "MainPipeline"]
    sub_steps = [n for n in dag.steps.values() if n.pipeline == "TextProcessor"]
    assert len(main_steps) == 2
    for node in main_steps:
        assert node.step_total_in_scope == 2
    assert len(sub_steps) == 4
    for node in sub_steps:
        assert node.step_total_in_scope == 2


def _build_master_pipe():
    """Local helper to build a simple include-based pipeline for the
    invariant-regression tests below. Mirrors test_dag_expansion.py."""
    master = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each)],
    )
    return master


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
    dag = build_dag(pipe_a)
    shared = [n for n in dag.steps.values() if n.pipeline == "TextProcessor"]
    assert len(shared) == 4
    for node in shared:
        assert node.step_total_in_scope == 2
    indices = sorted((n.step_index_in_scope for n in shared))
    assert indices == [1, 1, 2, 2]


def test_dag_node_to_serializable_includes_step_index_and_total():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )
    dag = build_dag(pipe_a)
    serialized = dag.steps["my_text_processor__func_b1"].to_serializable()
    assert "step_index_in_scope" in serialized
    assert "step_total_in_scope" in serialized
    assert isinstance(serialized["step_index_in_scope"], int)
    assert isinstance(serialized["step_total_in_scope"], int)


def test_pipeline_def_fill_scope_metadata_stamps_direct_steps():
    """``PipelineDef.__post_init__`` stamps ``index_in_scope`` /
    ``total_in_scope`` on the root's DIRECT steps. Sub-pipeline steps
    are stamped by the sub-pipeline's own ``__post_init__`` when that
    instance is constructed (module-level ``pipe_b`` in this file
    was built at import time)."""
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )
    assert pipe_a.steps[0].index_in_scope == 1
    assert pipe_a.steps[0].total_in_scope == 2
    assert pipe_a.steps[1].index_in_scope == 2
    assert pipe_a.steps[1].total_in_scope == 2
    assert pipe_b.steps[0].index_in_scope == 1
    assert pipe_b.steps[0].total_in_scope == 2
    assert pipe_b.steps[1].index_in_scope == 2
    assert pipe_b.steps[1].total_in_scope == 2
    for inner in ("my_text_processor__func_b1", "my_text_processor"):
        node = build_dag(pipe_a).steps[inner]
        assert node.pipeline == "TextProcessor"
        assert node.step_total_in_scope == 2


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
    for include_step in pipe_a.steps:
        assert include_step.total_in_scope == 2
    for inner in ("first__func_b1", "first", "second__func_b1", "second"):
        assert build_dag(pipe_a).steps[inner].step_total_in_scope == 2
