from __future__ import annotations

from collections.abc import AsyncIterator
from typing import NamedTuple
from unittest import mock

import pytest

from synaflow import pipeline, run, step
from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_builder import build_dag
from synaflow.core.dag_steps import validate_sync_async_consistency
from synaflow.core.definition import PipelineDef


def _simple_sync_pipeline(name: str = "p") -> PipelineDef:
    class P2(NamedTuple):
        x: int = 0

    def fn(x: int) -> int:
        return x

    return pipeline(name=name, params=P2, steps=[step("s", fn=fn)])


class P(NamedTuple):
    x: int = 0


def _async_dag() -> Dag:
    """Build a Dag whose ``requires_async_runner`` is True."""

    async def async_fn() -> AsyncIterator[int]:  # type: ignore[misc]
        yield 1  # pragma: no cover

    node = DagNode(fn=async_fn, output=AsyncIterator[int], deps={})
    dag = Dag(name="async_only", params={}, resource_factories={}, steps={"s1": node})
    validate_sync_async_consistency(dag, "async_only")
    assert dag.requires_async_runner is True
    return dag


def test_given_dag_argument_then_runs_without_recompiling():
    p = _simple_sync_pipeline("a")
    dag = build_dag(p)
    with mock.patch(
        "synaflow.execution.sync_engine.executor.build_dag",
        side_effect=AssertionError(
            "build_dag should not be called when a Dag is passed"
        ),
    ):
        # If run() called build_dag internally, this would raise
        # AssertionError. Passing proves the Dag argument path is taken.
        run(dag, P(x=7))


def test_given_pipeline_def_argument_then_compiles_and_runs():
    p = _simple_sync_pipeline("a")
    # No mock -- build_dag is called by run() to compile the pipeline.
    run(p, P(x=7))


def test_given_async_dag_passed_to_sync_run_then_raises_engine_mismatch():
    dag = _async_dag()
    with pytest.raises(RuntimeError, match="async_run"):
        run(dag, P(x=7))
