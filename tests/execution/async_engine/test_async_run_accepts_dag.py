from __future__ import annotations

from collections.abc import Iterator
from typing import NamedTuple
from unittest import mock

import pytest

from synaflow import async_run, pipeline, step
from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_builder import build_dag
from synaflow.core.dag_steps import validate_sync_async_consistency
from synaflow.core.definition import PipelineDef


def _simple_async_pipeline(name: str = "p") -> PipelineDef:
    class P(NamedTuple):
        x: int = 0

    async def fn(x: int) -> int:  # type: ignore[misc]
        return x  # pragma: no cover

    return pipeline(name=name, params=P, steps=[step("s", fn=fn)])


class P(NamedTuple):
    x: int = 0


def _sync_dag() -> Dag:
    """Build a Dag whose ``requires_sync_runner`` is True."""

    def sync_fn() -> Iterator[int]:
        yield 1  # pragma: no cover

    node = DagNode(fn=sync_fn, output=Iterator[int], deps={})
    dag = Dag(name="sync_only", params={}, resource_factories={}, steps={"s1": node})
    validate_sync_async_consistency(dag, "sync_only")
    assert dag.requires_sync_runner is True
    return dag


async def test_given_dag_argument_then_runs_without_recompiling():
    p = _simple_async_pipeline("a")
    dag = build_dag(p)
    with mock.patch(
        "synaflow.execution.async_engine.executor.build_dag",
        side_effect=AssertionError(
            "build_dag should not be called when a Dag is passed"
        ),
    ):
        await async_run(dag, P(x=7))


async def test_given_pipeline_def_argument_then_compiles_and_runs():
    p = _simple_async_pipeline("a")
    await async_run(p, P(x=7))


async def test_given_sync_dag_passed_to_async_run_then_raises_engine_mismatch():
    dag = _sync_dag()
    with pytest.raises(RuntimeError, match="run"):
        await async_run(dag, P(x=7))
