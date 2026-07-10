from __future__ import annotations

from collections.abc import AsyncIterator
from typing import NamedTuple
from unittest import mock

import pytest

from synaflow import run
from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_steps import validate_sync_async_consistency


class P(NamedTuple):
    x: int = 0


def _async_dag() -> Dag:
    """Build a Dag whose ``requires_async_runner`` is True."""

    async def async_fn() -> AsyncIterator[int]:  # type: ignore[misc]
        yield 1  # pragma: no cover

    node = DagNode(fn=async_fn, output=AsyncIterator[int], deps={})
    dag = Dag(
        name="async_only",
        params={},
        resource_factories={},
        steps={"s1": node},
    )
    validate_sync_async_consistency(dag, "async_only")
    assert dag.requires_async_runner is True
    return dag


def test_given_dag_argument_then_runs():
    """``run()`` consumes a prebuilt Dag; no compile step is reached.

    With the signature narrowed to ``Dag`` only, runtime has no
    ``PipelineDef`` to compile. As a regression guard, we mock
    ``build_dag`` (the one path that *could* re-introduce
    runtime compilation) to assert it is never called when a Dag
    is passed. If a future change wires compile back into
    ``run()``, this test fails before the real Dag is even
    consumed.
    """

    def fn(x: int) -> int:
        return x

    node = DagNode(fn=fn, output=int, deps={})
    dag = Dag(
        name="regression",
        params={},
        resource_factories={},
        steps={"s": node},
        requires_sync_runner=True,
    )

    with mock.patch(
        "synaflow.execution.sync_engine.executor.build_dag",
        create=True,
        side_effect=AssertionError("run() must never compile at runtime; pass a Dag"),
    ):
        run(dag, P(x=7))


def test_given_async_dag_passed_to_sync_run_then_raises_engine_mismatch():
    dag = _async_dag()
    with pytest.raises(RuntimeError, match="async_run"):
        run(dag, P(x=7))
