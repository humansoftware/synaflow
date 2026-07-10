from __future__ import annotations

from collections.abc import Iterator
from typing import NamedTuple
from unittest import mock

import pytest

from synaflow import async_run
from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_steps import validate_sync_async_consistency


class P(NamedTuple):
    x: int = 0


def _sync_dag() -> Dag:
    """Build a Dag whose ``requires_sync_runner`` is True."""

    def sync_fn() -> Iterator[int]:
        yield 1  # pragma: no cover

    node = DagNode(fn=sync_fn, output=Iterator[int], deps={})
    dag = Dag(
        name="sync_only",
        params={},
        resource_factories={},
        steps={"s1": node},
    )
    validate_sync_async_consistency(dag, "sync_only")
    assert dag.requires_sync_runner is True
    return dag


async def test_given_dag_argument_then_runs():
    """``async_run()`` consumes a prebuilt Dag; no compile step is reached.

    With the signature narrowed to ``Dag`` only, runtime has no
    ``PipelineDef`` to compile. As a regression guard, we mock
    ``build_dag`` (the one path that *could* re-introduce
    runtime compilation) to assert it is never called when a Dag
    is passed. If a future change wires compile back into
    ``async_run()``, this test fails before the real Dag is even
    consumed.
    """

    async def fn(x: int) -> int:  # type: ignore[misc]
        return x  # pragma: no cover

    node = DagNode(fn=fn, output=int, deps={})
    dag = Dag(
        name="regression",
        params={},
        resource_factories={},
        steps={"s": node},
        requires_async_runner=True,
    )

    with mock.patch(
        "synaflow.execution.async_engine.executor.build_dag",
        create=True,
        side_effect=AssertionError(
            "async_run() must never compile at runtime; pass a Dag"
        ),
    ):
        await async_run(dag, P(x=7))


async def test_given_sync_dag_passed_to_async_run_then_raises_engine_mismatch():
    dag = _sync_dag()
    with pytest.raises(RuntimeError, match="run"):
        await async_run(dag, P(x=7))
