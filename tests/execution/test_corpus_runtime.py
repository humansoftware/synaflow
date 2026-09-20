"""Runtime corpus execution: every registered pack runs on its own engine.

This is the runtime half of the corpus spec (the build-time half lives in
``tests/core/test_corpus_dag.py`` and ``test_dag_execution_order.py``).
Each pack executes on the engine it was written for and its observable
outputs must match the frozen ``step_results``.

Exclusions are explicit and documented — a pack that cannot run is a bug
or a spec for an unsupported pattern, and must never be silently absent.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator

import pytest

from synaflow import async_run, run
from synaflow.core.dag_builder import build_dag
from synaflow.execution.async_engine.executor import AsyncPipelineExecutor
from synaflow.execution.sync_engine.executor import PipelineExecutor
from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS
from tests.execution.async_engine.corpus.error_handling import (
    ErrorHandlingParams as AsyncErrorHandlingParams,
)
from tests.execution.async_engine.corpus.error_handling import (
    error_pipeline as async_error_pipeline,
)
from tests.execution.async_engine.corpus.error_handling import (
    errors_list as async_errors_list,
)
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS
from tests.execution.sync_engine.corpus.error_handling import (
    ErrorHandlingParams,
    error_pipeline,
    errors_list,
)

# Packs whose topologies deadlock both engines by design: the terminal
# step consumes its two lazy fan-out branches SEQUENTIALLY, which
# circular-backpressures a bounded lockstep handoff (the producer's pump
# blocks on the unread branch and starves the branch being drained).
# Lockstep consumers must consume sibling streams in lockstep (zip), as
# the mixed_fanout pack demonstrates.  Tracked in issue #128.
RUNTIME_EXCLUDED = {"complex_parallel", "complex_parallel_mixed"}

ALL_PACKS = {**SYNC_PACKS, **ASYNC_PACKS}


def _runtime_pack_cases():
    cases = []
    for pack_name, pack in sorted(ALL_PACKS.items()):
        base_name = pack_name.removeprefix("sync_").removeprefix("async_")
        if base_name in RUNTIME_EXCLUDED:
            continue
        cases.append((pack_name, pack))
    return cases


def _concrete(value):
    """Convert iterators to lists; leave scalars and tuples alone.

    A failing iterator propagates its error — a silently truncated
    "successful" drain could mask a broken step (and produce a passing
    assertion against an empty expectation)."""
    if isinstance(value, Iterator):
        return list(value)
    return value


async def _aconcrete(value):
    if value is None:
        return None
    if isinstance(value, AsyncIterator):
        return [x async for x in value]
    return _concrete(value)


def _read_step_output(outputs, dag, step_name):
    value = outputs.get(step_name)
    if value is None:
        consumers = dag.consumers_of(step_name)
        if consumers:
            key = f"{step_name}__{consumers[0]}"
            value = outputs.get(key)
    if value is None:
        return None
    return _concrete(value)


@pytest.mark.parametrize(
    "pack_name,pack",
    _runtime_pack_cases(),
    ids=[name for name, _ in _runtime_pack_cases()],
)
def test_given_corpus_pack_when_executed_then_outputs_match_step_results(
    pack_name, pack
):
    dag = build_dag(pack.pipeline)
    if pack.exception_match:
        if pack_name.startswith("sync_"):
            with pytest.raises(Exception, match=pack.exception_match):
                run(dag, pack.input_params)
        else:
            with pytest.raises(Exception, match=pack.exception_match):
                asyncio.run(async_run(dag, pack.input_params))
        return

    if pack_name.startswith("sync_"):
        executor = PipelineExecutor(dag)
        executor.execute(pack.input_params)
        outputs = executor.outputs
        read = _read_step_output
    else:
        executor = AsyncPipelineExecutor(dag)
        asyncio.run(executor.execute(pack.input_params))
        outputs = executor.outputs

        def read(outputs, dag, step_name):  # noqa: F811
            value = outputs.get(step_name)
            if value is None:
                consumers = dag.consumers_of(step_name)
                if consumers:
                    value = outputs.get(f"{step_name}__{consumers[0]}")
            if value is None:
                return None
            return asyncio.run(_aconcrete(value))

    for step_name, expected in pack.step_results.items():
        if dag.consumers_of(step_name) and not dag.needs_materialize(step_name):
            continue
        assert read(outputs, dag, step_name) == expected


def test_given_sync_error_pipeline_when_run_then_error_materializer_registers():
    errors_list.clear()
    run(build_dag(error_pipeline), ErrorHandlingParams())
    assert errors_list == ["gen failed"]


def test_given_async_error_pipeline_when_run_then_error_materializer_registers():
    async_errors_list.clear()
    asyncio.run(async_run(build_dag(async_error_pipeline), AsyncErrorHandlingParams()))
    assert async_errors_list == ["gen failed"]


def test_given_runtime_exclusions_when_checked_then_they_exist_in_both_engines():
    """Every excluded topology must exist (and be excluded) in BOTH
    engines — a one-engine exclusion hides an engine-specific bug."""
    for base in RUNTIME_EXCLUDED:
        assert f"sync_{base}" in SYNC_PACKS, base
        assert f"async_{base}" in ASYNC_PACKS, base
