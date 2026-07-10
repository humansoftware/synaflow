from synaflow.core.dag_builder import build_dag
from collections.abc import AsyncIterator, Iterator
import pytest
from synaflow import async_run
from synaflow.execution.async_engine.executor import AsyncPipelineExecutor
from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS
from tests.execution.async_engine.corpus.error_handling import (
    error_pipeline,
    ErrorHandlingParams,
    errors_list,
)

ASYNC_PACK_NAMES = (
    "async_explicit_modes",
    "async_linear",
    "async_diamond",
    "async_fibonacci",
    "async_mixed_fanout",
    "async_sub_pipelines",
    "async_deep_sub_pipelines",
    "async_error_handling",
)


async def _concrete(value):
    if value is None:
        return None
    if isinstance(value, AsyncIterator):
        return [x async for x in value]
    if isinstance(value, Iterator):
        return list(value)
    return value


def _read_step_output(outputs, dag, step_name):
    value = outputs.get(step_name)
    if value is None:
        consumers = dag.consumers_of(step_name)
        if consumers:
            key = f"{step_name}__{consumers[0]}"
            value = outputs.get(key)
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize("pack_name", ASYNC_PACK_NAMES)
async def test_step_results(pack_name):
    pack = ASYNC_PACKS[pack_name]
    executor = AsyncPipelineExecutor(build_dag(pack.pipeline))
    if pack.exception_match:
        with pytest.raises(Exception, match=pack.exception_match):
            await executor.execute(pack.input_params)
        return
    await executor.execute(pack.input_params)
    for step_name, expected in pack.step_results.items():
        if build_dag(pack.pipeline).consumers_of(step_name) and (
            not build_dag(pack.pipeline).needs_materialize(step_name)
        ):
            continue
        actual = await _concrete(
            _read_step_output(executor.outputs, build_dag(pack.pipeline), step_name)
        )
        assert actual == expected


@pytest.mark.asyncio
async def test_error_handling_corpus_registers_error():
    errors_list.clear()
    await async_run(build_dag(error_pipeline), ErrorHandlingParams())
    assert errors_list == ["gen failed"]
