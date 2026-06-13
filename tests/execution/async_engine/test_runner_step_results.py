from collections.abc import AsyncIterator, Iterator

import pytest

from synaflow.execution.async_engine.executor import AsyncPipelineExecutor
from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS

ASYNC_PACK_NAMES = (
    "async_linear",
    "async_diamond",
    "async_complex_parallel",
    "async_fibonacci",
    "async_complex_parallel_mixed",
    "async_mixed_fanout",
    "async_sub_pipelines",
    "async_deep_sub_pipelines",
)


async def _concrete(value):
    if value is None:
        return None
    if isinstance(value, AsyncIterator):
        return [x async for x in value]
    if isinstance(value, Iterator):
        return list(value)
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize("pack_name", ASYNC_PACK_NAMES)
async def test_step_results(pack_name):
    pack = ASYNC_PACKS[pack_name]

    recorded = {}

    def record_step_output(step_name, output):
        recorded[step_name] = output

    executor = AsyncPipelineExecutor(
        pack.pipeline.dag, step_output_observers=[record_step_output]
    )

    if pack.exception_match:
        with pytest.raises(Exception, match=pack.exception_match):
            await executor.execute(pack.input_params)
        return

    await executor.execute(pack.input_params)

    for step_name, expected in pack.step_results.items():
        actual = await _concrete(recorded.get(step_name))
        assert actual == expected
