from collections.abc import AsyncIterator, Iterator
from typing import NamedTuple

import pytest

from synaflow import pipeline, step
from synaflow.core.types import OnError
from synaflow.execution.async_engine.executor import AsyncPipelineExecutor
from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS

ASYNC_PACK_NAMES = (
    "async_explicit_modes",
    "async_linear",
    "async_diamond",
    "async_complex_parallel",
    "async_fibonacci",
    "async_complex_parallel_mixed",
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

    if pack_name == "async_error_handling":
        from tests.execution.async_engine.corpus.error_handling import errors_list

        assert errors_list == ["gen failed"]


@pytest.mark.asyncio
async def test_given_mixed_fanout_when_observed_then_producer_observer_sees_stream_values_once():
    class P(NamedTuple):
        count: int = 3

    async def gen(count: int):
        for i in range(count):
            yield i

    async def lazy(gen: AsyncIterator[int]):
        return [item async for item in gen]

    async def eager(gen: list[int]):
        return gen

    recorded = {}

    def record_step_output(step_name, output):
        recorded[step_name] = output

    my_pipeline = pipeline(
        name="observer_mixed_fanout_async",
        params=P,
        steps=[
            step("gen", fn=gen),
            step("lazy", fn=lazy),
            step("eager", fn=eager),
        ],
    )

    executor = AsyncPipelineExecutor(
        my_pipeline.dag, step_output_observers=[record_step_output]
    )
    await executor.execute(P())

    assert await _concrete(recorded["gen"]) == [0, 1, 2]
    assert recorded["lazy"] == [0, 1, 2]
    assert recorded["eager"] == [0, 1, 2]


@pytest.mark.asyncio
async def test_given_partial_stream_iteration_error_with_continue_when_observed_then_observer_sees_preserved_items():
    class P(NamedTuple):
        pass

    recorded = {}

    def record_step_output(step_name, output):
        recorded[step_name] = output

    async def source():
        yield 1
        raise ValueError("iterboom")

    async def sink(source: list[int]):
        return source

    my_pipeline = pipeline(
        name="observer_partial_async",
        params=P,
        steps=[
            step("source", fn=source, on_error=OnError.CONTINUE),
            step("sink", fn=sink),
        ],
    )

    executor = AsyncPipelineExecutor(
        my_pipeline.dag, step_output_observers=[record_step_output]
    )
    await executor.execute(P())

    assert await _concrete(recorded["source"]) == [1]
    assert recorded["sink"] == [1]
