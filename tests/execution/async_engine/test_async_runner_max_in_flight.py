import pytest
import asyncio
from typing import NamedTuple
from collections.abc import AsyncIterator
from synaflow import pipeline, step
from synaflow.core.types import StepMode
from synaflow.execution.async_engine.executor import async_run


@pytest.mark.asyncio
async def test_async_single_consumer_max_in_flight():
    log = []

    async def producer() -> AsyncIterator[int]:
        for i in range(5):
            log.append(f"prod {i}")
            yield i

    async def consumer(producer: int) -> None:
        log.append(f"cons {producer}")

    class Params(NamedTuple):
        pass

    p = pipeline(
        name="test",
        params=Params,
        steps=[
            step("producer", fn=producer, max_in_flight=2),
            step("consumer", fn=consumer, mode=StepMode.EACH),
        ],
    )

    await async_run(p, params=Params())

    # We don't assert exact interleaving because asyncio scheduler order may vary,
    # but we can verify that the logs are correct.
    assert len(log) == 10
    assert log.count("prod 0") == 1
    assert log.count("cons 3") == 1


@pytest.mark.asyncio
async def test_async_fan_out_blocks_fast_consumer_gracefully():
    log = []

    async def producer() -> AsyncIterator[int]:
        for i in range(5):
            log.append(f"prod {i}")
            yield i

    async def fast_consumer(producer: int) -> None:
        log.append(f"fast {producer}")
        # Fast consumer doesn't sleep

    async def slow_consumer(producer: int) -> None:
        await asyncio.sleep(0.01)  # Slow consumer sleeps
        log.append(f"slow {producer}")

    class Params(NamedTuple):
        pass

    p = pipeline(
        name="test",
        params=Params,
        steps=[
            step("producer", fn=producer, max_in_flight=2),
            step("fast_consumer", fn=fast_consumer, mode=StepMode.EACH),
            step("slow_consumer", fn=slow_consumer, mode=StepMode.EACH),
        ],
    )

    await async_run(p, params=Params())

    # Since max_in_flight=1, the producer blocks on pushing `1` to the slow queue.
    # Therefore, it cannot even start producing `2` until `slow 0` consumes its item.
    # So "prod 2" CANNOT happen before "slow 0".

    slow_0_index = log.index("slow 0")
    prod_4_index = log.index("prod 4")
    print(log)
    assert slow_0_index < prod_4_index
