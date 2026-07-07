from typing import AsyncIterator, NamedTuple

import pytest

from synaflow import include, pipeline, step


class BParams(NamedTuple):
    texto: str


async def func_b1(texto: str) -> str:
    return texto.upper()


async def func_b2(func_b1: str) -> int:
    return len(func_b1)


pipe_b = pipeline(
    name="ProcessadorDeTexto",
    params=BParams,
    exports="func_b2",
    steps=[step("func_b1", fn=func_b1), step("func_b2", fn=func_b2)],
)


class AParams(NamedTuple):
    textos_brutos: list[str]


async def preparar_b_each(textos_brutos: list[str]) -> AsyncIterator[BParams]:
    for t in textos_brutos:
        yield BParams(texto=t)


async def consolidar(meu_processador_b: list[int]) -> int:
    return sum(meu_processador_b)


@pytest.mark.asyncio
async def test_runner_executes_flattened_pipeline_each_mode():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("meu_processador_b", pipeline=pipe_b, fn=preparar_b_each),
            step("consolidar", fn=consolidar),
        ],
    )

    from synaflow.execution.async_engine.executor import AsyncPipelineExecutor

    executor = AsyncPipelineExecutor(pipe_a.dag)
    await executor.execute(params=AParams(textos_brutos=["oi", "mundo", "synaflow"]))
    # len("oi") = 2, len("mundo") = 5, len("synaflow") = 8
    # sum = 15
    assert executor.outputs["consolidar"] == 15


@pytest.mark.asyncio
async def test_given_sub_pipeline_resource_inherited_when_run_then_resource_is_injected():
    from synaflow import async_run

    class DB:
        pass

    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 3

    seen = []

    async def use(db: DB, value: int) -> int:
        seen.append((db, value))
        return value

    def get_db() -> DB:
        return DB()

    sub = pipeline(
        name="sub",
        params=SubParams,
        resources={"db": get_db},
        steps=[step("use", fn=use)],
        exports="use",
    )

    # The include adapter must be async because async_run() enforces
    # all step fns (including the generated adapter step) to be async
    # for an async pipeline (see PipelineDef._validate_no_sync_handlers).
    async def adapt(value: int) -> SubParams:
        return SubParams(value=value)

    p = pipeline(
        name="parent",
        params=Params,
        steps=[include("incl", pipeline=sub, fn=adapt)],
    )

    await async_run(p, Params())

    assert len(seen) == 1
    db_instance, value = seen[0]
    assert isinstance(db_instance, DB)
    assert value == 3
