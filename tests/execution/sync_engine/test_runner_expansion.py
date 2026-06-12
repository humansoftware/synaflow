from typing import Iterator, NamedTuple

import pytest

from synaflow import include, pipeline, run, step


class BParams(NamedTuple):
    texto: str


def func_b1(texto: str) -> str:
    return texto.upper()


def func_b2(func_b1: str) -> int:
    return len(func_b1)


pipe_b = pipeline(
    name="ProcessadorDeTexto",
    params=BParams,
    exports="func_b2",
    steps=[step("func_b1", fn=func_b1), step("func_b2", fn=func_b2)],
)


class AParams(NamedTuple):
    textos_brutos: list[str]


def preparar_b_each(textos_brutos: list[str]) -> Iterator[BParams]:
    for t in textos_brutos:
        yield BParams(texto=t)


def consolidar(meu_processador_b: list[int]) -> int:
    return sum(meu_processador_b)


def test_runner_executes_flattened_pipeline_each_mode():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("meu_processador_b", pipeline=pipe_b, fn=preparar_b_each),
            step("consolidar", fn=consolidar),
        ],
    )

    from synaflow.execution.sync_engine.pipeline import PipelineExecutor

    executor = PipelineExecutor(pipe_a)
    executor.execute(params=AParams(textos_brutos=["oi", "mundo", "synaflow"]))
    # len("oi") = 2, len("mundo") = 5, len("synaflow") = 8
    # sum = 15
    assert executor.context["consolidar"] == 15
