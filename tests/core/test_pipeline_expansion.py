from typing import Iterator, NamedTuple

import pytest

from synaflow import include, pipeline, step


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


def test_pipeline_compiles_flattened_dag():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("meu_processador_b", pipeline=pipe_b, fn=preparar_b_each),
            step("consolidar", fn=consolidar),
        ],
    )

    dag = pipe_a._dag
    assert "meu_processador_b__adapter" in dag
    assert "meu_processador_b__func_b1" in dag
    assert "meu_processador_b" in dag  # This is func_b2
    assert "consolidar" in dag

    assert "meu_processador_b__adapter" in dag["meu_processador_b__func_b1"]["deps"]
    assert "meu_processador_b__func_b1" in dag["meu_processador_b"]["deps"]
    assert "meu_processador_b" in dag["consolidar"]["deps"]


def test_include_step_requires_return_type_hint():
    def bad_adapter(textos_brutos: list[str]):
        return BParams(texto="test")

    with pytest.raises(ValueError, match="must have a return type hint"):
        pipeline(
            name="MainPipeline",
            params=AParams,
            steps=[include("bad_sub", pipeline=pipe_b, fn=bad_adapter)],
        )


def test_include_step_requires_pipeline_exports():
    pipe_no_exports = pipeline(
        name="NoExports", params=BParams, steps=[step("func_b1", fn=func_b1)]
    )

    with pytest.raises(ValueError, match="does not define 'exports'"):
        pipeline(
            name="MainPipeline",
            params=AParams,
            steps=[include("bad_sub", pipeline=pipe_no_exports, fn=preparar_b_each)],
        )
