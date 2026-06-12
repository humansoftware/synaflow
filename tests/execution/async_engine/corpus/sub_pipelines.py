from typing import AsyncIterator, NamedTuple

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


pipe = pipeline(
    name="MainPipeline",
    params=AParams,
    steps=[
        include("meu_processador_b", pipeline=pipe_b, fn=preparar_b_each),
        step("consolidar", fn=consolidar),
    ],
)

expected_params = AParams(textos_brutos=["oi", "mundo", "synaflow"])
expected_results = {
    "meu_processador_b__adapter": None,
    "meu_processador_b__func_b1": ["OI", "MUNDO", "SYNAFLOW"],
    "meu_processador_b": [2, 5, 8],
    "consolidar": 15,
}
