from synaflow import pipeline, step
from typing import NamedTuple


class SubParams(NamedTuple):
    x: int
    y: int


class MasterParams(NamedTuple):
    sub: SubParams


def my_step(sub: SubParams):
    print("Received sub:", type(sub), sub)


try:
    p = pipeline("test", params=MasterParams, steps=[step("my_step", fn=my_step)])
    print("Pipeline built successfully!")
    from synaflow.execution.sync_engine import run

    run(p, MasterParams(sub=SubParams(x=1, y=2)))
except Exception as e:
    print("Error:", e)
