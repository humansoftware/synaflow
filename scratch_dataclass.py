from synaflow import pipeline, step
from dataclasses import dataclass


@dataclass
class MasterParams:
    x: int = 5


def my_step(x: int):
    print("Received x:", x)


try:
    p = pipeline("test", params=MasterParams, steps=[step("my_step", fn=my_step)])
    print("Pipeline built successfully!")
    from synaflow.execution.sync_engine import run

    run(p, MasterParams(x=10))
except Exception as e:
    print("Error:", e)
