from synaflow import pipeline, step
from typing import NamedTuple


class MyNamedTuple(NamedTuple):
    x: int
    y: int


class MasterParams(NamedTuple):
    my_named_tuple: MyNamedTuple


def my_step(my_named_tuple: MyNamedTuple):
    print("Received my_named_tuple:", type(my_named_tuple), my_named_tuple)


try:
    p = pipeline("test", params=MasterParams, steps=[step("my_step", fn=my_step)])
    print("Pipeline built successfully!")
    from synaflow.execution.sync_engine import run

    run(p, MasterParams(my_named_tuple=MyNamedTuple(1, 2)))
except Exception as e:
    print("Error:", e)
