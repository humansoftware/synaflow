from synaflow.core.dag_builder import build_dag
from typing import Iterator, NamedTuple
from synaflow import include, pipeline, step


class BParams(NamedTuple):
    text: str


def func_b1(text: str) -> str:
    return text.upper()


def func_b2(func_b1: str) -> int:
    return len(func_b1)


pipe_b = pipeline(
    name="TextProcessor",
    params=BParams,
    exports="func_b2",
    steps=[step("func_b1", fn=func_b1), step("func_b2", fn=func_b2)],
)


class AParams(NamedTuple):
    raw_texts: list[str]


def prepare_b_each(raw_texts: list[str]) -> Iterator[BParams]:
    for t in raw_texts:
        yield BParams(text=t)


def consolidate(my_text_processor: list[int]) -> int:
    return sum(my_text_processor)


def test_runner_executes_flattened_pipeline_each_mode():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )
    from synaflow.execution.sync_engine.executor import PipelineExecutor

    executor = PipelineExecutor(build_dag(pipe_a))
    executor.execute(params=AParams(raw_texts=["hi", "world", "synaflow"]))
    assert executor.outputs["consolidate"] == 15


def test_given_sub_pipeline_resource_inherited_when_run_then_resource_is_injected():
    from synaflow.execution.sync_engine.executor import run

    class DB:
        pass

    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 3

    seen = []

    def use(db: DB, value: int) -> int:
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

    def adapt(value: int) -> SubParams:
        return SubParams(value=value)

    p = pipeline(
        name="parent", params=Params, steps=[include("incl", pipeline=sub, fn=adapt)]
    )
    run(p, Params())
    assert len(seen) == 1
    db_instance, value = seen[0]
    assert isinstance(db_instance, DB)
    assert value == 3


def test_given_two_subs_same_resource_instance_when_run_then_resource_is_injected():
    from synaflow.execution.sync_engine.executor import run

    class DB:
        pass

    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 3

    seen = []

    def use(db: DB, value: int) -> int:
        seen.append((db, value))
        return value

    shared = DB()

    def get_shared() -> DB:
        return shared

    sub_a = pipeline(
        name="sub_a",
        params=SubParams,
        resources={"db": get_shared},
        steps=[step("use", fn=use)],
        exports="use",
    )
    sub_b = pipeline(
        name="sub_b",
        params=SubParams,
        resources={"db": get_shared},
        steps=[step("use", fn=use)],
        exports="use",
    )

    def adapt_a(value: int) -> SubParams:
        return SubParams(value=value)

    def adapt_b(value: int) -> SubParams:
        return SubParams(value=value)

    p = pipeline(
        name="parent",
        params=Params,
        steps=[
            include("incl_a", pipeline=sub_a, fn=adapt_a),
            include("incl_b", pipeline=sub_b, fn=adapt_b),
        ],
    )
    run(p, Params())
    assert len(seen) == 2
    for db_instance, value in seen:
        assert db_instance is shared
        assert value == 3
