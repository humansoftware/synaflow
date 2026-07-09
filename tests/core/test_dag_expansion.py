from synaflow.core.dag_builder import build_dag
from typing import Iterator, NamedTuple
import pytest
from synaflow import StepMode, include, pipeline, step


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


def test_pipeline_compiles_flattened_dag():
    pipe_a = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[
            include("my_text_processor", pipeline=pipe_b, fn=prepare_b_each),
            step("consolidate", fn=consolidate),
        ],
    )
    dag = build_dag(pipe_a)
    assert "my_text_processor__adapter" in dag
    assert "my_text_processor__func_b1" in dag
    assert "my_text_processor" in dag
    assert "consolidate" in dag
    assert "my_text_processor__adapter" in dag["my_text_processor__func_b1"]["deps"]
    assert "my_text_processor__func_b1" in dag["my_text_processor"]["deps"]
    assert "my_text_processor" in dag["consolidate"]["deps"]


def test_include_step_requires_return_type_hint():

    def bad_adapter(raw_texts: list[str]):
        return BParams(text="test")

    p = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[include("bad_sub", pipeline=pipe_b, fn=bad_adapter)],
    )
    with pytest.raises(ValueError, match="must have a return type hint"):
        build_dag(p)


def test_include_step_requires_pipeline_exports():
    pipe_no_exports = pipeline(
        name="NoExports", params=BParams, steps=[step("func_b1", fn=func_b1)]
    )
    p = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[include("bad_sub", pipeline=pipe_no_exports, fn=prepare_b_each)],
    )
    with pytest.raises(ValueError, match="does not define 'exports'"):
        build_dag(p)


def test_include_step_requires_strict_type_hint():

    def bad_type_adapter(raw_texts: list[str]) -> int:
        return 5

    p = pipeline(
        name="MainPipeline",
        params=AParams,
        steps=[include("bad_sub", pipeline=pipe_b, fn=bad_type_adapter)],
    )
    with pytest.raises(ValueError, match="must return 'BParams'"):
        build_dag(p)


def test_infinite_cycle_detection():

    class Empty(NamedTuple):
        pass

    def dummy() -> Empty:
        return Empty()

    pipe_cycle_a = pipeline(
        name="PipeA", params=Empty, exports="dummy", steps=[step("dummy", fn=dummy)]
    )
    pipe_cycle_b = pipeline(
        name="PipeB",
        params=Empty,
        exports="dummy",
        steps=[
            include("inc_a", pipeline=pipe_cycle_a, fn=dummy),
            step("dummy", fn=dummy),
        ],
    )
    pipe_cycle_a.steps.append(include("inc_b", pipeline=pipe_cycle_b, fn=dummy))
    p = pipeline(
        name="TriggerCycle",
        params=Empty,
        steps=[include("start", pipeline=pipe_cycle_a, fn=dummy)],
    )
    with pytest.raises(ValueError, match="Infinite cycle detected"):
        build_dag(p)


def test_sub_pipeline_preserves_explicit_step_mode_after_expansion():

    class ChildParams(NamedTuple):
        items: list[int]

    def emit(items: list[int]) -> Iterator[int]:
        yield from items

    def child_each(emit: int) -> int:
        return emit * 2

    child = pipeline(
        name="Child",
        params=ChildParams,
        exports="child_each",
        steps=[
            step("emit", fn=emit),
            step("child_each", fn=child_each, mode=StepMode.EACH),
        ],
    )

    class ParentParams(NamedTuple):
        items: list[int]

    def adapt(items: list[int]) -> ChildParams:
        return ChildParams(items=items)

    parent = pipeline(
        name="Parent",
        params=ParentParams,
        steps=[include("child", pipeline=child, fn=adapt)],
    )
    assert build_dag(parent).steps["child"].mode is StepMode.EACH
    assert build_dag(parent).steps["child"].each_mode_deps == ["child__emit"]


def test_sub_pipeline_preserves_explicit_all_mode_after_expansion():

    class ChildParams(NamedTuple):
        items: list[int]

    def child_all(items: list[int]) -> int:
        return len(items)

    child = pipeline(
        name="Child",
        params=ChildParams,
        exports="child_all",
        steps=[step("child_all", fn=child_all, mode=StepMode.ALL)],
    )

    class ParentParams(NamedTuple):
        items: list[int]

    def adapt(items: list[int]) -> ChildParams:
        return ChildParams(items=items)

    parent = pipeline(
        name="Parent",
        params=ParentParams,
        steps=[include("child", pipeline=child, fn=adapt)],
    )
    assert build_dag(parent).steps["child"].mode is StepMode.ALL
    assert build_dag(parent).steps["child"].each_mode_deps == []


def test_include_expansion_preserves_pipeline_metadata_and_materializer_overrides():

    def pipeline_mat(ctx):
        return list

    def step_mat(ctx):
        return tuple

    def step_err(ctx):
        return lambda exc: None

    class ChildParams(NamedTuple):
        items: list[int]

    def emit(items: list[int]) -> Iterator[int]:
        yield from items

    child = pipeline(
        name="Child",
        params=ChildParams,
        exports="emit",
        materializer=pipeline_mat,
        steps=[
            step(
                "emit",
                fn=emit,
                materializer=step_mat,
                error_materializer=step_err,
                force_materialize=True,
            )
        ],
    )

    class ParentParams(NamedTuple):
        items: list[int]

    def adapt(items: list[int]) -> ChildParams:
        return ChildParams(items=items)

    parent = pipeline(
        name="Parent",
        params=ParentParams,
        steps=[include("child", pipeline=child, fn=adapt)],
    )
    adapter = build_dag(parent).steps["child__adapter"]
    exported = build_dag(parent).steps["child"]
    assert adapter.pipeline == "Parent"
    assert adapter.parent_pipeline is None
    assert exported.pipeline == "Child"
    assert exported.parent_pipeline == "Parent"
    assert exported.materializer is tuple
    assert getattr(exported.error_materializer, "__name__", "") == "<lambda>"


def test_include_expansion_rewrites_wrapper_signature_to_adapter_and_prefixed_inputs():

    class ChildParams(NamedTuple):
        items: list[int]
        factor: int

    def emit(items: list[int]) -> Iterator[int]:
        yield from items

    def multiply(emit: int, factor: int) -> int:
        return emit * factor

    child = pipeline(
        name="Child",
        params=ChildParams,
        exports="multiply",
        steps=[step("emit", fn=emit), step("multiply", fn=multiply)],
    )

    class ParentParams(NamedTuple):
        items: list[int]
        factor: int

    def adapt(items: list[int], factor: int) -> ChildParams:
        return ChildParams(items=items, factor=factor)

    parent = pipeline(
        name="Parent",
        params=ParentParams,
        steps=[include("child", pipeline=child, fn=adapt)],
    )
    wrapped = build_dag(parent).steps["child"].fn
    signature = wrapped.__signature__
    assert list(signature.parameters) == ["child__emit", "child__adapter"]
    assert signature.parameters["child__emit"].annotation is int
    assert signature.parameters["child__adapter"].annotation is ChildParams


def test_sub_pipeline_preserves_max_in_flight_after_expansion():

    class ChildParams(NamedTuple):
        items: list[int]

    def emit(items: list[int]) -> Iterator[int]:
        yield from items

    child = pipeline(
        name="Child",
        params=ChildParams,
        exports="emit",
        steps=[step("emit", fn=emit, max_in_flight=30, force_materialize=True)],
    )

    class ParentParams(NamedTuple):
        items: list[int]

    def adapt(items: list[int]) -> ChildParams:
        return ChildParams(items=items)

    parent = pipeline(
        name="Parent",
        params=ParentParams,
        steps=[include("child", pipeline=child, fn=adapt)],
    )
    assert build_dag(parent).steps["child"].max_in_flight == 30


def test_adapter_step_serializes_default_max_in_flight():

    class ChildParams(NamedTuple):
        items: list[int]

    def emit(items: list[int]) -> Iterator[int]:
        yield from items

    child = pipeline(
        name="Child",
        params=ChildParams,
        exports="emit",
        steps=[step("emit", fn=emit, force_materialize=True)],
    )

    class ParentParams(NamedTuple):
        items: list[int]

    def adapt(items: list[int]) -> ChildParams:
        return ChildParams(items=items)

    parent = pipeline(
        name="Parent",
        params=ParentParams,
        steps=[include("child", pipeline=child, fn=adapt)],
    )
    d = parent.to_dict()
    assert d["steps"]["child__adapter"]["max_in_flight"] == 1


def test_include_with_multiple_params_fields():

    class SubParams(NamedTuple):
        x: str = ""
        y: str = ""

    def adapter() -> SubParams:
        return SubParams(x="a", y="b")

    sub_pipeline = pipeline(
        name="sub",
        params=SubParams,
        exports="done",
        steps=[step("done", fn=lambda x, y: (x, y))],
    )
    parent = pipeline(
        "broken",
        params=SubParams,
        exports="sub",
        steps=[include("sub", fn=adapter, pipeline=sub_pipeline)],
    )
    assert "sub__adapter" in build_dag(parent)
    assert "sub" in build_dag(parent)
