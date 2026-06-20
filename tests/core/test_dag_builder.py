from typing import NamedTuple

import pytest

from synaflow import StepMode, pipeline, step
from synaflow.core.definition import include
from synaflow.core.types import OnError


def test_given_scalar_params_when_constructed_then_passes():
    class P(NamedTuple):
        x: int = 5

    def fn(x: int) -> int:
        return x

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_list_param_when_constructed_then_passes():
    class P(NamedTuple):
        items: list[int] = []

    def fn(items: list[int]) -> int:
        return len(items)

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_union_param_when_constructed_then_passes():
    class P(NamedTuple):
        x: int = 5

    def fn(x: int | str) -> str:
        return str(x)

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_dependency_on_prior_step_when_constructed_then_passes():
    class P(NamedTuple):
        count: int = 3

    def s1(count: int) -> list[int]:
        return [count]

    def s2(s1: list[int]) -> int:
        return len(s1)

    pipeline(name="t", params=P, steps=[step("s1", fn=s1), step("s2", fn=s2)])


def test_given_dependency_on_future_step_when_constructed_then_raises():
    class P(NamedTuple):
        count: int = 3

    def s1(future: int) -> int:
        return future

    def s2(count: int) -> int:
        return count

    with pytest.raises(ValueError, match="no prior step"):
        pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn=s1), step("future", fn=s2)],
        )


def test_given_dependency_on_pipeline_param_when_constructed_then_passes():
    class P(NamedTuple):
        limit: int = 10

    def fn(limit: int) -> int:
        return limit

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_dependency_on_declared_resource_when_constructed_then_passes():
    class DB:
        pass

    class P(NamedTuple):
        limit: int = 10

    def fn(db: DB, limit: int) -> int:
        return limit

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="t",
        params=P,
        resources={"db": get_db},
        steps=[step("s1", fn=fn)],
    )

    assert p.dag.resources == {"db": DB}
    assert p.dag.steps["s1"].deps == {"db": DB, "limit": int}
    assert p.to_dict()["resources"] == {"db": "DB"}


def test_given_resource_name_colliding_with_params_field_when_constructed_then_raises():
    class DB:
        pass

    class P(NamedTuple):
        db: int = 10

    def fn(db: DB) -> None:
        pass

    def get_db() -> DB:
        return DB()

    with pytest.raises(ValueError, match="collides with a params field"):
        pipeline(
            name="t",
            params=P,
            resources={"db": get_db},
            steps=[step("s1", fn=fn)],
        )


def test_given_resource_name_colliding_with_step_name_when_constructed_then_raises():
    class DB:
        pass

    class P(NamedTuple):
        limit: int = 10

    def fn(limit: int) -> int:
        return limit

    def get_db() -> DB:
        return DB()

    with pytest.raises(ValueError, match="collides with a step name"):
        pipeline(
            name="t",
            params=P,
            resources={"db": get_db},
            steps=[step("db", fn=fn)],
        )


def test_given_sub_pipeline_resource_when_constructed_then_resource_is_inherited_into_parent_contract():
    class DB:
        pass

    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 10

    def use(db: DB, value: int) -> int:
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
        name="parent",
        params=Params,
        steps=[include("incl", pipeline=sub, fn=adapt)],
    )

    assert p.dag.resources == {"db": DB}
    assert p.dag.steps["incl"].deps == {"db": DB, "incl__adapter": SubParams}


def test_given_parent_and_sub_pipeline_same_resource_instance_when_constructed_then_builds():
    def shared() -> object:
        return object()

    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 10

    def use(db: object, value: int) -> int:
        return value

    sub = pipeline(
        name="sub",
        params=SubParams,
        resources={"db": shared},
        steps=[step("use", fn=use)],
        exports="use",
    )

    def adapt(value: int) -> SubParams:
        return SubParams(value=value)

    p = pipeline(
        name="parent",
        params=Params,
        resources={"db": shared},
        steps=[include("incl", pipeline=sub, fn=adapt)],
    )

    assert p.dag.resources["db"] is object


def test_given_parent_and_sub_pipeline_different_resource_instances_with_same_name_when_constructed_then_raises():
    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 10

    def use(db: object, value: int) -> int:
        return value

    def get_sub_db() -> object:
        return object()

    def get_parent_db() -> object:
        return object()

    sub = pipeline(
        name="sub",
        params=SubParams,
        resources={"db": get_sub_db},
        steps=[step("use", fn=use)],
        exports="use",
    )

    def adapt(value: int) -> SubParams:
        return SubParams(value=value)

    with pytest.raises(ValueError, match="resource 'db' is declared multiple times"):
        pipeline(
            name="parent",
            params=Params,
            resources={"db": get_parent_db},
            steps=[include("incl", pipeline=sub, fn=adapt)],
        )


def test_given_resource_factory_without_return_annotation_when_constructed_then_raises():
    class DB:
        pass

    class P(NamedTuple):
        limit: int = 10

    def fn(db: DB, limit: int) -> int:
        return limit

    def get_db():
        return DB()

    with pytest.raises(ValueError, match="must declare a return type annotation"):
        pipeline(
            name="t",
            params=P,
            resources={"db": get_db},
            steps=[step("s1", fn=fn)],
        )


def test_given_resource_factory_returning_none_when_constructed_then_raises():
    class DB:
        pass

    class P(NamedTuple):
        limit: int = 10

    def fn(db: DB, limit: int) -> int:
        return limit

    def get_db() -> None:
        return None

    with pytest.raises(ValueError, match="must not return None"):
        pipeline(
            name="t",
            params=P,
            resources={"db": get_db},
            steps=[step("s1", fn=fn)],
        )


def test_given_duplicate_step_name_when_constructed_then_raises():
    class P(NamedTuple):
        pass

    def fn():
        pass

    with pytest.raises(ValueError, match="duplicate"):
        pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn=fn), step("s1", fn=fn)],
        )


def test_given_on_error_stop_when_pipeline_created_then_forces_materialization():
    class P(NamedTuple):
        items: list[int] = [1, 2]

    def gen(items: int) -> int:
        return items

    def consumer(gen: int) -> int:
        return gen

    def downstream(consumer: int) -> int:
        return consumer

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("gen", fn=gen, on_error=OnError.CONTINUE),
            step("consumer", fn=consumer, on_error=OnError.STOP),
            step("downstream", fn=downstream, on_error=OnError.CONTINUE),
        ],
    )

    # consumer should require materialization because of OnError.STOP
    assert p.dag.needs_materialize("consumer") is True

    # gen should remain lazy because consumer processes it one-by-one
    assert p.dag.needs_materialize("gen") is False


def test_given_non_namedtuple_params_when_constructed_then_raises():
    class P:
        pass

    def fn():
        pass

    with pytest.raises(ValueError, match="must be a NamedTuple"):
        my_pipeline = pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn=fn)],
        )


def test_given_non_callable_step_when_constructed_then_raises():
    class P(NamedTuple):
        pass

    with pytest.raises(ValueError, match="must have a callable 'fn'"):
        my_pipeline = pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn="not_a_function")],
        )


def test_given_dependency_on_nonexistent_param_when_constructed_then_raises():
    class P(NamedTuple):
        pass

    def fn(missing: int):
        pass

    with pytest.raises(ValueError, match="but no prior step or param produces it"):
        my_pipeline = pipeline(
            name="t",
            params=P,
            steps=[step("s1", fn=fn)],
        )


def test_given_explicit_none_producer_and_strict_consumer_when_constructed_then_raises():
    class P(NamedTuple):
        pass

    def producer() -> type(None):
        return None

    def consumer(producer: int):
        pass

    with pytest.raises(ValueError, match="produces explicit NoneType"):
        my_pipeline = pipeline(
            name="t",
            params=P,
            steps=[step("producer", fn=producer), step("consumer", fn=consumer)],
        )


def test_given_mixed_sync_and_async_functions_when_constructed_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def sync_generator(items: list[int]) -> Iterator[int]:
        for i in items:
            yield i

    async def async_consumer(sync_generator: int):
        pass

    with pytest.raises(ValueError, match="UNRUNNABLE"):
        pipeline(
            name="test",
            params=P,
            steps=[
                step("sync_generator", fn=sync_generator),
                step("async_consumer", fn=async_consumer),
            ],
        )


def test_given_each_mode_step_with_iterable_dependency_not_in_first_parameter_when_dag_built_then_output_is_compiled_as_list_type():
    class P(NamedTuple):
        multiplier: int = 2
        items: list[int] = [1, 2, 3]

    def transform(multiplier: int, items: int) -> int:
        return multiplier * items

    p = pipeline(name="test", params=P, steps=[step("transform", fn=transform)])

    assert repr(p.dag.steps["transform"].output) == "ListType(<class 'int'>)"


def test_given_mode_each_when_return_type_is_tuple_then_output_is_compiled_as_list_of_tuples():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def pair(items: int) -> tuple[int, str]:
        return (items, str(items))

    p = pipeline(
        name="test",
        params=P,
        steps=[step("pair", fn=pair, mode=StepMode.EACH)],
    )

    assert repr(p.dag.steps["pair"].output) == "ListType(tuple[int, str])"


def test_given_mode_auto_when_each_mode_is_inferred_and_return_type_is_tuple_then_output_is_compiled_as_list_of_tuples():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def pair(items: int) -> tuple[int, str]:
        return (items, str(items))

    p = pipeline(name="test", params=P, steps=[step("pair", fn=pair)])

    assert p.dag.steps["pair"].mode is StepMode.EACH
    assert repr(p.dag.steps["pair"].output) == "ListType(tuple[int, str])"


def test_given_mode_all_when_return_type_is_tuple_then_output_remains_tuple():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def summarize(items: list[int]) -> tuple[int, int]:
        return (len(items), sum(items))

    p = pipeline(
        name="test",
        params=P,
        steps=[step("summarize", fn=summarize, mode=StepMode.ALL)],
    )

    assert p.dag.steps["summarize"].mode is StepMode.ALL
    assert p.dag.steps["summarize"].output == tuple[int, int]


def test_given_mode_each_when_return_type_is_none_then_output_remains_none():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    seen = []

    def sink(items: int) -> None:
        seen.append(items)

    p = pipeline(
        name="test",
        params=P,
        steps=[step("sink", fn=sink, mode=StepMode.EACH)],
    )

    assert p.dag.steps["sink"].mode is StepMode.EACH
    assert p.dag.steps["sink"].output in (None, type(None))


def test_given_mode_each_when_return_type_is_tuple_and_downstream_expects_list_of_tuples_then_pipeline_constructs():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def pair(items: int) -> tuple[int, str]:
        return (items, str(items))

    def consume(pair: list[tuple[int, str]]) -> int:
        return len(pair)

    p = pipeline(
        name="test",
        params=P,
        steps=[step("pair", fn=pair, mode=StepMode.EACH), step("consume", fn=consume)],
    )

    assert repr(p.dag.steps["pair"].output) == "ListType(tuple[int, str])"


def test_given_mode_auto_when_step_supports_each_then_mode_is_inferred_as_each():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> int:
        return items * 2

    p = pipeline(name="test", params=P, steps=[step("transform", fn=transform)])

    assert p.dag.steps["transform"].mode is StepMode.EACH
    assert p.dag.steps["transform"].each_mode_deps == ["items"]


def test_given_mode_each_when_signature_supports_each_then_dag_marks_step_as_each():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> int:
        return items * 2

    p = pipeline(
        name="test",
        params=P,
        steps=[step("transform", fn=transform, mode=StepMode.EACH)],
    )

    assert p.dag.steps["transform"].mode is StepMode.EACH
    assert p.dag.steps["transform"].each_mode_deps == ["items"]


def test_given_mode_all_when_signature_supports_all_then_dag_marks_step_as_all():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: list[int]) -> int:
        return len(items)

    p = pipeline(
        name="test",
        params=P,
        steps=[step("transform", fn=transform, mode=StepMode.ALL)],
    )

    assert p.dag.steps["transform"].mode is StepMode.ALL
    assert p.dag.steps["transform"].each_mode_deps == []


def test_given_mode_auto_when_signature_supports_all_then_mode_is_inferred_as_all():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: list[int]) -> int:
        return len(items)

    p = pipeline(name="test", params=P, steps=[step("transform", fn=transform)])

    assert p.dag.steps["transform"].mode is StepMode.ALL
    assert p.dag.steps["transform"].each_mode_deps == []


def test_given_mode_auto_when_multiple_dependencies_mix_scalar_and_iterable_then_only_iterable_scalar_inputs_are_marked_each():
    class P(NamedTuple):
        multiplier: int = 2
        items: list[int] = [1, 2, 3]

    def transform(multiplier: int, items: int) -> int:
        return multiplier * items

    p = pipeline(name="test", params=P, steps=[step("transform", fn=transform)])

    assert p.dag.steps["transform"].mode is StepMode.EACH
    assert p.dag.steps["transform"].each_mode_deps == ["items"]


def test_given_mode_auto_when_multiple_iterable_dependencies_are_consumed_as_scalars_then_all_are_marked_each():
    class P(NamedTuple):
        left: list[int] = [1, 2]
        right: list[int] = [10, 20]

    def pair(left: int, right: int) -> tuple[int, int]:
        return (left, right)

    p = pipeline(name="test", params=P, steps=[step("pair", fn=pair)])

    assert p.dag.steps["pair"].mode is StepMode.EACH
    assert p.dag.steps["pair"].each_mode_deps == ["left", "right"]


def test_given_mode_each_when_multiple_iterable_dependencies_are_consumed_as_scalars_then_each_mode_deps_preserve_all_matching_inputs():
    class P(NamedTuple):
        left: list[int] = [1, 2]
        right: list[int] = [10, 20]

    def pair(left: int, right: int) -> tuple[int, int]:
        return (left, right)

    p = pipeline(
        name="test",
        params=P,
        steps=[step("pair", fn=pair, mode=StepMode.EACH)],
    )

    assert p.dag.steps["pair"].mode is StepMode.EACH
    assert p.dag.steps["pair"].each_mode_deps == ["left", "right"]


def test_given_each_mode_step_with_iterable_dependency_not_in_first_parameter_and_list_downstream_when_constructed_then_passes():
    class P(NamedTuple):
        multiplier: int = 2
        items: list[int] = [1, 2, 3]

    def transform(multiplier: int, items: int) -> int:
        return multiplier * items

    def consume(transform: list[int]) -> int:
        return sum(transform)

    pipeline(
        name="test",
        params=P,
        steps=[step("transform", fn=transform), step("consume", fn=consume)],
    )
