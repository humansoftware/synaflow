from typing import NamedTuple
from dataclasses import dataclass
import pytest
from synaflow import StepMode, pipeline, step
from synaflow.core.definition import include
from synaflow.core.dag_builder import build_dag


def test_given_scalar_params_when_constructed_then_passes():

    class P(NamedTuple):
        x: int = 5

    def fn(x: int) -> int:
        return x

    pipeline(name="t", params=P, steps=[step("s1", fn=fn)])


def test_given_dataclass_params_when_constructed_then_passes():

    @dataclass
    class P:
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


def test_given_dependency_on_future_step_when_built_then_raises():

    class P(NamedTuple):
        count: int = 3

    def s1(future: int) -> int:
        return future

    def s2(count: int) -> int:
        return count

    p = pipeline(name="t", params=P, steps=[step("s1", fn=s1), step("future", fn=s2)])
    with pytest.raises(ValueError, match="no resource, prior step, or params field"):
        p.dag


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
        name="t", params=P, resources={"db": get_db}, steps=[step("s1", fn=fn)]
    )
    assert p.dag.get("db").output is DB
    assert p.dag.steps["s1"].deps == {"db": DB, "limit": int}
    assert p.to_dict()["resources"] == {"db": "DB"}


def test_given_resource_name_colliding_with_params_field_when_built_then_raises():

    class DB:
        pass

    class P(NamedTuple):
        db: int = 10

    def fn(db: DB) -> None:
        pass

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="t", params=P, resources={"db": get_db}, steps=[step("s1", fn=fn)]
    )
    with pytest.raises(ValueError, match="collides with a params field"):
        p.dag


def test_given_resource_name_colliding_with_step_name_when_built_then_raises():

    class DB:
        pass

    class P(NamedTuple):
        limit: int = 10

    def fn(limit: int) -> int:
        return limit

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="t", params=P, resources={"db": get_db}, steps=[step("db", fn=fn)]
    )
    with pytest.raises(ValueError, match="collides with a step name"):
        p.dag


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
        name="parent", params=Params, steps=[include("incl", pipeline=sub, fn=adapt)]
    )
    assert p.dag.get("db").output is DB
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
    assert p.dag.get("db").output is object


def test_given_parent_and_sub_pipeline_different_resource_instances_with_same_name_when_built_then_raises():

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

    p = pipeline(
        name="parent",
        params=Params,
        resources={"db": get_parent_db},
        steps=[include("incl", pipeline=sub, fn=adapt)],
    )
    with pytest.raises(ValueError, match="resource 'db' is declared multiple times"):
        p.dag


def test_given_resource_factory_without_return_annotation_when_built_then_raises():

    class DB:
        pass

    class P(NamedTuple):
        limit: int = 10

    def fn(db: DB, limit: int) -> int:
        return limit

    def get_db():
        return DB()

    p = pipeline(
        name="t", params=P, resources={"db": get_db}, steps=[step("s1", fn=fn)]
    )
    with pytest.raises(ValueError, match="must declare a return type annotation"):
        p.dag


def test_given_resource_factory_returning_none_when_built_then_raises():

    class DB:
        pass

    class P(NamedTuple):
        limit: int = 10

    def fn(db: DB, limit: int) -> int:
        return limit

    def get_db() -> None:
        return None

    p = pipeline(
        name="t", params=P, resources={"db": get_db}, steps=[step("s1", fn=fn)]
    )
    with pytest.raises(ValueError, match="must not return None"):
        p.dag


def test_given_duplicate_step_name_when_built_then_raises():

    class P(NamedTuple):
        pass

    def fn():
        pass

    p = pipeline(name="t", params=P, steps=[step("s1", fn=fn), step("s1", fn=fn)])
    with pytest.raises(ValueError, match="duplicate"):
        p.dag


def test_given_non_namedtuple_params_when_built_then_raises():

    class P:
        pass

    def fn():
        pass

    p = pipeline(name="t", params=P, steps=[step("s1", fn=fn)])
    with pytest.raises(ValueError, match="must be a NamedTuple or dataclass"):
        p.dag


def test_given_non_callable_step_when_built_then_raises():

    class P(NamedTuple):
        pass

    p = pipeline(name="t", params=P, steps=[step("s1", fn="not_a_function")])
    with pytest.raises(ValueError, match="must have a callable 'fn'"):
        p.dag


def test_given_dependency_on_nonexistent_param_when_built_then_raises():

    class P(NamedTuple):
        pass

    def fn(missing: int):
        pass

    p = pipeline(name="t", params=P, steps=[step("s1", fn=fn)])
    with pytest.raises(
        ValueError, match="but no resource, prior step, or params field produces it"
    ):
        p.dag


def test_given_undeclared_resource_used_by_step_when_built_then_raises_with_resource_hint():
    """Design-time validation: step uses resource type with no factory declared."""

    class DB:
        pass

    class P(NamedTuple):
        value: int = 1

    def use(db: DB, value: int) -> None:
        pass

    p = pipeline(name="t", params=P, steps=[step("s1", fn=use)])
    with pytest.raises(ValueError, match="did you forget to declare it in resources"):
        p.dag


def test_given_resource_not_used_by_any_step_when_built_then_raises():
    """Design-time validation: resource declared in resources={} but no step uses it."""

    class DB:
        pass

    class P(NamedTuple):
        limit: int = 10

    def use(limit: int) -> int:
        return limit

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="t", params=P, resources={"db": get_db}, steps=[step("s1", fn=use)]
    )
    with pytest.raises(ValueError, match="not used by any step"):
        p.dag


def test_given_resource_used_by_step_when_constructed_then_no_unused_error():
    """Happy path: declared resource is used by a step — no error."""

    class DB:
        pass

    class P(NamedTuple):
        value: int = 1

    def use(db: DB, value: int) -> None:
        pass

    def get_db() -> DB:
        return DB()

    p = pipeline(
        name="t", params=P, resources={"db": get_db}, steps=[step("s1", fn=use)]
    )
    assert "db" in p.dag.resource_factories


def test_given_sub_pipeline_resource_used_internally_when_constructed_then_no_unused_error():
    """Sub-pipeline resource used internally should not raise unused-resource error."""

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

    assert_no_raises = True
    try:
        pipeline(
            name="parent",
            params=Params,
            steps=[include("incl", pipeline=sub, fn=adapt)],
        )
    except ValueError as exc:
        assert_no_raises = False
        assert False, f"Unexpected ValueError raised: {exc}"
    assert assert_no_raises

    class P(NamedTuple):
        pass

    def producer() -> type(None):
        return None

    def consumer(producer: int):
        pass

    p = pipeline(
        name="t",
        params=P,
        steps=[step("producer", fn=producer), step("consumer", fn=consumer)],
    )
    with pytest.raises(ValueError, match="produces explicit NoneType"):
        p.dag


def test_given_mixed_sync_and_async_functions_when_built_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def sync_generator(items: list[int]) -> Iterator[int]:
        for i in items:
            yield i

    async def async_consumer(sync_generator: int):
        pass

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("sync_generator", fn=sync_generator),
            step("async_consumer", fn=async_consumer),
        ],
    )
    with pytest.raises(ValueError, match="UNRUNNABLE"):
        p.dag


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
        name="test", params=P, steps=[step("pair", fn=pair, mode=StepMode.EACH)]
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
        name="test", params=P, steps=[step("sink", fn=sink, mode=StepMode.EACH)]
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
        name="test", params=P, steps=[step("pair", fn=pair, mode=StepMode.EACH)]
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


def test_given_sub_pipeline_resource_when_constructed_then_merged_factories_are_stored_on_dag():

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
        name="parent", params=Params, steps=[include("incl", pipeline=sub, fn=adapt)]
    )
    assert p.dag.resource_factories == {"db": get_db}
    assert p.to_dict()["resources"] == {"db": "DB"}


def test_given_two_subs_different_resource_instances_with_same_name_when_built_then_raises_design_time():

    class SubParams(NamedTuple):
        value: int

    class Params(NamedTuple):
        value: int = 0

    def use(db: object, value: int) -> int:
        return value

    def get_db_a() -> object:
        return object()

    def get_db_b() -> object:
        return object()

    sub_a = pipeline(
        name="sub_a",
        params=SubParams,
        resources={"db": get_db_a},
        steps=[step("use", fn=use)],
        exports="use",
    )
    sub_b = pipeline(
        name="sub_b",
        params=SubParams,
        resources={"db": get_db_b},
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
    with pytest.raises(ValueError, match="resource 'db' is declared multiple times"):
        p.dag


def test_given_fill_scope_metadata_when_flat_pipeline_then_stamps_each_step():
    """``PipelineDef.fill_scope_metadata`` stamps 1-indexed position +
    total on every direct step in the caller scope (issue #105)."""

    class P(NamedTuple):
        x: int = 1

    def fn(x: int) -> int:
        return x

    p = pipeline(
        name="flat",
        params=P,
        steps=[
            step(name="alpha", fn=fn),
            step(name="beta", fn=fn),
            step(name="gamma", fn=fn),
        ],
    )
    for step_obj, expected_index in zip(p.steps, [1, 2, 3]):
        assert step_obj.index_in_scope == expected_index
        assert step_obj.total_in_scope == 3


def test_given_fill_scope_metadata_when_sub_pipeline_mix_then_stamps_per_scope():
    """Stamping recurses into ``IncludeStep.pipeline`` — sub-pipeline's
    own steps carry the SUB's scope metadata, not the caller's."""

    class P(NamedTuple):
        x: int = 1

    def fn(x: int) -> int:
        return x

    def adapt(x: int) -> P:
        return P(x=x)

    sub_pipe = pipeline(
        name="Filters", params=P, exports="only", steps=[step(name="only", fn=fn)]
    )
    include_step = include(name="include_filters", pipeline=sub_pipe, fn=adapt)
    main = pipeline(
        name="Master", params=P, steps=[step(name="alpha", fn=fn), include_step]
    )
    assert main.steps[0].index_in_scope == 1
    assert main.steps[0].total_in_scope == 2
    assert main.steps[1].index_in_scope == 2
    assert main.steps[1].total_in_scope == 2
    assert sub_pipe.steps[0].index_in_scope == 1
    assert sub_pipe.steps[0].total_in_scope == 1


def test_given_dag_node_to_serializable_includes_step_index_and_total():
    """DagNode.to_serializable() emits the new scope fields (issue #105)."""

    class P(NamedTuple):
        x: int = 1

    def fn(x: int) -> int:
        return x

    p = pipeline(name="ser", params=P, steps=[step("only", fn=fn)])
    serialized = p.dag.steps["only"].to_serializable()
    assert "step_index_in_scope" in serialized
    assert "step_total_in_scope" in serialized
    assert serialized["step_index_in_scope"] == 1
    assert serialized["step_total_in_scope"] == 1


def test_given_pipeline_def_when_build_dag_called_then_returns_dag():
    """``build_dag`` accepts a ``PipelineDef`` directly (single-arg
    signature; refactor for issue #107). Building the dag IS the
    validation — no compile side effect in ``__post_init__``."""
    from synaflow.core.dag import Dag

    class P(NamedTuple):
        x: int = 1

    def fn(x: int) -> int:
        return x

    pipe_def = pipeline(name="X", params=P, steps=[step(name="a", fn=fn)])
    dag = build_dag(pipe_def)
    assert isinstance(dag, Dag)
    assert dag.name == "X"
    assert "a" in dag.steps
