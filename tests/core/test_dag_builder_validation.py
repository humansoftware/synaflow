from typing import NamedTuple

import pytest

from synaflow import StepMode, pipeline, step


def test_given_duplicate_step_names_when_dag_built_then_raises():
    class Empty(NamedTuple):
        pass

    with pytest.raises(ValueError, match="duplicate"):
        pipeline(
            name="test",
            params=Empty,
            steps=[
                step("s1", fn=lambda: None),
                step("s1", fn=lambda: None),
            ],
        )


def test_given_circular_dependency_when_dag_built_then_raises():
    class Empty(NamedTuple):
        pass

    def s1(s2: int) -> int:
        return s2

    def s2(s1: int) -> int:
        return s1

    with pytest.raises(ValueError, match="no prior step"):
        pipeline(
            name="test",
            params=Empty,
            steps=[
                step("s1", fn=s1),
                step("s2", fn=s2),
            ],
        )


def test_given_mode_each_when_no_iterable_dep_can_be_unrolled_then_raises():
    class P(NamedTuple):
        count: int = 3

    def transform(count: int) -> int:
        return count * 2

    with pytest.raises(ValueError, match="forced to EACH mode"):
        pipeline(
            name="test",
            params=P,
            steps=[step("transform", fn=transform, mode=StepMode.EACH)],
        )


def test_given_mode_all_when_signature_requires_each_then_raises():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> int:
        return items * 2

    with pytest.raises(ValueError, match="forced to ALL mode"):
        pipeline(
            name="test",
            params=P,
            steps=[step("transform", fn=transform, mode=StepMode.ALL)],
        )


def test_given_mode_each_when_output_would_require_nested_streams_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> Iterator[int]:
        yield items

    with pytest.raises(ValueError, match="nested streams"):
        pipeline(
            name="test",
            params=P,
            steps=[step("transform", fn=transform, mode=StepMode.EACH)],
        )


def test_given_mode_each_when_consumer_would_require_iterator_of_lists_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> Iterator[int]:
        yield items

    def consume(transform: Iterator[list[int]]):
        pass

    with pytest.raises(ValueError, match="nested streams"):
        pipeline(
            name="test",
            params=P,
            steps=[
                step("transform", fn=transform, mode=StepMode.EACH),
                step("consume", fn=consume),
            ],
        )


def test_given_mode_each_when_only_some_dependencies_can_be_unrolled_then_non_matching_dependencies_are_not_forced():
    class P(NamedTuple):
        scalar: int = 2
        items: list[int] = [1, 2, 3]

    def transform(scalar: int, items: int) -> int:
        return scalar * items

    p = pipeline(
        name="test",
        params=P,
        steps=[step("transform", fn=transform, mode=StepMode.EACH)],
    )

    assert p.dag.steps["transform"].each_mode_deps == ["items"]


def test_given_steps_with_same_base_dataset_when_dag_built_then_raises():
    class Empty(NamedTuple):
        pass

    def fn1() -> int:
        return 1

    def fn2() -> int:
        return 2

    with pytest.raises(ValueError, match="both map to Base Dataset"):
        pipeline(
            name="test",
            params=Empty,
            steps=[
                step("user", fn=fn1),
                step("users", fn=fn2),
            ],
        )


def test_given_step_with_duplicate_base_params_when_dag_built_then_raises():
    class P(NamedTuple):
        user: int = 1

    def fn(user: int, users: int) -> int:
        return user + users

    with pytest.raises(ValueError, match="duplicate parameters"):
        pipeline(
            name="test",
            params=P,
            steps=[step("fn", fn=fn)],
        )


def test_given_smart_binding_with_singular_when_dag_built_then_resolves():
    from collections.abc import Generator

    class P(NamedTuple):
        count: int = 3

    def items(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def transform(item: int) -> int:
        return item * 2

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("items", fn=items),
            step("transform", fn=transform),
        ],
    )

    assert "items" in p.dag.steps["transform"].deps
    assert p.dag.steps["transform"].dataset_param_names == {"items": "item"}
    assert p.dag.consumers_of("items") == ["transform"]


# ---------------------------------------------------------------------------
# Terminal stream validation
# ---------------------------------------------------------------------------


def test_given_terminal_step_returning_iterator_when_not_materialized_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int]

    def gen(items: list[int]) -> Iterator[int]:
        yield from items

    with pytest.raises(ValueError, match="terminal step 'gen' returns a stream type"):
        pipeline(
            name="test",
            params=P,
            steps=[step("gen", fn=gen)],
        )


def test_given_terminal_step_returning_async_iterator_when_not_materialized_then_raises():
    from collections.abc import AsyncIterator

    class P(NamedTuple):
        items: list[int]

    async def gen(items: list[int]) -> AsyncIterator[int]:
        for item in items:
            yield item

    with pytest.raises(ValueError, match="terminal step 'gen' returns a stream type"):
        pipeline(
            name="test",
            params=P,
            steps=[step("gen", fn=gen)],
        )


def test_given_terminal_step_returning_iterator_when_force_materialize_then_builds():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int]

    def gen(items: list[int]) -> Iterator[int]:
        yield from items

    p = pipeline(
        name="test",
        params=P,
        steps=[step("gen", fn=gen, force_materialize=True)],
    )

    assert "gen" in p.dag.steps


def test_given_terminal_step_returning_none_when_not_materialized_then_builds():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int]

    def gen(items: list[int]) -> Iterator[int]:
        yield from items

    def consume(gen: Iterator[int]) -> None:
        for _item in gen:
            pass

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("gen", fn=gen),
            step("consume", fn=consume),
        ],
    )

    assert "consume" in p.dag.steps


def test_given_non_terminal_step_returning_iterator_when_not_materialized_then_builds():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int]

    def gen(items: list[int]) -> Iterator[int]:
        yield from items

    def transform(gen: Iterator[int]) -> list[int]:
        return list(gen)

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("gen", fn=gen),
            step("transform", fn=transform),
        ],
    )

    assert "gen" in p.dag.steps


def test_given_exported_step_returning_iterator_when_in_child_pipeline_then_builds():
    from collections.abc import Iterator

    class ChildParams(NamedTuple):
        items: list[int]

    def emit(items: list[int]) -> Iterator[int]:
        yield from items

    child = pipeline(
        name="Child",
        params=ChildParams,
        exports="emit",
        steps=[step("emit", fn=emit)],
    )

    assert "emit" in child.dag.steps
