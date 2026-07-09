from synaflow.core.dag_builder import build_dag
from typing import NamedTuple
from collections.abc import Iterator, AsyncIterator
import pytest
from synaflow import StepMode, pipeline, step, Observer, OnError


def test_given_sync_step_in_async_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    async def p() -> AsyncIterator[int]:
        yield 1

    def s(p: int) -> int:
        return p

    p = pipeline(name="test", params=Empty, steps=[step("p", fn=p), step("s", fn=s)])
    with pytest.raises(
        TypeError,
        match="step function 's' is synchronous but the pipeline runs asynchronously",
    ):
        build_dag(p)


def test_given_async_observer_in_sync_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    def p() -> Iterator[int]:
        yield 1

    async def obs_handler(item: int) -> None:
        pass

    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step(
                "p",
                fn=p,
                force_materialize=True,
                observers=[Observer(handler=obs_handler)],
            )
        ],
    )
    with pytest.raises(
        TypeError,
        match="observer handler 'obs_handler' is async but the pipeline runs synchronously",
    ):
        build_dag(p)


def test_given_sync_observer_in_async_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    async def p() -> AsyncIterator[int]:
        yield 1

    def obs_handler(item: int) -> None:
        pass

    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step(
                "p",
                fn=p,
                force_materialize=True,
                observers=[Observer(handler=obs_handler)],
            )
        ],
    )
    with pytest.raises(
        TypeError,
        match="observer handler 'obs_handler' is synchronous but the pipeline runs asynchronously",
    ):
        build_dag(p)


def test_given_async_materializer_in_sync_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    def p() -> Iterator[int]:
        yield 1

    def mat_factory(ctx):

        async def mat_handler(it):
            pass

        return mat_handler

    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("p", fn=p, force_materialize=True, materializer=mat_factory)],
    )
    with pytest.raises(
        TypeError,
        match="materializer 'mat_handler' is async but the pipeline runs synchronously",
    ):
        build_dag(p)


def test_given_sync_materializer_in_async_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    async def p() -> AsyncIterator[int]:
        yield 1

    def mat_factory(ctx):

        def mat_handler(it):
            pass

        return mat_handler

    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("p", fn=p, force_materialize=True, materializer=mat_factory)],
    )
    with pytest.raises(
        TypeError,
        match="materializer 'mat_handler' is synchronous but the pipeline runs asynchronously",
    ):
        build_dag(p)


def test_given_async_error_materializer_in_sync_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    def p() -> Iterator[int]:
        yield 1

    def err_factory(ctx):

        async def err_handler(it):
            pass

        return err_handler

    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step(
                "p",
                fn=p,
                force_materialize=True,
                on_error=OnError.CONTINUE,
                error_materializer=err_factory,
            )
        ],
    )
    with pytest.raises(
        TypeError,
        match="error_materializer 'err_handler' is async but the pipeline runs synchronously",
    ):
        build_dag(p)


def test_given_sync_error_materializer_in_async_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    async def p() -> AsyncIterator[int]:
        yield 1

    def err_factory(ctx):

        def err_handler(it):
            pass

        return err_handler

    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step(
                "p",
                fn=p,
                force_materialize=True,
                on_error=OnError.CONTINUE,
                error_materializer=err_factory,
            )
        ],
    )
    with pytest.raises(
        TypeError,
        match="error_materializer 'err_handler' is synchronous but the pipeline runs asynchronously",
    ):
        build_dag(p)


def test_given_duplicate_step_names_when_dag_built_then_raises():

    class Empty(NamedTuple):
        pass

    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("s1", fn=lambda: None), step("s1", fn=lambda: None)],
    )
    with pytest.raises(ValueError, match="duplicate"):
        build_dag(p)


def test_given_circular_dependency_when_dag_built_then_raises():

    class Empty(NamedTuple):
        pass

    def s1(s2: int) -> int:
        return s2

    def s2(s1: int) -> int:
        return s1

    p = pipeline(
        name="test", params=Empty, steps=[step("s1", fn=s1), step("s2", fn=s2)]
    )
    with pytest.raises(ValueError, match="no resource, prior step, or params field"):
        build_dag(p)


def test_given_mode_each_when_no_iterable_dep_can_be_unrolled_then_raises():

    class P(NamedTuple):
        count: int = 3

    def transform(count: int) -> int:
        return count * 2

    p = pipeline(
        name="test",
        params=P,
        steps=[step("transform", fn=transform, mode=StepMode.EACH)],
    )
    with pytest.raises(ValueError, match="forced to EACH mode"):
        build_dag(p)


def test_given_mode_all_when_signature_requires_each_then_raises():

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> int:
        return items * 2

    p = pipeline(
        name="test",
        params=P,
        steps=[step("transform", fn=transform, mode=StepMode.ALL)],
    )
    with pytest.raises(ValueError, match="forced to ALL mode"):
        build_dag(p)


def test_given_mode_each_when_output_would_require_nested_streams_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> Iterator[int]:
        yield items

    p = pipeline(
        name="test",
        params=P,
        steps=[step("transform", fn=transform, mode=StepMode.EACH)],
    )
    with pytest.raises(ValueError, match="nested streams"):
        build_dag(p)


def test_given_mode_each_when_consumer_would_require_iterator_of_lists_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def transform(items: int) -> Iterator[int]:
        yield items

    def consume(transform: Iterator[list[int]]):
        pass

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("transform", fn=transform, mode=StepMode.EACH),
            step("consume", fn=consume),
        ],
    )
    with pytest.raises(ValueError, match="nested streams"):
        build_dag(p)


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
    assert build_dag(p).steps["transform"].each_mode_deps == ["items"]


def test_given_steps_with_same_base_dataset_when_dag_built_then_raises():

    class Empty(NamedTuple):
        pass

    def fn1() -> int:
        return 1

    def fn2() -> int:
        return 2

    p = pipeline(
        name="test", params=Empty, steps=[step("user", fn=fn1), step("users", fn=fn2)]
    )
    with pytest.raises(ValueError, match="both map to Base Dataset"):
        build_dag(p)


def test_given_step_with_duplicate_base_params_when_dag_built_then_raises():

    class P(NamedTuple):
        user: int = 1

    def fn(user: int, users: int) -> int:
        return user + users

    p = pipeline(name="test", params=P, steps=[step("fn", fn=fn)])
    with pytest.raises(ValueError, match="duplicate parameters"):
        build_dag(p)


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
        steps=[step("items", fn=items), step("transform", fn=transform)],
    )
    assert "items" in build_dag(p).steps["transform"].deps
    assert build_dag(p).steps["transform"].dataset_param_names == {"items": "item"}
    assert build_dag(p).consumers_of("items") == ["transform"]


def test_given_terminal_step_returning_iterator_when_not_materialized_then_raises():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int]

    def gen(items: list[int]) -> Iterator[int]:
        yield from items

    p = pipeline(name="test", params=P, steps=[step("gen", fn=gen)])
    with pytest.raises(ValueError, match="terminal step 'gen' returns a stream type"):
        build_dag(p)


def test_given_terminal_step_returning_async_iterator_when_not_materialized_then_raises():
    from collections.abc import AsyncIterator

    class P(NamedTuple):
        items: list[int]

    async def gen(items: list[int]) -> AsyncIterator[int]:
        for item in items:
            yield item

    p = pipeline(name="test", params=P, steps=[step("gen", fn=gen)])
    with pytest.raises(ValueError, match="terminal step 'gen' returns a stream type"):
        build_dag(p)


def test_given_terminal_step_returning_iterator_when_force_materialize_then_builds():
    from collections.abc import Iterator

    class P(NamedTuple):
        items: list[int]

    def gen(items: list[int]) -> Iterator[int]:
        yield from items

    p = pipeline(
        name="test", params=P, steps=[step("gen", fn=gen, force_materialize=True)]
    )
    assert "gen" in build_dag(p).steps


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
        name="test", params=P, steps=[step("gen", fn=gen), step("consume", fn=consume)]
    )
    assert "consume" in build_dag(p).steps


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
        steps=[step("gen", fn=gen), step("transform", fn=transform)],
    )
    assert "gen" in build_dag(p).steps


def test_given_exported_step_returning_iterator_when_in_child_pipeline_then_builds():
    from collections.abc import Iterator

    class ChildParams(NamedTuple):
        items: list[int]

    def emit(items: list[int]) -> Iterator[int]:
        yield from items

    child = pipeline(
        name="Child", params=ChildParams, exports="emit", steps=[step("emit", fn=emit)]
    )
    assert "emit" in build_dag(child).steps


def test_given_non_callable_error_materializer_in_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    def dummy() -> list[int]:
        return []

    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("dummy", fn=dummy, error_materializer="not callable")],
    )
    with pytest.raises(
        TypeError, match="error materializer for step 'dummy' is not callable"
    ):
        build_dag(p)


def test_given_non_callable_materializer_in_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    def dummy() -> list[int]:
        return []

    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("dummy", fn=dummy, materializer="not callable")],
    )
    with pytest.raises(
        TypeError, match="materializer for step 'dummy' is not callable"
    ):
        build_dag(p)


def test_given_non_callable_step_fn_in_pipeline_then_raises():

    class Empty(NamedTuple):
        pass

    p = pipeline(name="test", params=Empty, steps=[step("dummy", fn="not callable")])
    with pytest.raises(ValueError, match="must have a callable 'fn'"):
        build_dag(p)
