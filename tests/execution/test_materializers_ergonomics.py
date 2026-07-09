from synaflow.core.dag_builder import build_dag
from synaflow.core.adapters import async_adapter
import json
import pickle
import pytest
import asyncio
from typing import NamedTuple, Iterator
from synaflow import pipeline, step, run, async_run, OnError
from synaflow.materializers.composite import (
    composite_error_materializer,
    composite_materializer,
)
from synaflow.materializers.disk import disk_materializer
from synaflow.materializers.errors import disk_error_materializer
from synaflow.serializers import (
    json_serializer,
    jsonl_serializer,
    csv_serializer,
    text_serializer,
    pickle_serializer,
)
from synaflow.core.types import ErrorMaterializeContext
from collections.abc import AsyncIterator
from synaflow import include


def test_given_step_level_error_materializer_when_dag_built_then_accepted():

    class P(NamedTuple):
        pass

    def dummy():
        pass

    def dummy_error_mat(ctx):
        return lambda exc: None

    my_pipeline = pipeline(
        name="test_build",
        params=P,
        steps=[step("s", fn=dummy, error_materializer=dummy_error_mat)],
    )
    assert build_dag(my_pipeline)["s"].error_materializer.__name__ == "<lambda>"


def test_given_pipeline_level_materializer_when_dag_built_then_resolves():

    class P(NamedTuple):
        pass

    def dummy():
        pass

    def custom_mat(ctx):
        return lambda val: val

    def custom_err_mat(ctx):
        return lambda exc: None

    my_pipeline = pipeline(
        name="test_pipe_lvl",
        params=P,
        materializer=custom_mat,
        error_materializer=custom_err_mat,
        steps=[step("s", fn=dummy)],
    )
    assert build_dag(my_pipeline)["s"].materializer.__name__ == "<lambda>"
    assert build_dag(my_pipeline)["s"].error_materializer.__name__ == "<lambda>"


def test_given_step_level_materializer_when_dag_built_then_overrides_pipeline_level():

    class P(NamedTuple):
        pass

    def dummy():
        pass

    def p_mat(ctx):
        return list

    def s_mat(ctx):
        return set

    def p_err(ctx):
        return lambda e: None

    def s_err(ctx):
        return lambda e: None

    my_pipeline = pipeline(
        name="override",
        params=P,
        materializer=p_mat,
        error_materializer=p_err,
        steps=[step("s", fn=dummy, materializer=s_mat, error_materializer=s_err)],
    )
    assert build_dag(my_pipeline)["s"].materializer is set
    assert build_dag(my_pipeline)["s"].error_materializer.__name__ == "<lambda>"


def test_given_wrapped_callable_error_materializer_when_step_fails_then_runs_on_failure():

    class P(NamedTuple):
        pass

    errors = []

    def my_handler(error_ctx):
        errors.append(str(error_ctx.exception))

    def failing_step():
        raise ValueError("failed")

    my_pipeline = pipeline(
        name="direct_handler",
        params=P,
        steps=[
            step(
                "fail",
                fn=failing_step,
                error_materializer=my_handler,
                on_error=OnError.CONTINUE,
            )
        ],
    )
    run(my_pipeline, P())
    assert errors == ["failed"]


def test_given_error_materializer_factory_when_step_fails_then_runs_on_failure():

    class P(NamedTuple):
        pass

    errors = []

    def my_factory(ctx: ErrorMaterializeContext):

        def handler(error_ctx):
            errors.append((ctx.dataset_name, str(error_ctx.exception)))

        return handler

    def failing_step():
        raise ValueError("factory failed")

    my_pipeline = pipeline(
        name="factory_handler",
        params=P,
        steps=[
            step(
                "fail",
                fn=failing_step,
                error_materializer=my_factory,
                on_error=OnError.CONTINUE,
            )
        ],
    )
    run(my_pipeline, P())
    assert errors == [("fail", "factory failed")]


def test_given_each_mode_step_with_error_materializer_when_item_fails_then_runs_on_failure():

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    errors = []

    def my_handler(error_ctx):
        errors.append(str(error_ctx.exception))

    def fail_on_2(items: int):
        if items == 2:
            raise ValueError("boom 2")
        return items

    my_pipeline = pipeline(
        name="each_mode_fail",
        params=P,
        steps=[
            step(
                "s1",
                fn=fail_on_2,
                error_materializer=my_handler,
                on_error=OnError.CONTINUE,
            )
        ],
    )
    run(my_pipeline, P())
    assert errors == ["boom 2"]


def test_given_generator_step_with_error_materializer_when_downstream_fails_then_runs_on_failure():

    class P(NamedTuple):
        pass

    errors = []

    def my_handler(error_ctx):
        errors.append(str(error_ctx.exception))

    def generator_step() -> Iterator[int]:
        yield 1
        raise ValueError("generator failed")

    def consumer_step(generator_step: list):
        return generator_step

    my_pipeline = pipeline(
        name="iter_fail",
        params=P,
        steps=[
            step(
                "generator_step",
                fn=generator_step,
                error_materializer=my_handler,
                on_error=OnError.CONTINUE,
            ),
            step("consumer_step", fn=consumer_step),
        ],
    )
    run(my_pipeline, P())
    assert errors == ["generator failed"]


@pytest.mark.asyncio
async def test_given_async_error_materializer_when_async_step_fails_then_invoked():

    class P(NamedTuple):
        pass

    errors = []

    async def async_handler(error_ctx):
        await asyncio.sleep(0.01)
        errors.append(str(error_ctx.exception))

    async def failing_step():
        raise ValueError("async failed")

    my_pipeline = pipeline(
        name="async_handler_test",
        params=P,
        steps=[
            step(
                "fail",
                fn=failing_step,
                error_materializer=async_handler,
                on_error=OnError.CONTINUE,
            )
        ],
    )
    await async_run(my_pipeline, P())
    assert errors == ["async failed"]


def test_given_serializers_when_serializing_data_then_writes_correct_output(tmp_path):
    json_path = tmp_path / "test.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json_serializer.serialize(f, {"a": 1})
    assert json.loads(json_path.read_text(encoding="utf-8")) == {"a": 1}
    jsonl_path = tmp_path / "test.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        jsonl_serializer.serialize(f, [{"a": 1}, {"b": 2}])
    assert jsonl_path.read_text(encoding="utf-8") == '{"a": 1}\n{"b": 2}\n'
    csv_path = tmp_path / "test.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        csv_serializer.serialize(f, [{"col1": "val1"}, {"col1": "val2"}])
    assert (
        csv_path.read_text(encoding="utf-8").replace("\r\n", "\n")
        == "col1\nval1\nval2\n"
    )
    csv_list_path = tmp_path / "test_list.csv"
    with open(csv_list_path, "w", newline="", encoding="utf-8") as f:
        csv_serializer.serialize(f, [["a", "b"], ["c", "d"]])
    assert (
        csv_list_path.read_text(encoding="utf-8").replace("\r\n", "\n") == "a,b\nc,d\n"
    )
    txt_path = tmp_path / "test.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        text_serializer.serialize(f, ["line1", "line2"])
    assert txt_path.read_text(encoding="utf-8") == "line1\nline2\n"
    pkl_path = tmp_path / "test.pkl"
    with open(pkl_path, "wb") as f:
        pickle_serializer.serialize(f, {"data": 123})
    with open(pkl_path, "rb") as f:
        assert pickle.load(f) == {"data": 123}


def test_given_disk_materializer_when_no_filename_then_infers_from_dataset(tmp_path):

    class P(NamedTuple):
        pass

    def step_fn():
        return [1, 2, 3]

    my_mat = disk_materializer(path=tmp_path, serializer=json_serializer)
    my_pipeline = pipeline(
        name="disk_mat_test",
        params=P,
        steps=[
            step("my_dataset", fn=step_fn, materializer=my_mat, force_materialize=True)
        ],
    )
    run(my_pipeline, P())
    expected_file = tmp_path / "my_dataset.json"
    assert expected_file.exists()
    assert json.loads(expected_file.read_text()) == [1, 2, 3]


def test_given_disk_materializer_with_filename_when_run_then_respects_override(
    tmp_path,
):

    class P(NamedTuple):
        pass

    def step_fn():
        return [1, 2]

    my_mat = disk_materializer(
        path=tmp_path, serializer=json_serializer, file_name="custom.json"
    )
    my_pipeline = pipeline(
        name="disk_override_test",
        params=P,
        steps=[step("ds", fn=step_fn, materializer=my_mat, force_materialize=True)],
    )
    run(my_pipeline, P())
    assert (tmp_path / "custom.json").exists()
    assert not (tmp_path / "ds.json").exists()


def test_given_disk_error_materializer_when_run_then_appends_error_records(tmp_path):

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def fail_always(items: int):
        raise ValueError(f"err {items}")

    my_err_mat = disk_error_materializer(path=tmp_path, serializer=jsonl_serializer)
    my_pipeline = pipeline(
        name="disk_err_test",
        params=P,
        steps=[
            step(
                "s1",
                fn=fail_always,
                error_materializer=my_err_mat,
                on_error=OnError.CONTINUE,
            )
        ],
    )
    run(my_pipeline, P())
    expected_file = tmp_path / "s1.jsonl"
    assert expected_file.exists()
    lines = expected_file.read_text().strip().split("\n")
    assert len(lines) == 3
    first_record = json.loads(lines[0])
    assert first_record["pipeline_name"] == "disk_err_test"
    assert first_record["dataset_name"] == "s1"
    assert first_record["step_name"] == "s1"
    assert first_record["run_id"]
    assert first_record["exception_type"] == "ValueError"
    assert first_record["exception_message"] == "err 1"
    assert "traceback" in first_record


def test_given_disk_error_materializer_when_using_non_append_safe_serializers_then_raises_error(
    tmp_path,
):
    with pytest.raises(ValueError, match="does not support 'JsonSerializer'"):
        disk_error_materializer(path=tmp_path, serializer=json_serializer)
    with pytest.raises(ValueError, match="does not support 'CsvSerializer'"):
        disk_error_materializer(path=tmp_path, serializer=csv_serializer)


def test_given_composite_materializer_when_run_then_calls_all_underlying_materializers(
    tmp_path,
):

    class P(NamedTuple):
        pass

    def step_fn():
        return [10, 20]

    mat1 = disk_materializer(
        path=tmp_path, serializer=json_serializer, file_name="out1.json"
    )
    mat2 = disk_materializer(
        path=tmp_path, serializer=json_serializer, file_name="out2.json"
    )
    comp = composite_materializer(mat1, mat2)
    my_pipeline = pipeline(
        name="composite_test",
        params=P,
        steps=[step("s", fn=step_fn, materializer=comp, force_materialize=True)],
    )
    run(my_pipeline, P())
    assert (tmp_path / "out1.json").exists()
    assert (tmp_path / "out2.json").exists()


def test_given_composite_error_materializer_when_fails_then_calls_all_underlying_handlers():

    class P(NamedTuple):
        pass

    calls = []

    def handler1(error_ctx):
        calls.append("one")

    def handler2(error_ctx):
        calls.append("two")

    comp = composite_error_materializer(handler1, handler2)

    def step_fn():
        raise ValueError("boom")

    my_pipeline = pipeline(
        name="composite_err_test",
        params=P,
        steps=[
            step("s", fn=step_fn, error_materializer=comp, on_error=OnError.CONTINUE)
        ],
    )
    run(my_pipeline, P())
    assert calls == ["one", "two"]


@pytest.mark.asyncio
async def test_given_async_stream_and_lazy_consumer_with_force_materialize_then_materializer_is_called():

    class P(NamedTuple):
        pass

    calls = []

    def spy_materializer(ctx):

        async def concrete(g):
            calls.append("called")
            return g

        return concrete

    async def gen():
        yield 1

    async def consumer(gen: AsyncIterator[int]):
        async for x in gen:
            pass

    my_pipeline = pipeline(
        name="test_async_lazy_force_mat",
        params=P,
        steps=[
            step("gen", fn=gen, materializer=spy_materializer, force_materialize=True),
            step("consumer", fn=consumer),
        ],
    )
    await async_run(my_pipeline, P())
    assert calls == ["called"]


def test_given_include_when_no_explicit_materializer_then_sub_steps_remain_lazy():

    class P(NamedTuple):
        pass

    def sub_gen() -> Iterator[int]:
        yield 1

    sub_pipe = pipeline(
        name="sub_pipe", params=P, steps=[step("gen", fn=sub_gen)], exports="gen"
    )

    def adapter() -> P:
        return P()

    def consumer(sub_pipe: Iterator[int]) -> list[int]:
        assert not isinstance(sub_pipe, list)
        return list(sub_pipe)

    root_pipe = pipeline(
        name="root_pipe",
        params=P,
        steps=[
            include("sub_pipe", pipeline=sub_pipe, fn=adapter),
            step("consumer", fn=consumer),
        ],
    )
    run(root_pipe, P())


@pytest.mark.asyncio
async def test_given_async_disk_error_materializer_when_run_then_appends_error_records(
    tmp_path,
):

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def fail_always(items: int):
        raise ValueError(f"err {items}")

    def my_err_mat(ctx):
        handler = disk_error_materializer(path=tmp_path, serializer=jsonl_serializer)(
            ctx
        )
        return async_adapter(handler)

    my_pipeline = pipeline(
        name="async_disk_err_test",
        params=P,
        steps=[
            step(
                "s1",
                fn=fail_always,
                error_materializer=my_err_mat,
                on_error=OnError.CONTINUE,
            )
        ],
    )
    await async_run(my_pipeline, P())
    expected_file = tmp_path / "s1.jsonl"
    assert expected_file.exists()
    lines = expected_file.read_text().strip().split("\n")
    assert len(lines) == 3
    first_record = json.loads(lines[0])
    assert first_record["pipeline_name"] == "async_disk_err_test"
    assert first_record["dataset_name"] == "s1"
    assert first_record["step_name"] == "s1"
    assert first_record["run_id"]
    assert first_record["exception_type"] == "ValueError"
    assert first_record["exception_message"] == "err 1"
    assert "traceback" in first_record


@pytest.mark.asyncio
async def test_given_async_composite_error_materializer_when_fails_then_calls_all_underlying_handlers():

    class P(NamedTuple):
        pass

    calls = []

    def handler1(error_ctx):
        calls.append("one")

    def handler2(error_ctx):
        calls.append("two")

    def comp(ctx):
        handler = composite_error_materializer(handler1, handler2)(ctx)
        return async_adapter(handler)

    async def step_fn():
        raise ValueError("boom")

    my_pipeline = pipeline(
        name="async_composite_err_test",
        params=P,
        steps=[
            step("s", fn=step_fn, error_materializer=comp, on_error=OnError.CONTINUE)
        ],
    )
    await async_run(my_pipeline, P())
    assert calls == ["one", "two"]


def test_given_include_with_explicit_pipeline_materializer_then_propagates_to_sub_steps():

    class P(NamedTuple):
        pass

    def sub_gen() -> Iterator[int]:
        yield 1

    def my_pipeline_mat(ctx):
        return list

    sub_pipe = pipeline(
        name="sub_pipe",
        params=P,
        steps=[step("gen", fn=sub_gen)],
        exports="gen",
        materializer=my_pipeline_mat,
    )

    def adapter() -> P:
        return P()

    def consumer(sub_pipe: list[int]) -> int:
        return len(sub_pipe)

    root_pipe = pipeline(
        name="root_pipe",
        params=P,
        steps=[
            include("sub_pipe", pipeline=sub_pipe, fn=adapter),
            step("consumer", fn=consumer),
        ],
    )
    assert build_dag(root_pipe).steps["sub_pipe"].materializer is list


def test_given_include_with_step_materializer_overriding_pipeline_materializer_then_step_wins():

    class P(NamedTuple):
        pass

    def sub_gen() -> Iterator[int]:
        yield 1

    def my_pipeline_mat(ctx):
        return list

    def my_step_mat(ctx):
        return set

    sub_pipe = pipeline(
        name="sub_pipe",
        params=P,
        steps=[step("gen", fn=sub_gen, materializer=my_step_mat)],
        exports="gen",
        materializer=my_pipeline_mat,
    )

    def adapter() -> P:
        return P()

    def consumer(sub_pipe: list[int]) -> int:
        return len(sub_pipe)

    root_pipe = pipeline(
        name="root_pipe",
        params=P,
        steps=[
            include("sub_pipe", pipeline=sub_pipe, fn=adapter),
            step("consumer", fn=consumer),
        ],
    )
    assert build_dag(root_pipe).steps["sub_pipe"].materializer is set


def test_given_include_with_explicit_pipeline_error_materializer_then_propagates_to_sub_steps():

    class P(NamedTuple):
        pass

    def sub_gen() -> Iterator[int]:
        yield 1

    def my_pipeline_err(ctx):
        return lambda exc: None

    sub_pipe = pipeline(
        name="sub_pipe",
        params=P,
        steps=[step("gen", fn=sub_gen, force_materialize=True)],
        exports="gen",
        error_materializer=my_pipeline_err,
    )

    def adapter() -> P:
        return P()

    root_pipe = pipeline(
        name="root_pipe",
        params=P,
        steps=[include("sub_pipe", pipeline=sub_pipe, fn=adapter)],
    )
    assert (
        build_dag(root_pipe).steps["sub_pipe"].error_materializer.__name__ == "<lambda>"
    )


def test_given_include_with_step_error_materializer_overriding_pipeline_error_materializer_then_step_wins():

    class P(NamedTuple):
        pass

    def sub_gen() -> Iterator[int]:
        yield 1

    def my_pipeline_err(ctx):
        return lambda exc: None

    def my_step_err(ctx):
        return lambda exc: None

    sub_pipe = pipeline(
        name="sub_pipe",
        params=P,
        steps=[
            step(
                "gen",
                fn=sub_gen,
                error_materializer=my_step_err,
                force_materialize=True,
            )
        ],
        exports="gen",
        error_materializer=my_pipeline_err,
    )

    def adapter() -> P:
        return P()

    root_pipe = pipeline(
        name="root_pipe",
        params=P,
        steps=[include("sub_pipe", pipeline=sub_pipe, fn=adapter)],
    )
    assert (
        build_dag(root_pipe).steps["sub_pipe"].error_materializer.__name__ == "<lambda>"
    )


@pytest.mark.asyncio
async def test_given_async_composite_error_materializer_with_async_handlers_when_fails_then_awaits_all():

    class P(NamedTuple):
        pass

    calls = []

    async def async_handler1(exc):
        await asyncio.sleep(0.001)
        calls.append("one")

    async def async_handler2(exc):
        await asyncio.sleep(0.001)
        calls.append("two")

    comp = composite_error_materializer(async_handler1, async_handler2)

    async def step_fn():
        raise ValueError("boom")

    my_pipeline = pipeline(
        name="async_composite_err_async_handlers",
        params=P,
        steps=[
            step("s", fn=step_fn, error_materializer=comp, on_error=OnError.CONTINUE)
        ],
    )
    await async_run(my_pipeline, P())
    assert calls == ["one", "two"]


@pytest.mark.asyncio
async def test_given_async_composite_materializer_with_async_sub_materializers_when_run_then_awaits_all():

    class P(NamedTuple):
        pass

    calls = []

    def async_mat1(ctx):

        async def concrete(val):
            await asyncio.sleep(0.001)
            calls.append("one")
            return val

        return concrete

    def async_mat2(ctx):

        async def concrete(val):
            await asyncio.sleep(0.001)
            calls.append("two")
            return val

        return concrete

    comp = composite_materializer(async_mat1, async_mat2)

    async def step_fn():
        return 42

    my_pipeline = pipeline(
        name="async_composite_mat_async_mats",
        params=P,
        steps=[step("s", fn=step_fn, materializer=comp, force_materialize=True)],
    )
    await async_run(my_pipeline, P())
    assert calls == ["one", "two"]


def test_given_sync_stream_and_lazy_consumer_with_step_materializer_then_materializer_not_called():

    class P(NamedTuple):
        pass

    calls = []

    def spy_materializer(ctx):

        def concrete(g):
            calls.append("called")
            return g

        return concrete

    def gen() -> Iterator[int]:
        yield 1

    def consumer(gen: Iterator[int]) -> list[int]:
        assert not isinstance(gen, list)
        return list(gen)

    my_pipeline = pipeline(
        name="test_sync_lazy_step_mat",
        params=P,
        steps=[
            step("gen", fn=gen, materializer=spy_materializer),
            step("consumer", fn=consumer),
        ],
    )
    run(my_pipeline, P())
    assert calls == []


@pytest.mark.asyncio
async def test_given_async_stream_and_lazy_consumer_with_step_materializer_then_materializer_not_called():

    class P(NamedTuple):
        pass

    calls = []

    def spy_materializer(ctx):

        async def concrete(g):
            calls.append("called")
            return g

        return concrete

    async def gen() -> AsyncIterator[int]:
        yield 1

    async def consumer(gen: AsyncIterator[int]):
        async for x in gen:
            pass

    my_pipeline = pipeline(
        name="test_async_lazy_step_mat",
        params=P,
        steps=[
            step("gen", fn=gen, materializer=spy_materializer),
            step("consumer", fn=consumer),
        ],
    )
    await async_run(my_pipeline, P())
    assert calls == []
