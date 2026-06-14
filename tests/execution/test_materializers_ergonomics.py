import json
import pickle
import pytest
import asyncio
from typing import NamedTuple, Iterator
from synaflow import (
    pipeline,
    step,
    run,
    async_run,
    OnError,
    disk_materializer,
    disk_error_materializer,
    composite_materializer,
    composite_error_materializer,
    json_serializer,
    jsonl_serializer,
    csv_serializer,
    text_serializer,
    pickle_serializer,
    to_error_materializer,
)
from synaflow.core.types import ErrorMaterializeContext


# ---------------------------------------------------------------------------
# 1. Phase 1 - Core Framework tests (Build & Resolution)
# ---------------------------------------------------------------------------


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
    assert my_pipeline.dag["s"].error_materializer is dummy_error_mat


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
    assert my_pipeline.dag["s"].materializer is custom_mat
    assert my_pipeline.dag["s"].error_materializer is custom_err_mat


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
    assert my_pipeline.dag["s"].materializer is s_mat
    assert my_pipeline.dag["s"].error_materializer is s_err


def test_given_direct_callable_types_when_dag_built_then_raises_validation_error():
    class P(NamedTuple):
        pass

    def dummy():
        pass

    # list directly
    with pytest.raises(ValueError, match="cannot be a direct type/callable 'list'"):
        pipeline(name="err1", params=P, steps=[step("s", fn=dummy, materializer=list)])

    # 0 parameter callable
    def bad_mat():
        return lambda x: x

    with pytest.raises(ValueError, match="factory must accept at least one argument"):
        pipeline(
            name="err2", params=P, steps=[step("s", fn=dummy, materializer=bad_mat)]
        )


# ---------------------------------------------------------------------------
# 2. Phase 1 - Runtime Sync/Async tests (Resolutions & Fallbacks)
# ---------------------------------------------------------------------------


def test_given_wrapped_callable_error_materializer_when_step_fails_then_runs_on_failure():
    class P(NamedTuple):
        pass

    errors = []

    # Direct handler (not a factory)
    def my_handler(exc: BaseException):
        errors.append(str(exc))

    def failing_step():
        raise ValueError("failed")

    my_pipeline = pipeline(
        name="direct_handler",
        params=P,
        steps=[
            step(
                "fail",
                fn=failing_step,
                error_materializer=to_error_materializer(my_handler),
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
        def handler(exc: BaseException):
            errors.append((ctx.dataset_name, str(exc)))

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

    def my_handler(exc: BaseException):
        errors.append(str(exc))

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
                error_materializer=to_error_materializer(my_handler),
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

    def my_handler(exc: BaseException):
        errors.append(str(exc))

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
                error_materializer=to_error_materializer(my_handler),
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

    async def async_handler(exc: BaseException):
        await asyncio.sleep(0.01)
        errors.append(str(exc))

    async def failing_step():
        raise ValueError("async failed")

    my_pipeline = pipeline(
        name="async_handler_test",
        params=P,
        steps=[
            step(
                "fail",
                fn=failing_step,
                error_materializer=to_error_materializer(async_handler),
                on_error=OnError.CONTINUE,
            )
        ],
    )
    await async_run(my_pipeline, P())
    assert errors == ["async failed"]


# ---------------------------------------------------------------------------
# 3. Phase 2 - Serializers
# ---------------------------------------------------------------------------


def test_given_serializers_when_serializing_data_then_writes_correct_output(tmp_path):
    # JSON
    json_path = tmp_path / "test.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json_serializer.serialize(f, {"a": 1})
    assert json.loads(json_path.read_text(encoding="utf-8")) == {"a": 1}

    # JSONL
    jsonl_path = tmp_path / "test.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        jsonl_serializer.serialize(f, [{"a": 1}, {"b": 2}])
    assert jsonl_path.read_text(encoding="utf-8") == '{"a": 1}\n{"b": 2}\n'

    # CSV with dict rows
    csv_path = tmp_path / "test.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        csv_serializer.serialize(f, [{"col1": "val1"}, {"col1": "val2"}])
    assert (
        csv_path.read_text(encoding="utf-8").replace("\r\n", "\n")
        == "col1\nval1\nval2\n"
    )

    # CSV with list rows
    csv_list_path = tmp_path / "test_list.csv"
    with open(csv_list_path, "w", newline="", encoding="utf-8") as f:
        csv_serializer.serialize(f, [["a", "b"], ["c", "d"]])
    assert (
        csv_list_path.read_text(encoding="utf-8").replace("\r\n", "\n") == "a,b\nc,d\n"
    )

    # Text
    txt_path = tmp_path / "test.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        text_serializer.serialize(f, ["line1", "line2"])
    assert txt_path.read_text(encoding="utf-8") == "line1\nline2\n"

    # Pickle
    pkl_path = tmp_path / "test.pkl"
    with open(pkl_path, "wb") as f:
        pickle_serializer.serialize(f, {"data": 123})
    with open(pkl_path, "rb") as f:
        assert pickle.load(f) == {"data": 123}


# ---------------------------------------------------------------------------
# 4. Phase 2 - Disk Materializer & Error Materializer
# ---------------------------------------------------------------------------


def test_given_disk_materializer_when_no_filename_then_infers_from_dataset(tmp_path):
    class P(NamedTuple):
        pass

    def step_fn():
        return [1, 2, 3]

    my_mat = disk_materializer(path=tmp_path, serializer=json_serializer)

    my_pipeline = pipeline(
        name="disk_mat_test",
        params=P,
        steps=[step("my_dataset", fn=step_fn, materializer=my_mat)],
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
        steps=[step("ds", fn=step_fn, materializer=my_mat)],
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


# ---------------------------------------------------------------------------
# 5. Composite Materializers
# ---------------------------------------------------------------------------


def test_given_composite_materializer_when_run_then_calls_all_underlying_materializers(
    tmp_path,
):
    class P(NamedTuple):
        pass

    def step_fn():
        return [10, 20]

    # Two disk materializers writing same output
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
        steps=[step("s", fn=step_fn, materializer=comp)],
    )
    run(my_pipeline, P())

    assert (tmp_path / "out1.json").exists()
    assert (tmp_path / "out2.json").exists()


def test_given_composite_error_materializer_when_fails_then_calls_all_underlying_handlers():
    class P(NamedTuple):
        pass

    calls = []

    def handler1(exc):
        calls.append("one")

    def handler2(exc):
        calls.append("two")

    comp = composite_error_materializer(
        to_error_materializer(handler1),
        to_error_materializer(handler2),
    )

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
async def test_given_async_stream_and_lazy_consumer_when_step_has_materializer_then_materializer_is_called():
    from collections.abc import AsyncIterator

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
        name="test_async_lazy_mat",
        params=P,
        steps=[
            step("gen", fn=gen, materializer=spy_materializer),
            step("consumer", fn=consumer),
        ],
    )

    await async_run(my_pipeline, P())
    assert calls == ["called"]


def test_given_include_when_no_explicit_materializer_then_sub_steps_remain_lazy():
    from collections.abc import Iterator
    from synaflow import include

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
