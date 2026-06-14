import pytest
import asyncio
from typing import NamedTuple
from synaflow import pipeline, step, run, async_run, include
from synaflow.core.types import OnError, ErrorInterceptorContext
from synaflow.core.exceptions import PipelineStopException


def test_sync_step_level_error_interceptor():
    class P(NamedTuple):
        x: int = 10

    intercepted = []

    def my_interceptor(exc: Exception, ctx: ErrorInterceptorContext):
        intercepted.append((ctx.pipeline_name, ctx.step_name, ctx.inputs, str(exc)))

    def failing_step(x: int):
        raise ValueError(f"boom {x}")

    my_pipeline = pipeline(
        name="test_pipeline",
        params=P,
        steps=[
            step(
                "fail",
                fn=failing_step,
                error_interceptors=[my_interceptor],
                on_error=OnError.CONTINUE,
            )
        ],
    )

    run(my_pipeline, P())

    assert len(intercepted) == 1
    assert intercepted[0] == ("test_pipeline", "fail", {"x": 10}, "boom 10")


def test_sync_pipeline_level_error_interceptor():
    class P(NamedTuple):
        items: list[int] = [1, 2]

    intercepted = []

    def my_interceptor(exc: Exception, ctx: ErrorInterceptorContext):
        intercepted.append((ctx.pipeline_name, ctx.step_name, ctx.inputs, str(exc)))

    def failing_step(items: int):
        if items == 2:
            raise ValueError(f"boom {items}")
        return items

    my_pipeline = pipeline(
        name="test_pipeline_lvl",
        params=P,
        error_interceptors=[my_interceptor],
        steps=[step("fail", fn=failing_step, on_error=OnError.CONTINUE)],
    )

    run(my_pipeline, P())

    # Only item 2 fails, so 1 interception
    assert len(intercepted) == 1
    assert intercepted[0] == ("test_pipeline_lvl", "fail", {"items": 2}, "boom 2")


def test_sub_pipeline_error_interceptor_propagation():
    class SubP(NamedTuple):
        val: int

    intercepted_sub = []
    intercepted_main = []

    def sub_interceptor(exc: Exception, ctx: ErrorInterceptorContext):
        intercepted_sub.append((ctx.step_name, ctx.inputs, str(exc)))

    def main_interceptor(exc: Exception, ctx: ErrorInterceptorContext):
        intercepted_main.append((ctx.step_name, ctx.inputs, str(exc)))

    def sub_fail(val: int):
        raise ValueError(f"sub {val}")

    sub_pipe = pipeline(
        name="sub_pipe",
        params=SubP,
        exports="sub_fail",
        error_interceptors=[sub_interceptor],
        steps=[step("sub_fail", fn=sub_fail, on_error=OnError.STOP)],
    )

    class MainP(NamedTuple):
        val: int = 42

    def prepare_sub(val: int) -> SubP:
        return SubP(val=val)

    main_pipe = pipeline(
        name="main_pipe",
        params=MainP,
        error_interceptors=[main_interceptor],
        steps=[include("sub", fn=prepare_sub, pipeline=sub_pipe)],
    )

    # Note: sub_fail within include is configured with sub_step.on_error (which is CONTINUE by default)
    # Wait, the adapter step has OnError.STOP by design in macro expansion. But the sub_fail step has on_error=OnError.CONTINUE.
    # When sub_fail fails, it raises and runs interceptors.
    # Since its on_error is CONTINUE, it continues. But wait, in synaflow, included steps that raise exceptions will propagate up.
    with pytest.raises(PipelineStopException):
        run(main_pipe, MainP())

    # We expect BOTH interceptors to have run, sub_interceptor first
    assert len(intercepted_sub) == 1
    assert len(intercepted_main) == 1
    # Check that inputs contains the scoped adapter name
    assert intercepted_sub[0][0] == "sub"  # The exported step is named `prefix` (sub)
    assert intercepted_sub[0][1] == {"sub__adapter": SubP(val=42)}
    assert intercepted_main[0][0] == "sub"
    assert intercepted_main[0][1] == {"sub__adapter": SubP(val=42)}


@pytest.mark.asyncio
async def test_async_error_interceptors():
    class P(NamedTuple):
        items: list[int] = [10, 20]

    intercepted = []

    async def async_interceptor(exc: Exception, ctx: ErrorInterceptorContext):
        await asyncio.sleep(0.01)
        intercepted.append((ctx.step_name, ctx.inputs, str(exc)))

    async def failing_async(items: int):
        if items == 20:
            raise ValueError(f"async boom {items}")
        return items

    my_pipeline = pipeline(
        name="async_pipeline",
        params=P,
        steps=[
            step(
                "fail",
                fn=failing_async,
                error_interceptors=[async_interceptor],
                on_error=OnError.CONTINUE,
            )
        ],
    )

    await async_run(my_pipeline, P())

    assert len(intercepted) == 1
    assert intercepted[0] == ("fail", {"items": 20}, "async boom 20")


def test_multiple_interceptors_execution_order():
    class P(NamedTuple):
        x: int = 5

    order = []

    def int1(exc, ctx):
        order.append("step1")

    def int2(exc, ctx):
        order.append("step2")

    def int_pipe(exc, ctx):
        order.append("pipe")

    def step_fn(x: int):
        raise RuntimeError("oops")

    my_pipeline = pipeline(
        name="order_pipe",
        params=P,
        error_interceptors=[int_pipe],
        steps=[
            step(
                "s",
                fn=step_fn,
                error_interceptors=[int1, int2],
                on_error=OnError.CONTINUE,
            )
        ],
    )

    run(my_pipeline, P())

    # Step level runs in registration order, then pipeline level
    assert order == ["step1", "step2", "pipe"]
