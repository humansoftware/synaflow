from __future__ import annotations

import inspect
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.types import ErrorRuntimeContext


def build_error_runtime_context(
    dag: Dag,
    node: Any,
    step_name: str,
    run_id: str,
    success_count: int = 0,
    error_count: int = 1,
    completed_all_inputs: bool | None = None,
) -> ErrorRuntimeContext:
    return ErrorRuntimeContext(
        pipeline_name=dag.name,
        dataset_name=step_name,
        step_name=step_name,
        run_id=run_id,
        mode=getattr(node, "mode", None),
        on_error=getattr(node, "on_error", None),
        success_count=success_count,
        error_count=error_count,
        completed_all_inputs=completed_all_inputs,
    )


def invoke_error_handler(
    handler: Any,
    exc: BaseException,
    runtime_context: ErrorRuntimeContext,
) -> Any:
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):
        return handler(exc)

    params = list(sig.parameters.values())
    if any(param.kind == inspect.Parameter.VAR_POSITIONAL for param in params):
        return handler(exc, runtime_context)

    positional = [
        param
        for param in params
        if param.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    if len(positional) >= 2:
        return handler(exc, runtime_context)

    for keyword in ("runtime_context", "error_context", "ctx", "context"):
        param = sig.parameters.get(keyword)
        if param is not None and param.kind in (
            inspect.Parameter.KEYWORD_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            return handler(exc, **{keyword: runtime_context})

    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params):
        return handler(exc, runtime_context=runtime_context)

    return handler(exc)
