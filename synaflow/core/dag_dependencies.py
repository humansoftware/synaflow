from __future__ import annotations

import inspect
import types
from typing import Any, NamedTuple, Union

from synaflow.core.dag import DagNode
from synaflow.core.definition import Step
from synaflow.core.types import StepMode
from synaflow.core.type_compatibility import (
    ListType,
    get_type_name,
    is_async_stream_type,
    is_sync_stream_type,
    is_type_compatible,
)


def initialize_parameters(params: type[NamedTuple]) -> dict[str, DagNode]:
    produced: dict[str, DagNode] = {}
    for field in getattr(params, "_fields", []):
        tp = getattr(params, "__annotations__", {}).get(field)
        produced[field] = DagNode(output=tp)
    return produced


def get_safe_type_hints(fn: Any) -> dict[str, Any]:
    try:
        return getattr(fn, "__annotations__", {})
    except Exception:
        return {}


def is_optional_or_any(tp: Any) -> bool:
    if tp is Any:
        return True
    origin = getattr(tp, "__origin__", None)
    if origin is types.UnionType or origin is Union:
        return type(None) in getattr(tp, "__args__", [])
    return False


def validate_and_resolve_dependencies(
    step: Step,
    sig: inspect.Signature,
    hints: dict[str, Any],
    produced: dict[str, DagNode],
    pipeline_name: str,
) -> dict[str, Any]:
    deps: dict[str, Any] = {}

    for param_name, param in sig.parameters.items():
        consumer_type = hints.get(param_name, param.annotation)
        if consumer_type is inspect.Parameter.empty:
            consumer_type = None

        if param_name not in produced:
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' depends on '{param_name}' "
                "but no prior step or param produces it"
            )

        producer_type = produced[param_name].output

        if (
            producer_type is type(None)
            and consumer_type is not None
            and not is_optional_or_any(consumer_type)
        ):
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' param '{param_name}': "
                f"expects {get_type_name(consumer_type)} "
                f"but '{param_name}' produces explicit NoneType"
            )

        if not is_type_compatible(producer_type, consumer_type):
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' param '{param_name}': "
                f"expects {get_type_name(consumer_type)} "
                f"but '{param_name}' produces {get_type_name(producer_type)}"
            )

        deps[param_name] = consumer_type

    return deps


def resolve_step_output_type(
    sig: inspect.Signature,
    hints: dict[str, Any],
    deps: dict[str, Any],
    produced: dict[str, DagNode],
    mode: StepMode,
) -> Any:
    return_type = hints.get("return", sig.return_annotation)
    if return_type is inspect.Parameter.empty:
        return_type = None

    if mode == StepMode.EACH and return_type not in (None, type(None)):
        if is_sync_stream_type(return_type) or is_async_stream_type(return_type):
            raise ValueError(
                "Each-mode steps cannot return stream-like outputs because Synaflow "
                "does not support nested streams in user-facing contracts."
            )
        return ListType(return_type)

    return return_type
