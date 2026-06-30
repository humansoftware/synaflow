from __future__ import annotations

import inspect
import types
import typing
import dataclasses
from typing import Any, NamedTuple, Union

from synaflow.core.dag import DagNode
from synaflow.core.definition import Step
from synaflow.core.naming import get_base_dataset_name
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
    hints = {}
    try:
        hints = typing.get_type_hints(params)
    except (NameError, TypeError):
        hints = getattr(params, "__annotations__", {})
    if dataclasses.is_dataclass(params):
        param_fields = [f.name for f in dataclasses.fields(params)]
    else:
        param_fields = getattr(params, "_fields", [])

    for field in param_fields:
        tp = hints.get(field)
        produced[field] = DagNode(output=tp)
    return produced


def initialize_resources(resources: dict[str, Any]) -> dict[str, DagNode]:
    produced: dict[str, DagNode] = {}
    for name, factory in resources.items():
        produced[name] = DagNode(output=resolve_resource_output_type(name, factory))
    return produced


def resolve_resource_output_type(resource_name: str, factory: Any) -> Any:
    if not callable(factory):
        raise ValueError(
            f"Resource '{resource_name}' must be declared as a factory callable."
        )

    hints = get_safe_type_hints(factory)
    try:
        sig = inspect.signature(factory)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Resource '{resource_name}' must be inspectable as a factory callable."
        ) from exc

    return_type = hints.get("return", sig.return_annotation)
    if return_type is inspect.Signature.empty:
        factory_name = getattr(factory, "__name__", type(factory).__name__)
        raise ValueError(
            f"Resource factory '{factory_name}' for key '{resource_name}' must declare a return type annotation."
        )
    if return_type in (None, type(None)):
        factory_name = getattr(factory, "__name__", type(factory).__name__)
        raise ValueError(
            f"Resource factory '{factory_name}' for key '{resource_name}' must not return None."
        )
    return return_type


def get_safe_type_hints(fn: Any) -> dict[str, Any]:
    try:
        return typing.get_type_hints(fn, include_extras=True)
    except (NameError, TypeError):
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
    resources: dict[str, DagNode],
    pipeline_name: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    deps: dict[str, Any] = {}
    dataset_param_names: dict[str, str] = {}

    for param_name, param in sig.parameters.items():
        consumer_type = hints.get(param_name, param.annotation)
        if consumer_type is inspect.Parameter.empty:
            consumer_type = None

        if param_name in resources:
            producer_name = param_name
        elif param_name in produced:
            producer_name = param_name
        else:
            param_base = get_base_dataset_name(param_name)
            producer_name = param_name
            for key in produced:
                if get_base_dataset_name(key) == param_base:
                    producer_name = key
                    break
            else:
                raise ValueError(
                    f"Pipeline '{pipeline_name}': step '{step.name}' depends on '{param_name}' "
                    "but no prior step or param produces it"
                )

        if param_name != producer_name:
            dataset_param_names[producer_name] = param_name

        if producer_name in deps:
            first = dataset_param_names.get(producer_name, producer_name)
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' has duplicate parameters "
                f"'{param_name}' and '{first}' that both map to '{producer_name}'"
            )

        producer_type = produced[producer_name].output

        if (
            producer_type is type(None)
            and consumer_type is not None
            and not is_optional_or_any(consumer_type)
        ):
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' param '{param_name}': "
                f"expects {get_type_name(consumer_type)} "
                f"but '{producer_name}' produces explicit NoneType"
            )

        if not is_type_compatible(producer_type, consumer_type):
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' param '{param_name}': "
                f"expects {get_type_name(consumer_type)} "
                f"but '{producer_name}' produces {get_type_name(producer_type)}"
            )

        deps[producer_name] = consumer_type

    return deps, dataset_param_names


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
