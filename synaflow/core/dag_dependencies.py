from __future__ import annotations

import inspect
import types
from typing import Any, NamedTuple, Union

from synaflow.core.step import Step
from synaflow.core.type_compatibility import (
    ListType,
    get_type_name,
    is_iterable_type,
    is_scalar,
    is_type_compatible,
)


class DependencyValidator:
    @staticmethod
    def initialize_parameters(params: type[NamedTuple]) -> dict[str, "DagNode"]:
        from synaflow.core.dag import DagNode

        produced: dict[str, DagNode] = {}
        for field in getattr(params, "_fields", []):
            tp = getattr(params, "__annotations__", {}).get(field)
            produced[field] = DagNode(output=tp)
        return produced

    @staticmethod
    def get_safe_type_hints(fn: Any) -> dict[str, Any]:
        try:
            return getattr(fn, "__annotations__", {})
        except Exception:
            return {}

    @staticmethod
    def is_optional_or_any(tp: Any) -> bool:
        if tp is Any:
            return True
        origin = getattr(tp, "__origin__", None)
        if origin is types.UnionType or origin is Union:
            return type(None) in getattr(tp, "__args__", [])
        return False

    @classmethod
    def validate_and_resolve_dependencies(
        cls,
        step: Step,
        sig: inspect.Signature,
        hints: dict[str, Any],
        produced: dict[str, "DagNode"],
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
                and not cls.is_optional_or_any(consumer_type)
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

    @staticmethod
    def resolve_step_output_type(
        sig: inspect.Signature,
        hints: dict[str, Any],
        deps: dict[str, Any],
        produced: dict[str, "DagNode"],
    ) -> Any:
        return_type = hints.get("return", sig.return_annotation)
        if return_type is inspect.Parameter.empty:
            return_type = None

        if is_scalar(return_type) and deps:
            first_dep_name = next(iter(deps))
            first_dep_output = produced[first_dep_name].output

            # We only infer a ListType if the output is scalar AND the first dependency is iterable
            # AND the parameter type for that dependency in this function is a scalar (meaning it will be unrolled).
            # If the parameter type is iterable, the function takes the whole iterable and returns a single scalar!
            if first_dep_output is not None and is_iterable_type(first_dep_output):
                first_param_name = list(sig.parameters.keys())[0]
                first_param_type = sig.parameters[first_param_name].annotation
                if is_scalar(first_param_type):
                    return ListType(return_type)

        return return_type
