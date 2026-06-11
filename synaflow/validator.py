import inspect
from typing import Any, NamedTuple

from .step import Step
from .type_compatibility import (
    ListType,
    get_type_name,
    is_iterable_type,
    is_materialized_consumer,
    is_scalar,
    is_type_compatible,
)


def validate_and_build_dag(
    name: str, steps: list[Step], params: type[NamedTuple]
) -> dict[str, dict]:
    """
    Validates a list of steps and pipeline parameters and compiles them into a
    Directed Acyclic Graph (DAG) represented as a dictionary.
    """
    dag: dict[str, dict] = {}

    _validate_params_is_namedtuple(params, name)

    produced: dict[str, dict] = _initialize_parameters(params)

    for step in steps:
        _validate_step_is_callable(step, name)
        _validate_unique_step_name(step.name, dag, pipeline_name=name)

        node = _validate_and_compile_step(step, produced, pipeline_name=name)

        dag[step.name] = node
        produced[step.name] = node

    _add_parameter_nodes_to_dag(dag, produced)
    _compute_needs_materialize(dag)

    return dag


def _validate_params_is_namedtuple(params: Any, pipeline_name: str) -> None:
    if not hasattr(params, "_fields"):
        raise ValueError(
            f"Pipeline '{pipeline_name}': 'params' must be a NamedTuple, got {type(params).__name__}"
        )


def _validate_step_is_callable(step: Step, pipeline_name: str) -> None:
    if not callable(step.fn):
        raise ValueError(
            f"Pipeline '{pipeline_name}': step '{step.name}' must have a callable 'fn', got {type(step.fn).__name__}"
        )


def _initialize_parameters(params: type[NamedTuple]) -> dict[str, dict]:
    produced: dict[str, dict] = {}
    for field in params._fields:
        tp = getattr(params, "__annotations__", {}).get(field)
        produced[field] = {"output": tp}
    return produced


def _validate_unique_step_name(step_name: str, dag: dict, pipeline_name: str) -> None:
    if step_name in dag:
        raise ValueError(
            f"Pipeline '{pipeline_name}': duplicate step name '{step_name}'"
        )


def _validate_and_compile_step(
    step: Step, produced: dict[str, dict], pipeline_name: str
) -> dict[str, Any]:
    sig = inspect.signature(step.fn)
    hints = _get_safe_type_hints(step.fn)

    deps = _validate_and_resolve_dependencies(step, sig, hints, produced, pipeline_name)
    output_type = _resolve_step_output_type(sig, hints, deps, produced)

    return {
        "deps": deps,
        "output": output_type,
        "fn": step.fn,
        "on_error": step.on_error,
        "needs_materialize": _any_dependency_needs_materialization(deps),
    }


def _get_safe_type_hints(fn: Any) -> dict[str, Any]:
    try:
        return getattr(fn, "__annotations__", {})
    except Exception:
        return {}


def _validate_and_resolve_dependencies(
    step: Step,
    sig: inspect.Signature,
    hints: dict[str, Any],
    produced: dict[str, dict],
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

        producer_type = produced[param_name]["output"]

        # Explicit validation for NoneType to strict types
        if (
            producer_type is type(None)
            and consumer_type is not None
            and not _is_optional_or_any(consumer_type)
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


def _is_optional_or_any(tp: Any) -> bool:
    if tp is Any:
        return True
    # If it's a union containing NoneType
    origin = getattr(tp, "__origin__", None)
    import types
    from typing import Union

    if origin is types.UnionType or origin is Union:
        return type(None) in getattr(tp, "__args__", [])
    return False


def _resolve_step_output_type(
    sig: inspect.Signature,
    hints: dict[str, Any],
    deps: dict[str, Any],
    produced: dict[str, dict],
) -> Any:
    return_type = hints.get("return", sig.return_annotation)
    if return_type is inspect.Parameter.empty:
        return_type = None

    if is_scalar(return_type) and deps:
        first_dep_name = next(iter(deps))
        first_dep_output = produced[first_dep_name]["output"]
        if first_dep_output is not None and is_iterable_type(first_dep_output):
            return ListType(return_type)

    return return_type


def _any_dependency_needs_materialization(deps: dict[str, Any]) -> bool:
    return any(is_materialized_consumer(t) for t in deps.values())


def _add_parameter_nodes_to_dag(dag: dict, produced: dict) -> None:
    """Nodes that are parameters need to be explicitly added to the final DAG so they can be processed."""
    for name, info in list(produced.items()):
        if name not in dag:
            info["fn"] = None
            info["deps"] = {}
            info["on_error"] = None
            info["needs_materialize"] = False
            dag[name] = info


def _compute_needs_materialize(dag: dict) -> None:
    """Updates each node to indicate if any downstream consumer needs its output materialized."""
    for name, node in dag.items():
        consumers = [
            other_name
            for other_name, other_node in dag.items()
            if name in other_node.get("deps", {})
        ]

        node["needs_materialize"] = any(
            is_materialized_consumer(dag[consumer_name]["deps"][name])
            for consumer_name in consumers
            if consumer_name in dag and name in dag[consumer_name].get("deps", {})
        )
