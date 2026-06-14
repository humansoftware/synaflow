"""
DAG Builder: compiles a pipeline definition into a validated Dag.

build_dag() orchestrates the full compilation pipeline:
  1. Expand macros (flatten IncludeSteps into plain Steps)
  2. Initialize params from the NamedTuple
  3. Compile each step (validate types, resolve deps, infer output type)
  4. Resolve materializers (step → pipeline → global default)
  5. Compute materialized_deps (which inputs need eager materialization)
  6. Validate: circular deps, sync/async consistency

Also exports the global default factories:
  - memory_materializer_factory       (stream → collection)
  - log_error_materializer_factory (exception → log)

All functions are stateless — no classes, no self.
"""

import logging
import traceback
import types as _types
from collections.abc import MutableMapping, MutableSequence, MutableSet
from typing import Any, NamedTuple, get_args

from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_dependencies import initialize_parameters
from synaflow.core.dag_steps import (
    validate_and_compile_step,
    validate_step_is_callable,
    validate_sync_async_consistency,
    validate_unique_step_name,
)
from synaflow.core.dag_topology import check_circular_dependencies
from synaflow.core.observers import (
    Observer,
)
from synaflow.core.type_compatibility import (
    get_inner_type,
    is_iterable_type,
    is_materialized_consumer,
    is_scalar,
)
from synaflow.core.types import ErrorMaterializeContext, MaterializeContext, OnError


def _identity(x):
    return x


def memory_materializer_factory(ctx: MaterializeContext):
    tp = getattr(ctx.consumer_type, "__origin__", None) or ctx.consumer_type
    if tp is not None:
        for candidate in (
            (list, MutableSequence),
            (set, MutableSet),
            (dict, MutableMapping),
        ):
            try:
                if issubclass(tp, candidate):
                    return candidate[0]
            except TypeError:
                continue
        if tp is tuple:
            return tuple
    if tp is not None and is_scalar(tp):
        return _identity
    return list


memory_materializer_factory.__name__ = "memory_materializer"


def log_error_materializer_factory(ctx: ErrorMaterializeContext):
    log = logging.getLogger("synaflow")

    def handle_error(exc: BaseException) -> None:
        log.warning(
            "[%s] [%s] %s: %s",
            ctx.pipeline_name,
            ctx.dataset_name,
            type(exc).__name__,
            exc,
        )
        log.debug(traceback.format_exc())

    return handle_error


log_error_materializer_factory.__name__ = "log_error_materializer"


_BUILTIN_TYPES = {int, float, str, bool, bytes, type(None), list, set, tuple, dict}


def _is_builtin_type(tp: Any) -> bool:
    if type(tp) is _types.UnionType:
        return all(_is_builtin_type(a) for a in get_args(tp))

    origin = getattr(tp, "__origin__", None)
    if origin is not None:
        if origin in _BUILTIN_TYPES:
            return True
        for b in _BUILTIN_TYPES:
            try:
                if issubclass(origin, b):
                    return True
            except TypeError:
                pass
        return False
    if tp in _BUILTIN_TYPES:
        return True
    for b in _BUILTIN_TYPES:
        try:
            if issubclass(tp, b):
                return True
        except TypeError:
            pass
    return False


def _validate_params_is_namedtuple(params: Any, pipeline_name: str) -> None:
    if not hasattr(params, "_fields"):
        raise ValueError(
            f"Pipeline '{pipeline_name}': 'params' must be a NamedTuple, got {type(params).__name__}"
        )


import inspect


def _validate_materializer_factory(name: str, mat: Any, is_error: bool = False) -> None:
    if mat is None:
        return
    if not callable(mat):
        raise TypeError(
            f"Node '{name}': {'error materializer' if is_error else 'materializer'} must be a callable factory, got {type(mat).__name__}"
        )

    # Built-in collection types are commonly passed by mistake instead of factory
    if isinstance(mat, type) and mat in (list, set, dict, tuple):
        label = "error materializer" if is_error else "materializer"
        helper = "to_error_materializer" if is_error else "to_materializer"
        raise ValueError(
            f"Node '{name}': {label} cannot be a direct type/callable '{mat.__name__}'. "
            f"Please wrap it using {helper}({mat.__name__})."
        )

    # Let's inspect the signature to verify it accepts a context argument
    try:
        sig = inspect.signature(mat)
        has_params = len(sig.parameters) > 0
    except (ValueError, TypeError):
        has_params = False

    if not has_params:
        label = "error materializer" if is_error else "materializer"
        helper = "to_error_materializer" if is_error else "to_materializer"
        raise ValueError(
            f"Node '{name}': {label} factory must accept at least one argument (context). "
            f"If you want to use a direct callable, wrap it using {helper}(...)."
        )


def _resolve_materializers(
    dag: dict[str, DagNode],
    pipeline_materializer: Any,
    pipeline_error_materializer: Any,
) -> None:
    for name, node in dag.items():
        if not node.fn:
            node.materializer = None
            node.error_materializer = None
            continue

        mat = node.materializer or pipeline_materializer or memory_materializer_factory
        _validate_materializer_factory(name, mat, is_error=False)
        node.materializer = mat

        err_mat = (
            node.error_materializer
            or pipeline_error_materializer
            or log_error_materializer_factory
        )
        _validate_materializer_factory(name, err_mat, is_error=True)
        node.error_materializer = err_mat

        if node.output and is_iterable_type(node.output):
            inner = get_inner_type(node.output)
            if inner is not None and not _is_builtin_type(inner):
                raise ValueError(
                    f"Node '{name}': output item type '{inner}' requires a custom"
                    " materializer. Provide a step-level materializer or a"
                    " pipeline-level materializer."
                )


def _compute_materialized_deps(dag: dict[str, DagNode]) -> None:
    for node in dag.values():
        if node.fn is None:
            node.materialized_deps = []
            continue

        materialized_deps = []
        for dep_name, dep_type in node.deps.items():
            if is_materialized_consumer(dep_type):
                materialized_deps.append(dep_name)
            elif dep_name in dag and dag[dep_name].on_error == OnError.STOP:
                materialized_deps.append(dep_name)
        if node.force_materialize:
            for dep_name in node.deps:
                if dep_name not in materialized_deps:
                    materialized_deps.append(dep_name)
        node.materialized_deps = materialized_deps


def build_dag(
    pipeline_name: str,
    params: type[NamedTuple],
    steps: list[Any],
    memory_materializer_factory: Any = None,
    is_default_factory: bool = False,
    error_materializer_factory: Any = None,
    pipeline_observers: list[Observer] | None = None,
) -> Dag:
    if error_materializer_factory is None:
        error_materializer_factory = log_error_materializer_factory

    _validate_params_is_namedtuple(params, pipeline_name)

    factory = memory_materializer_factory

    dag: dict[str, DagNode] = {}
    dag_obj = Dag(name=pipeline_name)

    from synaflow.core.dag_expansion import expand_macros

    for step in steps:
        if hasattr(step, "name"):
            validate_unique_step_name(step.name, {}, pipeline_name)

    expanded_steps = expand_macros(steps, current_pipeline_name=pipeline_name)

    pipeline_obs = list(pipeline_observers) if pipeline_observers else []

    produced = initialize_parameters(params)

    for step in expanded_steps:
        validate_step_is_callable(step, pipeline_name)
        validate_unique_step_name(step.name, dag, pipeline_name, is_expanded=True)

        effective = list(pipeline_obs)
        step_own = getattr(step, "observers", None)
        if step_own:
            effective.extend(step_own)

        compiled_step = validate_and_compile_step(
            step, produced, pipeline_name, observers=effective
        )
        dag[step.name] = compiled_step
        produced[step.name] = compiled_step

    _resolve_materializers(dag, factory, error_materializer_factory)
    _compute_materialized_deps(dag)

    dag_obj.params = {
        name: info.output for name, info in produced.items() if name not in dag
    }
    dag_obj.steps = dag
    dag_obj.error_materializer_factory = error_materializer_factory
    dag_obj.pipeline_observers = list(pipeline_obs)

    check_circular_dependencies(dag_obj, pipeline_name)

    validate_sync_async_consistency(
        dag_obj,
        pipeline_name,
        steps,
        memory_materializer_factory,
        is_default_factory=is_default_factory,
    )

    return dag_obj
