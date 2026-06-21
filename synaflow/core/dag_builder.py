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
from collections.abc import (
    AsyncIterable as AbcAsyncIterable,
    AsyncIterator as AbcAsyncIterator,
    Iterable as AbcIterable,
    Iterator as AbcIterator,
    MutableMapping,
    MutableSequence,
    MutableSet,
)
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Iterator,
    NamedTuple,
    get_args,
)

from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_dependencies import initialize_parameters, initialize_resources
from synaflow.core.definition import IncludeStep
from synaflow.core.dag_expansion import expand_macros
from synaflow.core.dag_steps import (
    validate_and_compile_step,
    validate_no_duplicate_base_datasets,
    validate_no_unmaterialized_terminal_streams,
    validate_step_is_callable,
    validate_sync_async_consistency,
    validate_unique_step_name,
)
from synaflow.core.dag_topology import check_circular_dependencies
from synaflow.core.observers import (
    Observer,
    ResolvedObserver,
)
from synaflow.core.type_compatibility import (
    get_inner_type,
    is_async_stream_type,
    is_factory,
    is_iterable_type,
    is_materialized_consumer,
    is_scalar,
    is_sync_stream_type,
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
        if is_scalar(tp):
            return _identity
        if tp in (
            AsyncIterator,
            Iterator,
            Iterable,
            AsyncIterable,
            AbcAsyncIterator,
            AbcIterator,
            AbcIterable,
            AbcAsyncIterable,
        ):
            return list

    raise ValueError(
        f"Cannot infer memory materializer for consumer type: '{tp}'. "
        "Please provide explicit type hints for your consumer parameters, or use a step-level materializer."
    )


memory_materializer_factory.__name__ = "memory_materializer"


def log_error_materializer_factory(ctx: ErrorMaterializeContext):
    log = logging.getLogger("synaflow")

    def log_error(exc: BaseException) -> None:
        log.warning(
            "[%s] [%s] %s: %s",
            ctx.pipeline_name,
            ctx.dataset_name,
            type(exc).__name__,
            exc,
        )
        log.debug(traceback.format_exc())

    return log_error


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


def _validate_declared_step_names(steps: list[Any], pipeline_name: str) -> None:
    for step in steps:
        if hasattr(step, "name"):
            validate_unique_step_name(step.name, {}, pipeline_name)


def _validate_resource_names(
    resources: dict[str, Any],
    params: type[NamedTuple],
    expanded_steps: list[Any],
    pipeline_name: str,
) -> None:
    param_fields = set(getattr(params, "_fields", []))
    step_names = {step.name for step in expanded_steps}

    for resource_name in resources:
        if resource_name in param_fields:
            raise ValueError(
                f"Pipeline '{pipeline_name}': resource '{resource_name}' collides with a params field."
            )
        if resource_name in step_names:
            raise ValueError(
                f"Pipeline '{pipeline_name}': resource '{resource_name}' collides with a step name."
            )


def _merge_resources(
    merged: dict[str, Any],
    incoming: dict[str, Any],
    pipeline_name: str,
) -> None:
    for resource_name, resource in incoming.items():
        if resource_name in merged and merged[resource_name] is not resource:
            raise ValueError(
                f"Pipeline '{pipeline_name}': resource '{resource_name}' is declared multiple times with different instances/factories."
            )
        merged.setdefault(resource_name, resource)


def _collect_pipeline_resources(
    pipeline_name: str,
    steps: list[Any],
    resources: dict[str, Any],
    include_chain: tuple[str, ...] = (),
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    _merge_resources(merged, resources, pipeline_name)

    for step in steps:
        if not isinstance(step, IncludeStep):
            continue

        sub_pipeline = step.pipeline
        if sub_pipeline.name in include_chain:
            raise ValueError(
                f"Infinite cycle detected: Pipeline '{sub_pipeline.name}' is already in the inclusion chain '{'.'.join(include_chain)}'"
            )

        sub_resources = _collect_pipeline_resources(
            pipeline_name,
            sub_pipeline.steps,
            sub_pipeline.resources,
            (*include_chain, sub_pipeline.name),
        )
        _merge_resources(merged, sub_resources, pipeline_name)

    return merged


def _resolve_pipeline_observers(
    pipeline_observers: list[Observer],
) -> list[ResolvedObserver]:
    return [
        ResolvedObserver(handler=observer.handler, source="pipeline")
        for observer in pipeline_observers
    ]


def _resolve_step_observers(
    pipeline_observers: list[ResolvedObserver],
    step_observers: list[Observer | ResolvedObserver],
) -> list[ResolvedObserver]:
    resolved = list(pipeline_observers)
    for observer in step_observers:
        if isinstance(observer, ResolvedObserver):
            resolved.append(observer)
            continue
        resolved.append(ResolvedObserver(handler=observer.handler, source="step"))
    return resolved


def _resolve_materializers(
    dag: Dag,
    pipeline_materializer: Any,
    pipeline_error_materializer: Any,
) -> None:
    for name, node in dag.steps.items():
        if not node.fn:
            node.materializer = None
            node.error_materializer = None
            continue

        has_explicit_mat = (
            node.materializer is not None or pipeline_materializer is not None
        )
        is_stream = is_sync_stream_type(node.output) or is_async_stream_type(
            node.output
        )
        is_untyped = node.output is None
        is_scalar = not is_untyped and not is_iterable_type(node.output)
        has_consumers = bool(dag.consumers_of(name))

        mat = None
        if has_explicit_mat:
            mat = node.materializer or pipeline_materializer
        else:
            if is_scalar:
                mat = None
            elif is_stream:
                if has_consumers:
                    mat = memory_materializer_factory
                else:
                    mat = None
            elif is_untyped:
                if has_consumers:
                    mat = memory_materializer_factory
                else:
                    mat = None

        if mat and is_factory(mat):
            consumers = []
            for consumer_node in dag.steps.values():
                if name in consumer_node.deps:
                    consumers.append(consumer_node)

            consumer_type = None
            if consumers:
                mat_consumers = [
                    c for c in consumers if name in getattr(c, "materialized_deps", [])
                ]
                if mat_consumers:
                    consumer_type = mat_consumers[0].deps.get(name)
                    from synaflow.core.type_compatibility import is_type_compatible

                    for other in mat_consumers[1:]:
                        other_tp = other.deps.get(name)
                        if (
                            consumer_type != other_tp
                            and not is_type_compatible(consumer_type, other_tp)
                            and not is_type_compatible(other_tp, consumer_type)
                        ):
                            raise ValueError(
                                f"Pipeline '{dag.name}': step '{name}' has consumers with incompatible types: "
                                f"'{mat_consumers[0].name}' expects {consumer_type} but '{other.name}' expects {other_tp}."
                            )
                else:
                    consumer_type = consumers[0].deps.get(name)
            ctx = MaterializeContext(
                pipeline_name=dag.name,
                dataset_name=name,
                item_type=node.output,
                consumer_type=consumer_type,
            )
            node.materializer = mat(ctx)
        else:
            node.materializer = mat
        err_mat = (
            node.error_materializer
            or pipeline_error_materializer
            or log_error_materializer_factory
        )
        if err_mat and is_factory(err_mat):
            err_ctx = ErrorMaterializeContext(
                pipeline_name=dag.name,
                dataset_name=name,
            )
            node.error_materializer = err_mat(err_ctx)
        else:
            node.error_materializer = err_mat

        if (
            node.output
            and is_iterable_type(node.output)
            and node.materializer is memory_materializer_factory
        ):
            if dag.needs_materialize(name):
                inner = get_inner_type(node.output)
                if inner is not None and not _is_builtin_type(inner):
                    raise ValueError(
                        f"Node '{name}': output item type '{inner}' requires a custom"
                        " materializer. Provide a step-level materializer or a"
                        " pipeline-level materializer."
                    )


def _detect_and_materialize_merging_fanout_edges(dag: dict[str, DagNode]) -> None:
    adjacency = {name: [] for name in dag}
    for consumer_name, node in dag.items():
        for dep_name in node.deps:
            if dep_name in adjacency:
                adjacency[dep_name].append(consumer_name)

    def is_stream_node(node_name: str) -> bool:
        output = dag[node_name].output
        return output is not None and (
            is_sync_stream_type(output) or is_async_stream_type(output)
        )

    def reachable_targets(start: str) -> set[str]:
        seen = set()
        stack = [start]
        reachable = set()

        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            reachable.add(current)
            if is_stream_node(current):
                stack.extend(adjacency[current])

        return reachable

    for producer_name, node in dag.items():
        if node.output is None or not (
            is_sync_stream_type(node.output) or is_async_stream_type(node.output)
        ):
            continue

        direct_consumers = adjacency[producer_name]
        if len(direct_consumers) < 2:
            continue

        branch_targets = {
            consumer_name: reachable_targets(consumer_name)
            for consumer_name in direct_consumers
        }
        target_branch_counts: dict[str, int] = {}
        for targets in branch_targets.values():
            for target in targets:
                target_branch_counts[target] = target_branch_counts.get(target, 0) + 1
        shared_targets = {
            target for target, branch_count in target_branch_counts.items() if branch_count >= 2
        }

        for consumer_name, targets in branch_targets.items():
            if not (targets & shared_targets):
                continue
            consumer_node = dag[consumer_name]
            if producer_name not in consumer_node.materialized_deps:
                consumer_node.materialized_deps.append(producer_name)


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

        lazy_stream_deps = []
        for dep_name in node.deps:
            if dep_name in materialized_deps:
                continue
            producer_node = dag.get(dep_name)
            if producer_node is None or producer_node.output is None:
                continue
            if is_sync_stream_type(producer_node.output) or is_async_stream_type(
                producer_node.output
            ):
                lazy_stream_deps.append(dep_name)

        if len(lazy_stream_deps) >= 2:
            for dep_name in lazy_stream_deps:
                if dep_name not in materialized_deps:
                    materialized_deps.append(dep_name)
        node.materialized_deps = materialized_deps


def _expand_and_validate_steps(
    steps: list[Any],
    pipeline_name: str,
) -> list[Any]:
    _validate_declared_step_names(steps, pipeline_name)
    expanded_steps = expand_macros(steps, current_pipeline_name=pipeline_name)
    validate_no_duplicate_base_datasets(expanded_steps, pipeline_name)
    return expanded_steps


def _compile_steps(
    expanded_steps: list[Any],
    pipeline_name: str,
    params: type[NamedTuple],
    resources: dict[str, Any],
    pipeline_observers: list[ResolvedObserver],
) -> tuple[dict[str, DagNode], dict[str, DagNode]]:
    dag: dict[str, DagNode] = {}
    produced = initialize_parameters(params)
    produced.update(initialize_resources(resources))
    resource_nodes = initialize_resources(resources)

    for step in expanded_steps:
        validate_step_is_callable(step, pipeline_name)
        validate_unique_step_name(step.name, dag, pipeline_name, is_expanded=True)

        compiled_step = validate_and_compile_step(
            step,
            produced,
            resource_nodes,
            pipeline_name,
            observers=_resolve_step_observers(pipeline_observers, step.observers),
        )
        dag[step.name] = compiled_step
        produced[step.name] = compiled_step

    return dag, produced


def _finalize_dag(
    pipeline_name: str,
    dag: dict[str, DagNode],
    produced: dict[str, DagNode],
    resource_names: set[str],
    error_materializer_factory: Any,
    pipeline_observers: list[ResolvedObserver],
) -> Dag:
    dag_obj = Dag(name=pipeline_name)
    dag_obj.params = {
        name: info.output
        for name, info in produced.items()
        if name not in dag and name not in resource_names
    }
    dag_obj.resources = {
        name: info.output for name, info in produced.items() if name in resource_names
    }
    dag_obj.steps = dag
    dag_obj.error_materializer_factory = error_materializer_factory
    dag_obj.pipeline_observers = list(pipeline_observers)
    return dag_obj


def build_dag(
    pipeline_name: str,
    params: type[NamedTuple],
    steps: list[Any],
    resources: dict[str, Any] | None = None,
    memory_materializer_factory: Any = None,
    is_default_factory: bool = False,
    error_materializer_factory: Any = None,
    pipeline_observers: list[Observer] | None = None,
    exports: str | None = None,
) -> Dag:
    if error_materializer_factory is None:
        error_materializer_factory = log_error_materializer_factory

    _validate_params_is_namedtuple(params, pipeline_name)
    pipeline_obs_resolved = _resolve_pipeline_observers(pipeline_observers or [])
    expanded_steps = _expand_and_validate_steps(steps, pipeline_name)
    effective_resources = _collect_pipeline_resources(
        pipeline_name,
        steps,
        resources or {},
        include_chain=(pipeline_name,),
    )
    _validate_resource_names(effective_resources, params, expanded_steps, pipeline_name)
    dag, produced = _compile_steps(
        expanded_steps,
        pipeline_name,
        params,
        effective_resources,
        pipeline_obs_resolved,
    )
    _compute_materialized_deps(dag)
    _detect_and_materialize_merging_fanout_edges(dag)
    dag_obj = _finalize_dag(
        pipeline_name,
        dag,
        produced,
        set(effective_resources),
        error_materializer_factory,
        pipeline_obs_resolved,
    )
    _resolve_materializers(
        dag_obj,
        memory_materializer_factory,
        error_materializer_factory,
    )

    check_circular_dependencies(dag_obj, pipeline_name)

    validate_no_unmaterialized_terminal_streams(dag_obj, pipeline_name, exports)

    validate_sync_async_consistency(
        dag_obj,
        pipeline_name,
        steps,
        memory_materializer_factory,
        is_default_factory=is_default_factory,
    )

    return dag_obj
