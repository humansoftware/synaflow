"""
DAG Builder: compiles a pipeline definition into a validated Dag.

build_dag() orchestrates the full compilation pipeline:
  1. Expand macros (flatten IncludeSteps into plain Steps)
  2. Initialize params from the NamedTuple
  3. Compile each step (validate types, resolve deps, infer output type)
  4. Resolve materializers (step → pipeline → global default)
  5. Compute producer-level materialization
  6. Validate: circular deps, sync/async consistency

Also exports the global default factories:
  - memory_materializer_factory       (stream → collection)
  - log_error_materializer_factory (exception → log)

Design note:
  The builder is responsible for the full materialization plan. Runtime must
  only ask whether a producer output is materialized globally. Any per-consumer
  materialization details that remain in the Dag are private diagnostics.

All functions are stateless — no classes, no self.
"""

import logging
import traceback
import types as _types
import dataclasses
from dataclasses import dataclass
from collections.abc import (
    AsyncIterable as AbcAsyncIterable,
    AsyncIterator as AbcAsyncIterator,
    AsyncGenerator,
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

from synaflow.core.adapters import async_adapter
from synaflow.core.dag import (
    ConsumerContract,
    Dag,
    DagNode,
    OutputContract,
    PublishPlan,
)
from synaflow.core.dag_dependencies import initialize_parameters, initialize_resources
from synaflow.core.definition import (
    IncludeStep,
    PipelineDef,
    Step,
    _validate_no_async_handlers,
    _validate_no_sync_handlers,
)
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
from synaflow.core.types import ErrorMaterializeContext, MaterializeContext, StepMode


def _identity(x):
    return x


def memory_materializer_factory(ctx: MaterializeContext):
    tp = getattr(ctx.consumer_type, "__origin__", None) or ctx.consumer_type

    constructor = None
    if tp is not None:
        for candidate in (
            (list, MutableSequence),
            (set, MutableSet),
            (dict, MutableMapping),
        ):
            try:
                if issubclass(tp, candidate):
                    constructor = candidate[0]
                    break
            except TypeError:
                continue
        if constructor is None:
            if tp is tuple:
                constructor = tuple
            elif is_scalar(tp):
                constructor = _identity
            elif tp in (
                AsyncIterator,
                Iterator,
                Iterable,
                AsyncIterable,
                AbcAsyncIterator,
                AbcIterator,
                AbcIterable,
                AbcAsyncIterable,
            ):
                constructor = list

    if constructor is None:
        raise ValueError(
            f"Cannot infer memory materializer for consumer type: '{tp}'. "
            "Please provide explicit type hints for your consumer parameters, or use a step-level materializer."
        )

    if ctx.is_async_pipeline:

        async def async_collection(stream: Any) -> Any:
            if isinstance(stream, (AsyncIterator, AbcAsyncIterator, AsyncGenerator)):
                items = [x async for x in stream]
            else:
                items = list(stream)
            return constructor(items)

        return async_collection

    return constructor


memory_materializer_factory.__name__ = "memory_materializer"


def log_error_materializer_factory(ctx: ErrorMaterializeContext):
    log = logging.getLogger("synaflow")

    def log_error(error_ctx) -> None:
        log.warning(
            "[%s] [%s] [%s] [%s] %s: %s",
            error_ctx.pipeline_name,
            error_ctx.dataset_name,
            error_ctx.step_name,
            error_ctx.run_id,
            type(error_ctx.exception).__name__,
            error_ctx.exception,
        )
        log.debug(traceback.format_exc())

    if ctx.is_async_pipeline:
        return async_adapter(log_error)

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


def _is_stream_output(tp: Any) -> bool:
    return tp is not None and (is_sync_stream_type(tp) or is_async_stream_type(tp))


@dataclass(frozen=True)
class _DagBuildIndexes:
    consumers_by_producer: dict[str, list[str]]
    stream_nodes: set[str]


def _build_dag_indexes(dag: dict[str, DagNode]) -> _DagBuildIndexes:
    consumers_by_producer = {name: [] for name in dag}
    stream_nodes = {
        name for name, node in dag.items() if _is_stream_output(node.output)
    }

    for consumer_name, node in dag.items():
        for dep_name in node.deps:
            if dep_name in consumers_by_producer:
                consumers_by_producer[dep_name].append(consumer_name)

    return _DagBuildIndexes(
        consumers_by_producer=consumers_by_producer,
        stream_nodes=stream_nodes,
    )


def _validate_params_type(params: Any, pipeline_name: str) -> None:
    if not (hasattr(params, "_fields") or dataclasses.is_dataclass(params)):
        raise ValueError(
            f"Pipeline '{pipeline_name}': 'params' must be a NamedTuple or dataclass, got {type(params).__name__}"
        )


def _validate_declared_step_names(
    steps: list[Step | IncludeStep], pipeline_name: str
) -> None:
    for step in steps:
        if hasattr(step, "name"):
            validate_unique_step_name(step.name, {}, pipeline_name)


def _validate_resource_names(
    resources: dict[str, Any],
    params: type[NamedTuple],
    expanded_steps: list[Step],
    pipeline_name: str,
) -> None:
    if dataclasses.is_dataclass(params):
        param_fields = {f.name for f in dataclasses.fields(params)}
    else:
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


def validate_no_unused_resources(
    dag: dict[str, DagNode],
    effective_resources: dict[str, Any],
    pipeline_name: str,
) -> None:
    """Raise ValueError for each declared resource that no step uses."""
    used: set[str] = set()
    for node in dag.values():
        used.update(node.deps)

    for name in effective_resources:
        if name not in used:
            raise ValueError(
                f"Pipeline '{pipeline_name}': resource '{name}' "
                "is declared in resources={} but not used by any step."
            )


def _collect_pipeline_resources(
    pipeline_name: str,
    steps: list[Step | IncludeStep],
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
    indexes: _DagBuildIndexes,
    pipeline_materializer: Any,
    pipeline_error_materializer: Any,
) -> None:
    def resolve_materializer_consumer_type(step_name: str) -> Any:
        consumers = [
            dag.steps[consumer_name]
            for consumer_name in indexes.consumers_by_producer.get(step_name, [])
        ]
        if not consumers:
            return None

        mat_consumers = [
            consumer
            for consumer in consumers
            if is_materialized_consumer(consumer.deps.get(step_name))
        ]
        if not mat_consumers and dag.needs_materialize(step_name):
            mat_consumers = consumers
        if not mat_consumers:
            return consumers[0].deps.get(step_name)

        consumer_type = mat_consumers[0].deps.get(step_name)
        from synaflow.core.type_compatibility import is_type_compatible

        for other in mat_consumers[1:]:
            other_tp = other.deps.get(step_name)
            if (
                consumer_type != other_tp
                and not is_type_compatible(consumer_type, other_tp)
                and not is_type_compatible(other_tp, consumer_type)
            ):
                raise ValueError(
                    f"Pipeline '{dag.name}': step '{step_name}' has consumers with incompatible types: "
                    f"'{mat_consumers[0].name}' expects {consumer_type} but '{other.name}' expects {other_tp}."
                )

        return consumer_type

    for name, node in dag.steps.items():
        if not node.fn:
            node.materializer = None
            node.error_materializer = None
            continue

        has_explicit_mat = (
            node.materializer is not None or pipeline_materializer is not None
        )
        is_stream = name in indexes.stream_nodes
        is_untyped = node.output is None
        is_scalar = not is_untyped and not is_iterable_type(node.output)
        has_consumers = bool(indexes.consumers_by_producer.get(name))

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
            ctx = MaterializeContext(
                pipeline_name=dag.name,
                dataset_name=name,
                item_type=node.output,
                consumer_type=resolve_materializer_consumer_type(name),
                is_async_pipeline=dag.requires_async_runner,
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
                is_async_pipeline=dag.requires_async_runner,
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


def _plan_materialization(dag: dict[str, DagNode], indexes: _DagBuildIndexes) -> None:
    """Compile producer-level materialization plan once, in the builder.
    See ``docs/MATERIALIZATION_RUNTIME_CONTRACT.md`` for the full model."""
    producer_needs_materialize = {name: False for name in dag}
    producer_reasons = {name: set() for name in dag}

    def mark_materialize(producer_name: str, reason: str) -> bool:
        producer_reasons[producer_name].add(reason)
        if producer_needs_materialize[producer_name]:
            return False
        producer_needs_materialize[producer_name] = True
        return True

    for producer_name, node in dag.items():
        if node.force_materialize:
            mark_materialize(producer_name, "producer_force_materialize")

    changed = True
    while changed:
        changed = False

        for consumer_name, node in dag.items():
            if node.fn is None:
                continue

            for dep_name, dep_type in node.deps.items():
                if dep_name not in dag:
                    continue
                if is_materialized_consumer(dep_type):
                    changed = (
                        mark_materialize(
                            dep_name, "consumer_requires_materialized_type"
                        )
                        or changed
                    )

            if node.force_materialize:
                for dep_name in node.deps:
                    if dep_name in dag:
                        changed = (
                            mark_materialize(dep_name, "consumer_force_materialize")
                            or changed
                        )

    for producer_name, node in dag.items():
        node.materialize_output = producer_needs_materialize[producer_name]
        node._materialize_reasons = sorted(producer_reasons[producer_name])

    from synaflow.core.lockstep_validation import validate_lockstep_symmetry

    # Extract pipeline name from the first node that has it
    pipeline_name = "unknown"
    for n in dag.values():
        if getattr(n, "pipeline", None):
            pipeline_name = n.pipeline
            break

    validate_lockstep_symmetry(dag, pipeline_name)

    for consumer_name, node in dag.items():
        if node.fn is None:
            node._materialized_deps = []
            continue
        node._materialized_deps = [
            dep_name
            for dep_name in node.deps
            if dep_name in dag and producer_needs_materialize[dep_name]
        ]


def _classify_output_runtime_kind(dag: Dag, node: DagNode) -> str:
    if node.mode == StepMode.EACH:
        if dag.requires_async_runner:
            return "async_stream"
        return "sync_stream"
    if is_async_stream_type(node.output):
        return "async_stream"
    if is_sync_stream_type(node.output):
        return "sync_stream"
    return "value"


def _classify_consumer_contract(
    producer_name: str,
    consumer_name: str,
    consumer: DagNode,
) -> ConsumerContract:
    dep_type = consumer.deps.get(producer_name)
    if consumer.mode == StepMode.EACH:
        consumption = "item"
    elif is_materialized_consumer(dep_type):
        consumption = "materialized"
    elif is_iterable_type(dep_type):
        consumption = "stream"
    else:
        consumption = "barrier_only"
    return ConsumerContract(consumer_name=consumer_name, consumption=consumption)


def _compile_execution_plan(dag: Dag, indexes: _DagBuildIndexes) -> None:
    for producer_name, node in dag.steps.items():
        consumers = indexes.consumers_by_producer.get(producer_name, [])
        consumer_contracts = [
            _classify_consumer_contract(
                producer_name, consumer_name, dag[consumer_name]
            )
            for consumer_name in consumers
        ]
        node.consumer_contracts = consumer_contracts

        runtime_kind = _classify_output_runtime_kind(dag, node)
        completion_policy = "on_exhaustion" if runtime_kind != "value" else "immediate"

        if runtime_kind == "value":
            drain_policy = "none"
        elif dag.is_terminal_step(producer_name):
            drain_policy = "terminal"
        elif consumer_contracts and all(
            contract.consumption == "barrier_only" for contract in consumer_contracts
        ):
            drain_policy = "barrier_only"
        else:
            drain_policy = "none"

        node.output_contract = OutputContract(
            runtime_kind=runtime_kind,
            completion_policy=completion_policy,
            drain_policy=drain_policy,
        )

        if runtime_kind != "value" and drain_policy != "none":
            strategy = "publish_value"
            handoff = "none"
        elif runtime_kind == "value":
            strategy = "publish_value"
            handoff = "none"
        elif node.materialize_output:
            strategy = "publish_materialized"
            handoff = "none"
        elif len(consumers) > 1:
            strategy = (
                "publish_async_fanout"
                if runtime_kind == "async_stream"
                else "publish_sync_fanout"
            )
            handoff = "async_queue" if runtime_kind == "async_stream" else "sync_fanout"
        else:
            strategy = "publish_stream"
            handoff = (
                "bounded_iterator"
                if runtime_kind == "sync_stream" and node.max_in_flight > 1
                else "none"
            )

        node.publish_plan = PublishPlan(
            strategy=strategy,
            handoff=handoff,
            max_in_flight=node.max_in_flight,
        )


def _expand_and_validate_steps(
    steps: list[Step | IncludeStep],
    pipeline_name: str,
) -> list[Step]:
    _validate_declared_step_names(steps, pipeline_name)
    expanded_steps = expand_macros(steps, current_pipeline_name=pipeline_name)
    validate_no_duplicate_base_datasets(expanded_steps, pipeline_name)
    return expanded_steps


def _assert_dag_invariants(dag: "Dag", pipeline_name: str) -> None:
    """Loud invariant: every compiled DagNode carries a non-empty
    ``pipeline`` attribute. RuntimeError on violation — absence is an
    internal framework bug."""
    for step_name, node in dag.steps.items():
        if not getattr(node, "pipeline", None):
            raise RuntimeError(
                f"build_dag invariant violation: pipeline "
                f"{pipeline_name!r} compiled step {step_name!r} with "
                "empty pipeline attribute. DagNode.pipeline is required "
                "to be a non-empty string after compilation."
            )


def _compile_steps(
    expanded_steps: list[Step],
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
            step_index_in_scope=step.index_in_scope,
            step_total_in_scope=step.total_in_scope,
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
    dag_obj.steps = dag
    dag_obj.error_materializer_factory = error_materializer_factory
    dag_obj.pipeline_observers = list(pipeline_observers)
    return dag_obj


def build_dag(pipeline_def: PipelineDef) -> Dag:
    """Compile ``pipeline_def`` into a ``Dag`` — single source of
    design-time validation."""
    error_materializer = (
        pipeline_def.error_materializer or log_error_materializer_factory
    )

    _validate_params_type(pipeline_def.params, pipeline_def.name)
    pipeline_obs_resolved = _resolve_pipeline_observers(pipeline_def.observers or [])
    expanded_steps = _expand_and_validate_steps(pipeline_def.steps, pipeline_def.name)
    effective_resources = _collect_pipeline_resources(
        pipeline_def.name,
        pipeline_def.steps,
        pipeline_def.resources or {},
        include_chain=(pipeline_def.name,),
    )
    _validate_resource_names(
        effective_resources,
        pipeline_def.params,
        expanded_steps,
        pipeline_def.name,
    )
    dag, produced = _compile_steps(
        expanded_steps,
        pipeline_def.name,
        pipeline_def.params,
        effective_resources,
        pipeline_obs_resolved,
    )
    validate_no_unused_resources(dag, effective_resources, pipeline_def.name)
    indexes = _build_dag_indexes(dag)
    _plan_materialization(dag, indexes)
    dag_obj = _finalize_dag(
        pipeline_def.name,
        dag,
        produced,
        set(effective_resources),
        error_materializer,
        pipeline_obs_resolved,
    )
    dag_obj.resource_factories = effective_resources
    check_circular_dependencies(dag_obj, pipeline_def.name)
    _assert_dag_invariants(dag_obj, pipeline_def.name)

    validate_no_unmaterialized_terminal_streams(
        dag_obj, pipeline_def.name, pipeline_def.exports
    )

    validate_sync_async_consistency(dag_obj, pipeline_def.name)

    _compile_execution_plan(dag_obj, indexes)

    _resolve_materializers(
        dag_obj,
        indexes,
        pipeline_def.materializer,
        error_materializer,
    )

    if dag_obj.requires_sync_runner or not dag_obj.requires_async_runner:
        _validate_no_async_handlers(pipeline_def, dag_obj)
    else:
        _validate_no_sync_handlers(pipeline_def, dag_obj)

    return dag_obj
