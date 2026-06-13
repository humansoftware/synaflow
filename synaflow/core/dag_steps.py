import inspect
from typing import Any

from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_dependencies import (
    get_safe_type_hints,
    resolve_step_output_type,
    validate_and_resolve_dependencies,
)
from synaflow.core.definition import Step
from synaflow.core.type_compatibility import is_async_stream_type, is_sync_stream_type


def validate_step_is_callable(step: Step, pipeline_name: str) -> None:
    if not callable(step.fn):
        raise ValueError(
            f"Pipeline '{pipeline_name}': step '{step.name}' must have a callable 'fn', got {type(step.fn).__name__}"
        )


def validate_unique_step_name(
    step_name: str, dag: dict, pipeline_name: str, is_expanded: bool = False
) -> None:
    if step_name in dag:
        raise ValueError(
            f"Pipeline '{pipeline_name}': duplicate step name '{step_name}'"
        )
    if not is_expanded and "__" in step_name:
        raise ValueError(
            f"Pipeline '{pipeline_name}': step name '{step_name}' contains '__',"
            " which is reserved for sub-pipeline name scoping."
        )


def validate_and_compile_step(
    step: Step, produced: dict[str, DagNode], pipeline_name: str
) -> DagNode:
    sig = inspect.signature(step.fn)
    hints = get_safe_type_hints(step.fn)

    deps = validate_and_resolve_dependencies(step, sig, hints, produced, pipeline_name)
    output_type = resolve_step_output_type(sig, hints, deps, produced)

    from synaflow.core.type_compatibility import is_materialized_consumer

    needs_materialize = any(is_materialized_consumer(t) for t in deps.values())

    return DagNode(
        fn=step.fn,
        deps=deps,
        output=output_type,
        on_error=step.on_error,
        materializer=step.materializer,
        force_materialize=step.force_materialize,
        needs_materialize=needs_materialize,
        pipeline=step.pipeline or pipeline_name,
        parent_pipeline=getattr(step, "parent_pipeline", None),
    )


def validate_sync_async_consistency(
    dag: Dag,
    pipeline_name: str,
    steps: list[Step],
    default_materializer_factory: Any,
    is_default_factory: bool = False,
) -> None:
    has_sync = False
    has_async = False

    for node in dag.steps.values():
        if not node.fn:
            continue

        if inspect.iscoroutinefunction(node.fn):
            has_async = True

        if is_sync_stream_type(node.output):
            has_sync = True
        if is_async_stream_type(node.output):
            has_async = True

        for dep_type in node.deps.values():
            if is_sync_stream_type(dep_type):
                has_sync = True
            if is_async_stream_type(dep_type):
                has_async = True

    has_async_materializer = False
    has_sync_materializer = False

    def _is_async_mat(m: Any) -> bool:
        sig = inspect.signature(m)
        if (
            len(sig.parameters) > 1
            or "ctx" in sig.parameters
            or "context" in sig.parameters
        ):
            from synaflow.core.types import MaterializeContext

            ctx = MaterializeContext(
                pipeline_name=pipeline_name, dataset_name="validator", item_type=Any
            )
            m = m(ctx)
        return inspect.iscoroutinefunction(m)

    if default_materializer_factory and not is_default_factory:
        if _is_async_mat(default_materializer_factory):
            has_async_materializer = True
        else:
            has_sync_materializer = True

    for step in steps:
        if getattr(step, "materializer", None):
            if _is_async_mat(step.materializer):
                has_async_materializer = True
            else:
                has_sync_materializer = True

    if has_sync and has_async_materializer:
        raise ValueError(
            f"Pipeline '{pipeline_name}' is UNRUNNABLE. It contains synchronous streams "
            "but has an asynchronous materializer."
        )

    if has_async and has_sync_materializer:
        raise ValueError(
            f"Pipeline '{pipeline_name}' is UNRUNNABLE. It contains asynchronous streams "
            "but has a synchronous materializer."
        )

    if has_sync and has_async:
        raise ValueError(
            f"Pipeline '{pipeline_name}' is UNRUNNABLE. It contains synchronous streams (Iterator) "
            "and asynchronous features (async def or AsyncIterator). "
            "You must convert all streams to AsyncIterator to run it asynchronously, "
            "or remove async functions to run it synchronously."
        )

    dag.requires_sync_runner = has_sync
    dag.requires_async_runner = has_async
