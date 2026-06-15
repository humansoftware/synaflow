import inspect
from typing import Any

from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_dependencies import (
    get_safe_type_hints,
    resolve_step_output_type,
    validate_and_resolve_dependencies,
)
from synaflow.core.definition import Step
from synaflow.core.naming import get_base_dataset_name
from synaflow.core.type_compatibility import (
    is_async_stream_type,
    is_iterable_type,
    is_scalar,
    is_sync_stream_type,
)
from synaflow.core.types import MaterializeContext, StepMode


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
    step: Step,
    produced: dict[str, DagNode],
    pipeline_name: str,
    observers: list | None = None,
) -> DagNode:
    sig = inspect.signature(step.fn)
    hints = get_safe_type_hints(step.fn)

    deps, dataset_param_names = validate_and_resolve_dependencies(
        step, sig, hints, produced, pipeline_name
    )

    mode, each_mode_deps = resolve_step_mode(step, deps, produced, pipeline_name)
    output_type = resolve_step_output_type(sig, hints, deps, produced, mode)

    return DagNode(
        fn=step.fn,
        deps=deps,
        output=output_type,
        on_error=step.on_error,
        mode=mode,
        materializer=step.materializer,
        error_materializer=step.error_materializer,
        each_mode_deps=each_mode_deps,
        force_materialize=step.force_materialize,
        pipeline=step.pipeline or pipeline_name,
        parent_pipeline=step.parent_pipeline,
        observers=observers or [],
        dataset_param_names=dataset_param_names,
    )


def resolve_step_mode(
    step: Step,
    deps: dict[str, Any],
    produced: dict[str, DagNode],
    pipeline_name: str,
) -> tuple[StepMode, list[str]]:
    each_mode_deps = [
        dep_name
        for dep_name, dep_type in deps.items()
        if dep_name in produced
        and produced[dep_name].output is not None
        and is_iterable_type(produced[dep_name].output)
        and is_scalar(dep_type)
    ]

    requested_mode = step.mode
    if requested_mode == StepMode.AUTO:
        if each_mode_deps:
            return StepMode.EACH, each_mode_deps
        return StepMode.ALL, []

    if requested_mode == StepMode.EACH:
        if not each_mode_deps:
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' is forced to EACH mode "
                "but none of its dependencies can be consumed item-by-item."
            )
        return StepMode.EACH, each_mode_deps

    if requested_mode == StepMode.ALL:
        if each_mode_deps:
            deps_list = ", ".join(each_mode_deps)
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' is forced to ALL mode "
                f"but dependencies [{deps_list}] require EACH-mode consumption."
            )
        return StepMode.ALL, []

    raise ValueError(
        f"Pipeline '{pipeline_name}': step '{step.name}' has unsupported mode '{requested_mode}'"
    )


def validate_sync_async_consistency(
    dag: Dag,
    pipeline_name: str,
    steps: list[Step],
    memory_materializer_factory: Any,
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
            ctx = MaterializeContext(
                pipeline_name=pipeline_name, dataset_name="validator", item_type=Any
            )
            m = m(ctx)
        return inspect.iscoroutinefunction(m)

    if memory_materializer_factory and not is_default_factory:
        if _is_async_mat(memory_materializer_factory):
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


def validate_no_duplicate_base_datasets(
    steps: list,
    pipeline_name: str,
) -> None:
    seen: dict[str, str] = {}
    for s in steps:
        base = get_base_dataset_name(s.name)
        if base in seen and s.name != seen[base]:
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{s.name}' and step '{seen[base]}' "
                f"both map to Base Dataset '{base}'. Use distinct nouns for step names."
            )
        if base not in seen:
            seen[base] = s.name
