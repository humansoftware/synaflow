import functools
import inspect
from typing import Any

from synaflow.core.definition import IncludeStep, Step
from synaflow.core.observers import ResolvedObserver
from synaflow.core.types import OnError


def expand_macros(
    steps: list[Any],
    current_pipeline_name: str | None = None,
    parent_chain: str | None = None,
) -> list[Step]:
    expanded = []
    for step in steps:
        if isinstance(step, IncludeStep):
            expanded.extend(_expand_include(step, current_pipeline_name, parent_chain))
        else:
            expanded.append(step)
    return expanded


def _resolve_include_observers(
    pipeline_observers: list,
    step_observers: list,
) -> list[ResolvedObserver]:
    return [
        ResolvedObserver(handler=obs.handler, source="pipeline")
        for obs in pipeline_observers
    ] + [ResolvedObserver(handler=obs.handler, source="step") for obs in step_observers]


def _build_parent_chain(
    current_pipeline_name: str | None,
    parent_chain: str | None,
) -> str | None:
    if not current_pipeline_name:
        return parent_chain
    if not parent_chain:
        return current_pipeline_name
    return f"{parent_chain}.{current_pipeline_name}"


def _validate_include_step(
    include_step: IncludeStep,
    new_parent_chain: str | None,
) -> None:
    sub_pipeline = include_step.pipeline
    current_chain_parts = new_parent_chain.split(".") if new_parent_chain else []
    if sub_pipeline.name in current_chain_parts:
        raise ValueError(
            f"Infinite cycle detected: Pipeline '{sub_pipeline.name}' is already in the inclusion chain '{new_parent_chain}'"
        )

    if not sub_pipeline.exports:
        raise ValueError(
            f"Pipeline '{sub_pipeline.name}' does not define 'exports', so it cannot be included."
        )

    sig = inspect.signature(include_step.fn)
    if sig.return_annotation is inspect.Parameter.empty:
        raise ValueError(
            f"Include step '{include_step.name}' must have a return type hint matching '{sub_pipeline.params.__name__}' or an Iterable of it."
        )

    annotation_str = str(sig.return_annotation)
    if sub_pipeline.params.__name__ not in annotation_str:
        raise ValueError(
            f"Include step '{include_step.name}' must return '{sub_pipeline.params.__name__}' or an Iterable of it. Got '{annotation_str}'"
        )


def _build_adapter_step(
    include_step: IncludeStep,
    current_pipeline_name: str | None,
    parent_chain: str | None,
) -> Step:
    return Step(
        name=f"{include_step.name}__adapter",
        fn=include_step.fn,
        on_error=OnError.STOP,
        description=include_step.description,
        pipeline=current_pipeline_name,
        parent_pipeline=parent_chain,
    )


def _extract_sub_pipeline_param_fields(params: Any) -> list[str]:
    if hasattr(params, "_fields"):
        return list(params._fields)
    return []


def _build_expanded_step_name(prefix: str, sub_step: Step, exported_name: str) -> str:
    if sub_step.name == exported_name:
        return prefix
    return f"{prefix}__{sub_step.name}"


def _resolve_sub_step_overrides(
    sub_pipeline: Any, sub_step: Step
) -> tuple[Any, Any, list]:
    materializer = sub_step.materializer or sub_pipeline.materializer
    error_materializer = sub_step.error_materializer or sub_pipeline.error_materializer
    observers = _resolve_include_observers(
        sub_pipeline.observers,
        sub_step.observers,
    )
    return materializer, error_materializer, observers


def _expand_sub_pipeline_steps(
    include_step: IncludeStep,
    adapter_name: str,
    sub_pipeline_param_fields: list[str],
    new_parent_chain: str | None,
) -> list[Step]:
    prefix = include_step.name
    sub_pipeline = include_step.pipeline
    expanded_steps: list[Step] = []
    sub_steps = expand_macros(
        sub_pipeline.steps,
        current_pipeline_name=sub_pipeline.name,
        parent_chain=new_parent_chain,
    )

    for sub_step in sub_steps:
        wrapped_fn = _wrap_sub_step_fn(
            sub_step.fn,
            prefix,
            adapter_name,
            sub_pipeline_param_fields,
            sub_pipeline.params,
        )
        materializer, error_materializer, observers = _resolve_sub_step_overrides(
            sub_pipeline,
            sub_step,
        )
        expanded_steps.append(
            Step(
                name=_build_expanded_step_name(prefix, sub_step, sub_pipeline.exports),
                fn=wrapped_fn,
                on_error=sub_step.on_error,
                mode=sub_step.mode,
                params=sub_step.params,
                materializer=materializer,
                error_materializer=error_materializer,
                description=sub_step.description,
                pipeline=sub_pipeline.name,
                parent_pipeline=new_parent_chain,
                observers=observers,
            )
        )

    return expanded_steps


def _expand_include(
    include_step: IncludeStep,
    current_pipeline_name: str | None = None,
    parent_chain: str | None = None,
) -> list[Step]:
    prefix = include_step.name
    sub_pipeline = include_step.pipeline
    adapter_name = f"{prefix}__adapter"
    new_parent_chain = _build_parent_chain(current_pipeline_name, parent_chain)
    _validate_include_step(include_step, new_parent_chain)
    adapter_step = _build_adapter_step(
        include_step, current_pipeline_name, parent_chain
    )
    sub_pipeline_param_fields = _extract_sub_pipeline_param_fields(sub_pipeline.params)
    expanded_steps = _expand_sub_pipeline_steps(
        include_step,
        adapter_name,
        sub_pipeline_param_fields,
        new_parent_chain,
    )
    return [adapter_step, *expanded_steps]


def _build_argument_mapping(
    signature: inspect.Signature,
    prefix: str,
    adapter_name: str,
    sub_pipeline_param_fields: list[str],
) -> dict[str, str]:
    arg_mapping: dict[str, str] = {}
    for param_name in signature.parameters:
        if param_name in sub_pipeline_param_fields:
            arg_mapping[param_name] = adapter_name
        else:
            arg_mapping[param_name] = f"{prefix}__{param_name}"
    return arg_mapping


def _remap_kwargs(
    kwargs: dict[str, Any],
    signature: inspect.Signature,
    argument_mapping: dict[str, str],
    sub_pipeline_param_fields: list[str],
) -> dict[str, Any]:
    remapped_kwargs: dict[str, Any] = {}
    for param_name in signature.parameters:
        source_name = argument_mapping[param_name]
        if param_name in sub_pipeline_param_fields:
            params_obj = kwargs[source_name]
            remapped_kwargs[param_name] = getattr(params_obj, param_name)
            continue
        remapped_kwargs[param_name] = kwargs[source_name]
    return remapped_kwargs


def _build_wrapper_signature(
    signature: inspect.Signature,
    argument_mapping: dict[str, str],
    sub_pipeline_param_fields: list[str],
    sub_pipeline_params_class: Any,
) -> inspect.Signature:
    parameters = []
    for param_name, param in signature.parameters.items():
        annotation = (
            sub_pipeline_params_class
            if param_name in sub_pipeline_param_fields
            else param.annotation
        )
        parameters.append(
            inspect.Parameter(
                argument_mapping[param_name],
                inspect.Parameter.KEYWORD_ONLY,
                annotation=annotation,
            )
        )
    return signature.replace(parameters=parameters)


def _wrap_sub_step_fn(
    original_fn: Any,
    prefix: str,
    adapter_name: str,
    sub_pipeline_param_fields: list[str],
    sub_pipeline_params_class: Any,
) -> Any:
    signature = inspect.signature(original_fn)
    argument_mapping = _build_argument_mapping(
        signature,
        prefix,
        adapter_name,
        sub_pipeline_param_fields,
    )

    if inspect.iscoroutinefunction(original_fn):

        @functools.wraps(original_fn)
        async def async_wrapper(**kwargs):
            remapped_kwargs = _remap_kwargs(
                kwargs,
                signature,
                argument_mapping,
                sub_pipeline_param_fields,
            )
            return await original_fn(**remapped_kwargs)

        wrapper = async_wrapper
    else:

        @functools.wraps(original_fn)
        def sync_wrapper(**kwargs):
            remapped_kwargs = _remap_kwargs(
                kwargs,
                signature,
                argument_mapping,
                sub_pipeline_param_fields,
            )
            return original_fn(**remapped_kwargs)

        wrapper = sync_wrapper

    wrapper.__signature__ = _build_wrapper_signature(
        signature,
        argument_mapping,
        sub_pipeline_param_fields,
        sub_pipeline_params_class,
    )
    return wrapper
