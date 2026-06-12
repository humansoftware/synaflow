import functools
import inspect
from typing import Any

from synaflow.core.step import IncludeStep, Step


class MacroExpander:
    @classmethod
    def expand(cls, steps: list[Any]) -> list[Step]:
        expanded = []
        for step in steps:
            if isinstance(step, IncludeStep):
                expanded.extend(cls._expand_include(step))
            else:
                expanded.append(step)
        return expanded

    @classmethod
    def _expand_include(cls, include_step: IncludeStep) -> list[Step]:
        prefix = include_step.name
        sub_pipeline = include_step.pipeline
        adapter_name = f"{prefix}__adapter"

        if not sub_pipeline.exports:
            raise ValueError(
                f"Pipeline '{sub_pipeline.name}' does not define 'exports', so it cannot be included."
            )

        # Validate adapter typing
        sig = inspect.signature(include_step.fn)
        if sig.return_annotation is inspect.Parameter.empty:
            raise ValueError(
                f"Include step '{prefix}' must have a return type hint matching '{sub_pipeline.params.__name__}' or an Iterable of it."
            )

        # Check if the return annotation matches the sub_pipeline.params (ignoring Generic/Iterable wrappers for now, or just trusting it exists and isn't empty)
        # For a truly strict validation we'd unwrap Iterable/Iterator, but ensuring it's not empty is a good start.

        # 1. The adapter step
        adapter_step = Step(
            name=adapter_name,
            fn=include_step.fn,
            on_error=include_step.on_error,
            description=include_step.description,
        )

        expanded = [adapter_step]

        # 2. Extract the sub-pipeline's parameter fields
        if hasattr(sub_pipeline.params, "_fields"):
            b_params_fields = list(sub_pipeline.params._fields)
        else:
            b_params_fields = []

        # 3. Expand the sub-pipeline's steps
        sub_steps = cls.expand(sub_pipeline.steps)  # Recursive!

        for sub_step in sub_steps:
            wrapped_fn = cls._wrap_sub_step_fn(
                sub_step.fn, prefix, adapter_name, b_params_fields, sub_pipeline.params
            )

            # If this is the exported step, we name it exactly `prefix` so downstream A can use it!
            # Otherwise, we prefix it with `prefix__`
            is_exported = sub_step.name == sub_pipeline.exports
            new_name = prefix if is_exported else f"{prefix}__{sub_step.name}"

            expanded.append(
                Step(
                    name=new_name,
                    fn=wrapped_fn,
                    on_error=sub_step.on_error,
                    params=sub_step.params,
                    materializer=sub_step.materializer,
                    description=sub_step.description,
                )
            )

        return expanded

    @classmethod
    def _wrap_sub_step_fn(
        cls,
        original_fn: Any,
        prefix: str,
        adapter_name: str,
        b_params_fields: list[str],
        b_params_class: Any,
    ) -> Any:
        sig = inspect.signature(original_fn)

        # Create mapping of old arg name to new arg name
        arg_mapping = {}
        for param_name in sig.parameters:
            if param_name in b_params_fields:
                arg_mapping[param_name] = adapter_name
            else:
                arg_mapping[param_name] = f"{prefix}__{param_name}"

        if inspect.iscoroutinefunction(original_fn):

            @functools.wraps(original_fn)
            async def async_wrapper(**kwargs):
                new_kwargs = {}
                for param_name in sig.parameters:
                    source_name = arg_mapping[param_name]
                    if param_name in b_params_fields:
                        # Extract from the adapter's Params tuple
                        params_obj = kwargs[source_name]
                        new_kwargs[param_name] = getattr(params_obj, param_name)
                    else:
                        new_kwargs[param_name] = kwargs[source_name]
                return await original_fn(**new_kwargs)

            wrapper = async_wrapper
        else:

            @functools.wraps(original_fn)
            def sync_wrapper(**kwargs):
                new_kwargs = {}
                for param_name in sig.parameters:
                    source_name = arg_mapping[param_name]
                    if param_name in b_params_fields:
                        params_obj = kwargs[source_name]
                        new_kwargs[param_name] = getattr(params_obj, param_name)
                    else:
                        new_kwargs[param_name] = kwargs[source_name]
                return original_fn(**new_kwargs)

            wrapper = sync_wrapper

        new_params = []
        for param_name, param in sig.parameters.items():
            source_name = arg_mapping[param_name]
            if param_name in b_params_fields:
                annotation = b_params_class
            else:
                annotation = param.annotation
            new_params.append(
                inspect.Parameter(
                    source_name, inspect.Parameter.KEYWORD_ONLY, annotation=annotation
                )
            )

        wrapper.__signature__ = sig.replace(parameters=new_params)
        return wrapper
