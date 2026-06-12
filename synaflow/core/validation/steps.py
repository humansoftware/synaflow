import inspect
from typing import Any

from synaflow.core.step import Step
from synaflow.core.type_compatibility import is_async_stream_type, is_sync_stream_type
from synaflow.core.validation.dependencies import DependencyValidator


class StepValidator:
    @staticmethod
    def validate_step_is_callable(step: Step, pipeline_name: str) -> None:
        if not callable(step.fn):
            raise ValueError(
                f"Pipeline '{pipeline_name}': step '{step.name}' must have a callable 'fn', got {type(step.fn).__name__}"
            )

    @staticmethod
    def validate_unique_step_name(
        step_name: str, dag: dict, pipeline_name: str
    ) -> None:
        if step_name in dag:
            raise ValueError(
                f"Pipeline '{pipeline_name}': duplicate step name '{step_name}'"
            )

    @classmethod
    def validate_and_compile_step(
        cls, step: Step, produced: dict[str, dict], pipeline_name: str
    ) -> dict[str, Any]:
        sig = inspect.signature(step.fn)
        hints = DependencyValidator.get_safe_type_hints(step.fn)

        deps = DependencyValidator.validate_and_resolve_dependencies(
            step, sig, hints, produced, pipeline_name
        )
        output_type = DependencyValidator.resolve_step_output_type(
            sig, hints, deps, produced
        )

        from synaflow.core.type_compatibility import is_materialized_consumer
        from synaflow.core.validation.topology import TopologyValidator

        needs_materialize = any(is_materialized_consumer(t) for t in deps.values())

        return {
            "deps": deps,
            "output": output_type,
            "fn": step.fn,
            "on_error": step.on_error,
            "needs_materialize": needs_materialize,
        }

    @staticmethod
    def validate_sync_async_consistency(
        dag: dict,
        pipeline_name: str,
        steps: list[Step],
        default_materializer_factory: Any,
    ) -> None:
        has_sync = False
        has_async = False

        for node in dag.values():
            if not node.get("fn"):
                continue

            if inspect.iscoroutinefunction(node["fn"]):
                has_async = True

            output = node.get("output")
            if is_sync_stream_type(output):
                has_sync = True
            if is_async_stream_type(output):
                has_async = True

            for dep_type in node.get("deps", {}).values():
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

        if default_materializer_factory:
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

        dag["__metadata__"] = {
            "requires_sync_runner": has_sync,
            "requires_async_runner": has_async,
        }
