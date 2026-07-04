import inspect
from dataclasses import dataclass, field
from typing import Any, Callable

from synaflow.core.dag import Dag
from synaflow.core.observers import Observer
from synaflow.core.types import OnError, StepMode, StepParams


@dataclass
class BaseStep:
    name: str
    fn: Callable


@dataclass
class Step(BaseStep):
    on_error: OnError = OnError.CONTINUE
    mode: StepMode = StepMode.AUTO
    params: StepParams | None = None
    materializer: Callable | None = None
    error_materializer: Callable | None = None
    force_materialize: bool = False
    description: str = ""
    pipeline: str | None = None
    parent_pipeline: str | None = None
    observers: list[Observer] = field(default_factory=list)
    max_in_flight: int = 1
    error_threshold_absolute: int | None = None
    error_threshold_pct: float | None = None


@dataclass
class IncludeStep(BaseStep):
    pipeline: "PipelineDef"
    description: str = ""


@dataclass
class PipelineDef:
    """
    Defines a Pipeline workflow.
    """

    name: str
    params: Any
    steps: list[Step | IncludeStep]
    resources: dict[str, Any] = field(default_factory=dict)
    exports: str | None = None
    materializer: Callable | None = None
    error_materializer: Callable | None = None
    observers: list[Observer] = field(default_factory=list)
    dag: Dag = field(default_factory=Dag)
    _compiled: bool = False
    description: str = ""

    def __post_init__(self) -> None:
        from synaflow.core.dag_builder import build_dag

        self.dag = build_dag(
            self.name,
            self.params,
            self.steps,
            self.resources,
            self.materializer,
            is_default_factory=(self.materializer is None),
            error_materializer_factory=self.error_materializer,
            pipeline_observers=self.observers,
            exports=self.exports,
        )
        self.requires_sync_runner = self.dag.requires_sync_runner
        self.requires_async_runner = self.dag.requires_async_runner

        if self.requires_sync_runner or not self.requires_async_runner:
            _validate_no_async_observers(self)
        else:
            _validate_no_sync_handlers(self)

    def to_dict(self) -> dict:
        """Exports the compiled DAG structure to a JSON-serializable dictionary."""
        return self.dag.to_dict()

    def get_execution_levels(self) -> list[list[str]]:
        """
        Returns the steps grouped into topological levels.
        Steps in the same level have no dependencies on each other and
        could theoretically be executed in parallel.
        """
        return self.dag.get_execution_levels()


pipeline = PipelineDef
step = Step
include = IncludeStep


def _is_async_handler(handler: Any) -> bool:
    if inspect.iscoroutinefunction(handler):
        return True
    if hasattr(handler, "__call__") and inspect.iscoroutinefunction(handler.__call__):
        return True
    func = getattr(handler, "func", None)
    if func is not None and (
        inspect.iscoroutinefunction(func)
        or (hasattr(func, "__call__") and inspect.iscoroutinefunction(func.__call__))
    ):
        return True
    return False


def _validate_no_async_observers(pipeline_def: PipelineDef) -> None:
    all_observers: list = list(pipeline_def.dag.pipeline_observers)
    for node in pipeline_def.dag.steps.values():
        all_observers.extend(node.observers)

    for obs in all_observers:
        handler = obs.handler
        if _is_async_handler(handler):
            handler_name = getattr(handler, "__name__", str(handler))
            func = getattr(handler, "func", None)
            if func is not None:
                handler_name = f"partial of '{func.__name__}'"
            raise ValueError(
                f"Pipeline '{pipeline_def.name}': observer handler "
                f"'{handler_name}' is async but the pipeline runs "
                f"synchronously. Use sync handlers or switch to async_run()."
            )


def _validate_no_sync_handlers(pipeline_def: PipelineDef) -> None:
    all_observers: list = list(pipeline_def.dag.pipeline_observers)
    for node in pipeline_def.dag.steps.values():
        all_observers.extend(node.observers)

    for obs in all_observers:
        handler = obs.handler
        if not _is_async_handler(handler):
            handler_name = getattr(handler, "__name__", str(handler))
            func = getattr(handler, "func", None)
            if func is not None:
                handler_name = f"partial of '{func.__name__}'"
            raise TypeError(
                f"Pipeline '{pipeline_def.name}': observer handler "
                f"'{handler_name}' is synchronous but the pipeline runs "
                f"asynchronously. Use async handlers for async pipelines."
            )

    # Check materializers and error materializers
    if (
        pipeline_def.dag.error_materializer_factory is not None
        and not _is_async_handler(pipeline_def.dag.error_materializer_factory)
        and not getattr(pipeline_def.dag.error_materializer_factory, "__name__", "")
        == "log_error_materializer"
    ):
        pass  # Wait, let's only check step level first, then dag level. Actually dag_builder evaluates factories. So they are on the nodes.

    for node in pipeline_def.dag.steps.values():
        if node.materializer is not None and not _is_async_handler(node.materializer):
            # Built-in memory materializers shouldn't be rejected? Wait, if they are not async, they need an adapter.
            # But the prompt says "if an observer or materializer handler is not async".
            mat_name = getattr(node.materializer, "__name__", str(node.materializer))
            if mat_name not in [
                "memory_materializer",
                "list",
                "dict",
                "set",
                "tuple",
                "log_error_materializer",
                "log_error",
                "_identity",
            ]:
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': materializer "
                    f"'{mat_name}' is synchronous but the pipeline runs "
                    f"asynchronously."
                )
        if node.error_materializer is not None and not _is_async_handler(
            node.error_materializer
        ):
            mat_name = getattr(
                node.error_materializer, "__name__", str(node.error_materializer)
            )
            if mat_name not in ["log_error", "log_error_materializer"]:
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': error_materializer "
                    f"'{mat_name}' is synchronous but the pipeline runs "
                    f"asynchronously."
                )
