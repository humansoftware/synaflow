from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable

from synaflow.core.observers import Observer
from synaflow.core.types import OnError, StepMode, StepParams
from synaflow.core.adapters import is_async_callable

if TYPE_CHECKING:
    from synaflow.core.dag import Dag


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
    # Stamped at definition time by ``PipelineDef.fill_scope_metadata``
    # so ``build_dag`` -> ``_compile_steps`` can read the position
    # directly off the Step (no separate scope-counts walker).
    index_in_scope: int = 0
    total_in_scope: int = 0


@dataclass
class IncludeStep(BaseStep):
    pipeline: "PipelineDef"
    description: str = ""
    # Stamped at definition time alongside ``Step.index_in_scope`` /
    # ``total_in_scope``. The IncludeStep itself doesn't appear in the
    # expanded DAG — its adapter Step inherits these values.
    index_in_scope: int = 0
    total_in_scope: int = 0


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
    description: str = ""

    def fill_scope_metadata(self) -> None:
        """Stamp ``index_in_scope`` / ``total_in_scope`` on direct steps
        only. Sub-pipelines stamp themselves in their own
        ``__post_init__``; the tree of ``PipelineDef`` instances is
        well-formed by construction, so cycles are impossible here."""
        scope_total = len(self.steps)
        for index, step in enumerate(self.steps, start=1):
            step.index_in_scope = index
            step.total_in_scope = scope_total

    def __post_init__(self) -> None:
        self.fill_scope_metadata()

    @cached_property
    def dag(self) -> "Dag":
        """Compiled Dag for this pipeline. Built lazily on first access
        via ``build_dag(self)`` and cached. Raises on any structural
        error per the design-time validation contract. Removed when
        the PipelineRegistry (issue #107) lands."""
        from synaflow.core.dag_builder import build_dag

        return build_dag(self)

    @property
    def requires_sync_runner(self) -> bool:
        return self.dag.requires_sync_runner

    @property
    def requires_async_runner(self) -> bool:
        return self.dag.requires_async_runner

    def to_dict(self) -> dict:
        """Compiled DAG serialized as a JSON-serializable dict."""
        return self.dag.to_dict()

    def get_execution_levels(self) -> list[list[str]]:
        """Steps grouped into topological levels (no in-level dependencies)."""
        return self.dag.get_execution_levels()


pipeline = PipelineDef
step = Step
include = IncludeStep


def _validate_no_async_handlers(pipeline_def: PipelineDef, dag: "Dag") -> None:
    all_observers: list = list(dag.pipeline_observers)
    for node in dag.steps.values():
        all_observers.extend(node.observers)

    for obs in all_observers:
        handler = obs.handler
        if not callable(handler):
            raise TypeError(
                f"Pipeline '{pipeline_def.name}': observer handler is not callable."
            )
        elif is_async_callable(handler):
            handler_name = getattr(handler, "__name__", str(handler))
            func = getattr(handler, "func", None)
            if func is not None:
                handler_name = f"partial of '{func.__name__}'"
            raise TypeError(
                f"Pipeline '{pipeline_def.name}': observer handler "
                f"'{handler_name}' is async but the pipeline runs "
                f"synchronously. Use sync handlers or switch to async_run()."
            )

    for step_name, node in dag.steps.items():
        if node.materializer is not None:
            if not callable(node.materializer):
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': materializer for step '{step_name}' is not callable."
                )
            elif is_async_callable(node.materializer):
                mat_name = getattr(
                    node.materializer, "__name__", str(node.materializer)
                )
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': materializer "
                    f"'{mat_name}' is async but the pipeline runs "
                    f"synchronously."
                )

        if node.error_materializer is not None:
            if not callable(node.error_materializer):
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': error materializer for step '{step_name}' is not callable."
                )
            elif is_async_callable(node.error_materializer):
                mat_name = getattr(
                    node.error_materializer, "__name__", str(node.error_materializer)
                )
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': error_materializer "
                    f"'{mat_name}' is async but the pipeline runs "
                    f"synchronously."
                )

        if node.fn is not None:
            if not callable(node.fn):
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': step function for step '{step_name}' is not callable."
                )
            elif is_async_callable(node.fn):
                fn_name = getattr(node.fn, "__name__", str(node.fn))
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': step function "
                    f"'{fn_name}' is async but the pipeline runs "
                    f"synchronously."
                )


def _validate_no_sync_handlers(pipeline_def: PipelineDef, dag: "Dag") -> None:
    all_observers: list = list(dag.pipeline_observers)
    for node in dag.steps.values():
        all_observers.extend(node.observers)

    for obs in all_observers:
        handler = obs.handler
        if not callable(handler):
            raise TypeError(
                f"Pipeline '{pipeline_def.name}': observer handler is not callable."
            )
        elif not is_async_callable(handler):
            handler_name = getattr(handler, "__name__", str(handler))
            func = getattr(handler, "func", None)
            if func is not None:
                handler_name = f"partial of '{func.__name__}'"
            raise TypeError(
                f"Pipeline '{pipeline_def.name}': observer handler "
                f"'{handler_name}' is synchronous but the pipeline runs "
                f"asynchronously. Use async handlers for async pipelines."
            )

    for step_name, node in dag.steps.items():
        if node.materializer is not None:
            if not callable(node.materializer):
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': materializer for step '{step_name}' is not callable."
                )
            elif not is_async_callable(node.materializer):
                mat_name = getattr(
                    node.materializer, "__name__", str(node.materializer)
                )
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': materializer "
                    f"'{mat_name}' is synchronous but the pipeline runs "
                    f"asynchronously."
                )

        if node.error_materializer is not None:
            if not callable(node.error_materializer):
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': error materializer for step '{step_name}' is not callable."
                )
            elif not is_async_callable(node.error_materializer):
                mat_name = getattr(
                    node.error_materializer, "__name__", str(node.error_materializer)
                )
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': error_materializer "
                    f"'{mat_name}' is synchronous but the pipeline runs "
                    f"asynchronously."
                )

        if node.fn is not None:
            if not callable(node.fn):
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': step function for step '{step_name}' is not callable."
                )
            elif not is_async_callable(node.fn):
                fn_name = getattr(node.fn, "__name__", str(node.fn))
                raise TypeError(
                    f"Pipeline '{pipeline_def.name}': step function "
                    f"'{fn_name}' is synchronous but the pipeline runs "
                    f"asynchronously. Use async handlers for async pipelines."
                )
