from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

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

    def to_dict(self) -> dict:
        """Compiled DAG serialized as a JSON-serializable dict."""
        from synaflow.core.dag_builder import build_dag

        return build_dag(self).to_dict()

    def get_execution_levels(self) -> list[list[str]]:
        """Steps grouped into topological levels (no in-level dependencies)."""
        from synaflow.core.dag_builder import build_dag

        return build_dag(self).get_execution_levels()


pipeline = PipelineDef
step = Step
include = IncludeStep
