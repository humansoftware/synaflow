from dataclasses import dataclass, field
from typing import Any, Callable

from synaflow.core.dag import Dag
from synaflow.core.types import OnError, StepMode, StepParams


def _get_default_factory():
    from synaflow.core.dag_builder import memory_materializer_factory

    return memory_materializer_factory


def _get_default_error_factory():
    from synaflow.core.dag_builder import log_error_materializer_factory

    return log_error_materializer_factory


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
    exports: str | None = None
    materializer: Callable | None = None
    error_materializer: Callable | None = None
    dag: Dag = field(default_factory=Dag)
    _compiled: bool = False
    description: str = ""

    def __post_init__(self) -> None:
        from synaflow.core.dag_builder import build_dag
        from synaflow.core.dag_builder import (
            memory_materializer_factory as _default_factory,
            log_error_materializer_factory as _default_error_factory,
        )

        if self.materializer is None:
            self.materializer = _default_factory
        if self.error_materializer is None:
            self.error_materializer = _default_error_factory

        self.dag = build_dag(
            self.name,
            self.params,
            self.steps,
            self.materializer,
            is_default_factory=(self.materializer is _default_factory),
            error_materializer_factory=self.error_materializer,
        )
        self.requires_sync_runner = self.dag.requires_sync_runner
        self.requires_async_runner = self.dag.requires_async_runner

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
