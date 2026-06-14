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
    observers: list = field(default_factory=list)
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
    observers: list = field(default_factory=list)
    dag: Dag = field(default_factory=Dag)
    _compiled: bool = False
    description: str = ""

    def __post_init__(self) -> None:
        from synaflow.core.dag_builder import build_dag

        self.dag = build_dag(
            self.name,
            self.params,
            self.steps,
            self.materializer,
            is_default_factory=(self.materializer is None),
            error_materializer_factory=self.error_materializer,
            pipeline_observers=self.observers,
        )
        self.requires_sync_runner = self.dag.requires_sync_runner
        self.requires_async_runner = self.dag.requires_async_runner

        if self.requires_sync_runner or not self.requires_async_runner:
            _validate_no_async_observers(self)

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


def _validate_no_async_observers(pipeline_def: PipelineDef) -> None:
    import inspect
    import functools

    def _is_async_callable(func: Any) -> bool:
        if func is None:
            return False
        if inspect.iscoroutinefunction(func):
            return True
        if isinstance(func, functools.partial):
            return _is_async_callable(func.func)
        if hasattr(func, "__call__"):
            if inspect.iscoroutinefunction(func.__call__):
                return True
        return False

    all_observers = list(pipeline_def.dag.observers)
    for node in pipeline_def.dag.steps.values():
        all_observers.extend(node.observers)

    for obs in all_observers:
        handler = obs.handler
        if _is_async_callable(handler):
            handler_name = getattr(handler, "__name__", None)
            if handler_name is None:
                func = getattr(handler, "func", None)
                if func is not None:
                    handler_name = (
                        f"partial of '{getattr(func, '__name__', str(func))}'"
                    )
                else:
                    handler_name = str(handler)
            raise ValueError(
                f"Pipeline '{pipeline_def.name}': observer handler "
                f"'{handler_name}' is async but the pipeline runs "
                f"synchronously. Use sync handlers or switch to async_run()."
            )
