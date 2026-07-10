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
    description: str = ""


pipeline = PipelineDef
step = Step
include = IncludeStep
