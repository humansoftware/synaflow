from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from synaflow.core.types import OnError, StepMode


class PipelineEvent(Enum):
    STARTED = "pipeline_started"
    COMPLETED = "pipeline_completed"
    FAILED = "pipeline_failed"


class StepEvent(Enum):
    STARTED = "step_started"
    COMPLETED = "step_completed"
    FAILED = "step_failed"


class MaterializationEvent(Enum):
    STARTED = "materialization_started"
    COMPLETED = "materialization_completed"
    FAILED = "materialization_failed"


@dataclass
class Observer:
    event: Enum
    handler: Callable


@dataclass(frozen=True)
class ResolvedObserver:
    event: Enum
    handler: Callable
    source: str  # "pipeline" | "step"


@dataclass(frozen=True)
class BaseObserverContext:
    pipeline_name: str
    event: Enum


@dataclass(frozen=True)
class PipelineStartedContext(BaseObserverContext):
    pass


@dataclass(frozen=True)
class PipelineCompletedContext(BaseObserverContext):
    pass


@dataclass(frozen=True)
class PipelineFailedContext(BaseObserverContext):
    step_name: str | None
    exception: BaseException


@dataclass(frozen=True)
class StepStartedContext(BaseObserverContext):
    step_name: str
    mode: StepMode
    on_error: OnError


@dataclass(frozen=True)
class StepCompletedContext(BaseObserverContext):
    step_name: str
    mode: StepMode
    on_error: OnError
    success_count: int
    error_count: int
    completed_all_inputs: bool


@dataclass(frozen=True)
class StepFailedContext(BaseObserverContext):
    step_name: str
    mode: StepMode
    on_error: OnError
    success_count: int
    error_count: int
    completed_all_inputs: bool
    exception: BaseException


@dataclass(frozen=True)
class MaterializationStartedContext(BaseObserverContext):
    step_name: str
    dataset_name: str
    consumer_type: Any | None = None
    materializer_name: str | None = None


@dataclass(frozen=True)
class MaterializationCompletedContext(BaseObserverContext):
    step_name: str
    dataset_name: str
    consumer_type: Any | None = None
    materializer_name: str | None = None


@dataclass(frozen=True)
class MaterializationFailedContext(BaseObserverContext):
    step_name: str
    dataset_name: str
    exception: BaseException
    consumer_type: Any | None = None
    materializer_name: str | None = None
