import functools
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from synaflow.core.types import OnError, StepMode


def _get_handler_name(handler: Callable) -> str:
    if isinstance(handler, functools.partial):
        return _get_handler_name(handler.func)
    if hasattr(handler, "__name__"):
        return handler.__name__
    if hasattr(handler, "__class__") and hasattr(handler.__class__, "__name__"):
        return handler.__class__.__name__
    return str(handler)


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
    handler: Callable


@dataclass(frozen=True)
class ResolvedObserver:
    handler: Callable
    handler_name: str
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
