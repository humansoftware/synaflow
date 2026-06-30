from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class OnError(Enum):
    CONTINUE = "continue"
    STOP = "stop"


class StepMode(Enum):
    AUTO = "auto"
    EACH = "each"
    ALL = "all"


@dataclass(frozen=True)
class StepParams:
    values: dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    step_name: str
    status: str  # "ok" | "error" | "skipped"
    output: Any = None
    error: Exception | None = None
    metrics: dict[str, int] = field(default_factory=dict)


@dataclass
class MaterializeContext:
    pipeline_name: str
    dataset_name: str
    item_type: Any
    consumer_type: Any = None


@dataclass
class ErrorMaterializeContext:
    pipeline_name: str
    dataset_name: str


@dataclass(frozen=True)
class ErrorContext:
    pipeline_name: str
    dataset_name: str
    step_name: str
    run_id: str
    exception: BaseException
    mode: StepMode | None = None
    on_error: OnError | None = None
    success_count: int = 0
    error_count: int = 1
    completed_all_inputs: bool | None = None


@dataclass
class ErrorRecord:
    pipeline_name: str
    dataset_name: str
    exception_type: str
    exception_message: str
    traceback: str
    step_name: str | None = None
    run_id: str | None = None
