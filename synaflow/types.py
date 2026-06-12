from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable, Iterator, Protocol


class OnError(Enum):
    CONTINUE = "continue"
    STOP = "stop"


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


Materializer = Callable[[Iterator[Any]], Iterable[Any]]


class MaterializerFactory(Protocol):
    def __call__(self, ctx: MaterializeContext) -> Materializer:
        ...
