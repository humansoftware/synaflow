from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .types import OnError, StepParams


@dataclass
class Step:
    name: str
    fn: Callable
    on_error: OnError = OnError.CONTINUE
    params: StepParams | None = None
    materializer: Callable | None = None
    description: str = ""


@dataclass
class IncludeStep:
    name: str
    pipeline: Any  # Cannot import PipelineDef due to circular imports, typed as Any
    fn: Callable
    on_error: OnError = OnError.STOP
    description: str = ""


step = Step
include = IncludeStep
