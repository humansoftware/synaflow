from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .types import OnError, StepParams


@dataclass
class BaseStep:
    name: str
    fn: Callable


@dataclass
class Step(BaseStep):
    on_error: OnError = OnError.CONTINUE
    params: StepParams | None = None
    materializer: Callable | None = None
    force_materialize: bool = False
    description: str = ""
    pipeline: str | None = None
    parent_pipeline: str | None = None


@dataclass
class IncludeStep(BaseStep):
    pipeline: "PipelineDef" = None  # type: ignore
    description: str = ""


step = Step
include = IncludeStep
