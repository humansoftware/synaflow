from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .types import Materializer, MaterializerFactory, OnError, StepParams


@dataclass
class Step:
    name: str
    fn: Callable
    on_error: OnError = OnError.CONTINUE
    params: StepParams | None = None
    materializer: MaterializerFactory | Materializer | None = None
    description: str = ""


step = Step
