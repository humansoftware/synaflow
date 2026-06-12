from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .types import OnError, StepParams


@dataclass
class Step:
    name: str
    fn: Callable
    on_error: OnError = OnError.CONTINUE
    params: StepParams | None = None
    materializer: SyncMaterializer | AsyncMaterializer | Callable | None = None
    description: str = ""


step = Step
