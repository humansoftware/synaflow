"""
Observer system: public lifecycle event types, registration object, immutable
context dataclasses, and a fire-and-forget dispatch helper.

Events are grouped into three small families:
  - PipelineEvent  (pipeline-level lifecycle)
  - StepEvent       (per-step lifecycle)
  - MaterializationEvent (per-materialization-operation lifecycle)

Observer registration is declarative only — it does not change execution
semantics, laziness, or materialization decisions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from synaflow.core.types import OnError, StepMode

_log = logging.getLogger("synaflow.observers")


# ---------------------------------------------------------------------------
# Event families
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Observer registration
# ---------------------------------------------------------------------------


@dataclass
class Observer:
    """Minimal public registration shape.

    ``step_name`` does not belong here — scope is defined by where the
    observer is registered (pipeline vs step).
    """

    event: Enum
    handler: Callable


# ---------------------------------------------------------------------------
# Immutable context dataclasses — one per event
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Dispatch helpers
# ---------------------------------------------------------------------------


def dispatch_observers(
    registrations: list[Observer],
    event: Enum,
    context: BaseObserverContext,
) -> None:
    """Fire-and-forget synchronous dispatch.

    Calls every matching handler.  Observer failures are logged and
    swallowed — they never affect step or pipeline execution.
    """
    for reg in registrations:
        if reg.event is not event:
            continue
        try:
            reg.handler(context)
        except Exception:
            _log.exception("Observer handler for event %r failed", event.value)


async def dispatch_observers_async(
    registrations: list[Observer],
    event: Enum,
    context: BaseObserverContext,
) -> None:
    """Fire-and-forget asynchronous dispatch.

    Same semantics as ``dispatch_observers`` but awaits any awaitable
    returned by a handler (supports ``async def``, ``functools.partial``,
    and callable objects with ``async __call__``).
    """
    for reg in registrations:
        if reg.event is not event:
            continue
        try:
            result = reg.handler(context)
            if hasattr(result, "__await__"):
                await result
        except Exception:
            _log.exception("Observer handler for event %r failed", event.value)
