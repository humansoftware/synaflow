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

    Carries only the final handler callable.  Event filtering, step
    scoping, and other selection rules belong in wrapper helpers above
    the core framework — the handler inspects ``ctx.event`` to decide
    whether to act.

    ``step_name`` does not belong here — scope is defined by where the
    observer is registered (pipeline vs step).
    """

    handler: Callable


# ---------------------------------------------------------------------------
# Internal resolved registration (build-time only)
# ---------------------------------------------------------------------------


@dataclass
class ResolvedObserver:
    """Internal build-time resolution of a public Observer registration.

    Carries the final handler and the registration origin so the DAG
    JSON can preserve the real ``source`` without mutating the public
    ``Observer`` object.
    """

    handler: Callable
    source: str  # "pipeline" or "step"


# ---------------------------------------------------------------------------
# Immutable context dataclasses — one per event
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BaseObserverContext:
    pipeline_name: str
    run_id: str
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
    context: BaseObserverContext,
) -> None:
    """Fire-and-forget synchronous dispatch.

    Calls every registered handler unconditionally.  Handlers inspect
    ``ctx.event`` to decide whether to act.  Observer failures are
    logged and swallowed — they never affect step or pipeline execution.
    """
    for reg in registrations:
        try:
            reg.handler(context)
        except Exception:
            _log.exception("Observer handler failed for event %r", context.event.value)


async def dispatch_observers_async(
    registrations: list[Observer],
    context: BaseObserverContext,
) -> None:
    """Fire-and-forget asynchronous dispatch.

    Same semantics as ``dispatch_observers`` but awaits any awaitable
    returned by a handler (supports ``async def``, ``functools.partial``,
    and callable objects with ``async __call__``).
    """
    for reg in registrations:
        try:
            await reg.handler(context)
        except Exception:
            _log.exception("Observer handler failed for event %r", context.event.value)
