"""
Implements error threshold checking and validation for the synchronous execution engine.

This module evaluates step-level error thresholds (absolute or percentage-based)
to determine if a step has failed beyond acceptable limits, triggering a pipeline
failure. It also handles validation of manually raised threshold exceptions.
"""

from typing import Any
from synaflow.core.exceptions import (
    ThresholdExceededException,
    InvalidThresholdRaiseInEACHStep,
)


def wrap_threshold_raise_if_manual(exc: BaseException, step_name: str) -> BaseException:
    """If the user manually raised ThresholdExceededException from inside fn(),
    wrap it so the error materializer logs a clear "you're misusing the API"
    message. The original is preserved as __cause__ for full traceback."""
    if isinstance(exc, ThresholdExceededException):
        return InvalidThresholdRaiseInEACHStep(step_name=step_name, original=exc)
    return exc


def check_threshold(
    step_name: str,
    node: Any,
    invocation_count: int,
    error_count: int,
) -> None:
    """Called AFTER all inputs are consumed. Raises ThresholdExceededException
    if any threshold is violated. Not called mid-stream to avoid false positives."""
    if node.error_threshold_absolute is None and node.error_threshold_pct is None:
        return

    success_count = invocation_count - error_count

    if node.error_threshold_absolute is not None:
        if error_count >= node.error_threshold_absolute:
            raise ThresholdExceededException(
                step_name=step_name,
                error_count=error_count,
                success_count=success_count,
                threshold_absolute=node.error_threshold_absolute,
            )

    if node.error_threshold_pct is not None:
        if (
            invocation_count > 0
            and (error_count / invocation_count) >= node.error_threshold_pct
        ):
            raise ThresholdExceededException(
                step_name=step_name,
                error_count=error_count,
                success_count=success_count,
                threshold_pct=node.error_threshold_pct,
            )


def compute_completed_all_inputs_for_all(
    node: Any, arguments: dict, exc: ThresholdExceededException
) -> bool:
    """For an ALL step that manually raised ThresholdExceededException,
    determine if the user processed all inputs before raising.

    Total processed = exc.error_count + exc.success_count (user-supplied).
    Total input = sum of len(arg) across deps, for sized collections only.
    Returns False if input size is unknown (non-sized iterators).
    """
    total_input = 0
    known = True
    for value in arguments.values():
        try:
            total_input += len(value)
        except TypeError:
            known = False
            break
    if not known:
        return False
    return (exc.error_count + exc.success_count) == total_input


def has_threshold(node: Any) -> bool:
    return (
        node.error_threshold_absolute is not None
        or node.error_threshold_pct is not None
    )
