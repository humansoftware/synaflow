from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.execution.bounded_iterator import BoundedIterator
from synaflow.core.exceptions import PipelineStopException
from synaflow.core.types import (
    ErrorContext,
    OnError,
)


def _maybe_wrap_stream(output, node):
    """Wrap a progressive stream output with bounded handoff if needed."""
    if node.max_in_flight <= 1:
        return output
    if not isinstance(output, Iterator):
        return output
    return BoundedIterator(output, node.max_in_flight)


def _collect_iterator(
    dag: Dag,
    step_name: str,
    value: Iterator,
    run_id: str,
) -> tuple[list[Any], bool, BaseException | None]:
    items = []
    while True:
        try:
            items.append(next(value))
        except StopIteration:
            return items, False, None
        except Exception as exc:
            _handle_error(
                dag,
                step_name,
                exc,
                run_id=run_id,
                success_count=len(items),
                error_count=1,
                completed_all_inputs=False,
            )
            if dag[step_name].on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
            return items, True, exc


def _apply_materializer(
    dag: Dag,
    step_name: str,
    value: Any,
    materializer: Any,
    run_id: str,
    consumer_type: Any = None,
) -> tuple[Any, bool, BaseException | None]:
    if materializer is None:
        if isinstance(value, Iterator):
            items, had_error, exc = _collect_iterator(dag, step_name, value, run_id)
            return items, had_error, exc
        return value, False, None

    if isinstance(value, Iterator):
        items, had_error, exc = _collect_iterator(dag, step_name, value, run_id)
        return materializer(items), had_error, exc

    return materializer(value), False, None


def _handle_error(
    dag: Dag,
    step_name: str,
    exc: BaseException,
    *,
    run_id: str,
    success_count: int = 0,
    error_count: int = 1,
    completed_all_inputs: bool | None = None,
) -> None:
    node = dag.steps.get(step_name)
    if not node:
        return

    err_mat = getattr(node, "error_materializer", None)
    if err_mat is None:
        return

    if not callable(err_mat):
        raise TypeError(f"Error materializer for step '{step_name}' is not callable.")

    error_ctx = ErrorContext(
        pipeline_name=dag.name,
        dataset_name=step_name,
        step_name=step_name,
        run_id=run_id,
        exception=exc,
        mode=getattr(node, "mode", None),
        on_error=getattr(node, "on_error", None),
        success_count=success_count,
        error_count=error_count,
        completed_all_inputs=completed_all_inputs,
    )
    err_mat(error_ctx)


def _wrap_started_stream(it: Any, fire_started: Any) -> Any:
    try:
        for item in it:
            fire_started()
            yield item
    finally:
        fire_started()
