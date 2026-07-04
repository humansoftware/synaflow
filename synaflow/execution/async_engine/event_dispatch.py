"""
Provides event dispatching capabilities for the asynchronous execution engine.

This module encapsulates the logic for resolving and invoking pipeline-level and
step-level observers. It triggers lifecycle events (started, completed, failed)
and handles error contexts during pipeline execution.
"""

from typing import Any

from synaflow.core.dag import Dag
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.core.constants import PIPELINE_SCOPE
from synaflow.core.types import ErrorContext
from synaflow.core.observers import (
    MaterializationCompletedContext,
    MaterializationEvent,
    MaterializationFailedContext,
    MaterializationStartedContext,
    PipelineCompletedContext,
    PipelineEvent,
    PipelineFailedContext,
    PipelineStartedContext,
    StepCompletedContext,
    StepEvent,
    StepFailedContext,
    StepStartedContext,
    dispatch_observers_async,
)


class AsyncEventDispatcher:
    """
    Dispatches execution lifecycle events to registered observers asynchronously.

    The AsyncEventDispatcher is responsible for notifying external observers about the
    state transitions of the pipeline and its individual steps. It constructs the
    appropriate context objects for each event type (e.g., PipelineStartedContext,
    StepFailedContext) and routes them to observers, applying any execution overrides
    that may modify the observer lists.
    """

    def __init__(
        self, dag: Dag, run_id: str, overrides: ExecutionOverrides | None = None
    ):
        self._dag = dag
        self._run_id = run_id
        self._overrides = overrides

    @property
    def run_id(self) -> str:
        return self._run_id

    def _resolve_pipeline_observers(self) -> list:
        if self._overrides is None:
            return self._dag.pipeline_observers
        return self._overrides.observers.resolve(
            PIPELINE_SCOPE, self._dag.pipeline_observers
        )

    def _resolve_step_observers(self, node: Any, step_name: str) -> list:
        pipeline_observers = self._resolve_pipeline_observers()
        step_observers = [obs for obs in node.observers if obs.source == "step"]
        if self._overrides is not None:
            step_observers = self._overrides.observers.resolve(
                step_name, step_observers
            )
        return [*pipeline_observers, *step_observers]

    async def pipeline_started(self) -> None:
        registrations = self._resolve_pipeline_observers()
        if not registrations:
            return
        ctx = PipelineStartedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=PipelineEvent.STARTED,
        )
        await dispatch_observers_async(registrations, ctx)

    async def pipeline_completed(self) -> None:
        registrations = self._resolve_pipeline_observers()
        if not registrations:
            return
        ctx = PipelineCompletedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=PipelineEvent.COMPLETED,
        )
        await dispatch_observers_async(registrations, ctx)

    async def pipeline_failed(
        self, step_name: str | None = None, exception: BaseException | None = None
    ) -> None:
        registrations = self._resolve_pipeline_observers()
        if not registrations:
            return
        ctx = PipelineFailedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=PipelineEvent.FAILED,
            step_name=step_name,
            exception=exception,
        )
        await dispatch_observers_async(registrations, ctx)

    async def step_started(self, node: Any, step_name: str) -> None:
        registrations = self._resolve_step_observers(node, step_name)
        if not registrations:
            return
        ctx = StepStartedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=StepEvent.STARTED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
        )
        await dispatch_observers_async(registrations, ctx)

    async def step_completed(
        self,
        node: Any,
        step_name: str,
        success_count: int = 0,
        error_count: int = 0,
        completed_all_inputs: bool = True,
    ) -> None:
        registrations = self._resolve_step_observers(node, step_name)
        if not registrations:
            return
        ctx = StepCompletedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=StepEvent.COMPLETED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
            success_count=success_count,
            error_count=error_count,
            completed_all_inputs=completed_all_inputs,
        )
        await dispatch_observers_async(registrations, ctx)

    async def step_failed(
        self,
        node: Any,
        step_name: str,
        success_count: int = 0,
        error_count: int = 0,
        completed_all_inputs: bool = True,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self._resolve_step_observers(node, step_name)
        if not registrations:
            return
        ctx = StepFailedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=StepEvent.FAILED,
            step_name=step_name,
            mode=node.mode,
            on_error=node.on_error,
            success_count=success_count,
            error_count=error_count,
            completed_all_inputs=completed_all_inputs,
            exception=exception,
        )
        await dispatch_observers_async(registrations, ctx)

    async def materialization_started(
        self,
        step_name: str,
        node: Any,
        consumer_type: Any = None,
        materializer_name: str | None = None,
    ) -> None:
        registrations = self._resolve_step_observers(node, step_name)
        if not registrations:
            return
        ctx = MaterializationStartedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=MaterializationEvent.STARTED,
            step_name=step_name,
            dataset_name=step_name,
            consumer_type=consumer_type,
            materializer_name=materializer_name,
        )
        await dispatch_observers_async(registrations, ctx)

    async def materialization_completed(
        self,
        step_name: str,
        node: Any,
        consumer_type: Any = None,
        materializer_name: str | None = None,
    ) -> None:
        registrations = self._resolve_step_observers(node, step_name)
        if not registrations:
            return
        ctx = MaterializationCompletedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=MaterializationEvent.COMPLETED,
            step_name=step_name,
            dataset_name=step_name,
            consumer_type=consumer_type,
            materializer_name=materializer_name,
        )
        await dispatch_observers_async(registrations, ctx)

    async def materialization_failed(
        self,
        step_name: str,
        node: Any,
        consumer_type: Any = None,
        materializer_name: str | None = None,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self._resolve_step_observers(node, step_name)
        if not registrations:
            return
        ctx = MaterializationFailedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=MaterializationEvent.FAILED,
            step_name=step_name,
            dataset_name=step_name,
            consumer_type=consumer_type,
            materializer_name=materializer_name,
            exception=exception,
        )
        await dispatch_observers_async(registrations, ctx)

    async def handle_error(
        self,
        step_name: str,
        exc: BaseException,
        success_count: int = 0,
        error_count: int = 1,
        completed_all_inputs: bool | None = None,
    ) -> None:
        node = self._dag.steps.get(step_name)
        if not node:
            return

        err_mat = getattr(node, "error_materializer", None)
        if err_mat is None:
            return

        error_ctx = ErrorContext(
            pipeline_name=self._dag.name,
            dataset_name=step_name,
            step_name=step_name,
            run_id=self._run_id,
            exception=exc,
            mode=getattr(node, "mode", None),
            on_error=getattr(node, "on_error", None),
            success_count=success_count,
            error_count=error_count,
            completed_all_inputs=completed_all_inputs,
        )
        if not callable(err_mat):
            raise TypeError(
                f"Error materializer for step '{step_name}' is not callable."
            )
        await err_mat(error_ctx)
