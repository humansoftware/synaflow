"""
Provides event dispatching capabilities for the synchronous execution engine.

This module encapsulates the logic for resolving and invoking pipeline-level and
step-level observers. It triggers lifecycle events (started, completed, failed)
and handles error contexts during pipeline execution.
"""

from synaflow.core.dag import Dag, DagNode
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
    dispatch_observers,
)


class EventDispatcher:
    """
    Dispatches execution lifecycle events to registered observers.

    The EventDispatcher is responsible for notifying external observers about the
    state transitions of the pipeline and its individual steps. It constructs the
    appropriate context objects for each event type (e.g., PipelineStartedContext,
    StepFailedContext) and routes them to observers, applying any execution overrides
    that may modify the observer lists.

    Step-level methods take the ``DagNode`` directly (not the historical
    ``StepConfig`` wrapper or ``Any``). The runtime executor passes the
    compiled ``DagNode`` it is about to execute; tests construct one
    with the same shape. No wrapper indirection.
    """

    def __init__(
        self, dag: Dag, run_id: str, overrides: ExecutionOverrides | None = None
    ):
        self._dag: Dag = dag
        self._run_id: str = run_id
        self._overrides: ExecutionOverrides | None = overrides

    @property
    def run_id(self) -> str:
        return self._run_id

    def resolve_pipeline_observers(self) -> list:
        if self._overrides is None:
            return self._dag.pipeline_observers
        return self._overrides.observers.resolve(
            PIPELINE_SCOPE, self._dag.pipeline_observers
        )

    def resolve_step_observers(self, dag_node: DagNode, step_name: str) -> list:
        pipeline_observers = self.resolve_pipeline_observers()
        step_observers = [obs for obs in dag_node.observers if obs.source == "step"]
        if self._overrides is not None:
            step_observers = self._overrides.observers.resolve(
                step_name, step_observers
            )
        return [*pipeline_observers, *step_observers]

    def pipeline_started(self) -> None:
        registrations = self.resolve_pipeline_observers()
        if not registrations:
            return
        ctx = PipelineStartedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=PipelineEvent.STARTED,
        )
        dispatch_observers(registrations, ctx)

    def pipeline_completed(self) -> None:
        registrations = self.resolve_pipeline_observers()
        if not registrations:
            return
        ctx = PipelineCompletedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=PipelineEvent.COMPLETED,
        )
        dispatch_observers(registrations, ctx)

    def pipeline_failed(
        self, step_name: str | None = None, exception: BaseException | None = None
    ) -> None:
        registrations = self.resolve_pipeline_observers()
        if not registrations:
            return
        ctx = PipelineFailedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=PipelineEvent.FAILED,
            step_name=step_name,
            exception=exception,
        )
        dispatch_observers(registrations, ctx)

    def step_started(self, dag_node: DagNode, step_name: str) -> None:
        registrations = self.resolve_step_observers(dag_node, step_name)
        if not registrations:
            return
        ctx = StepStartedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=StepEvent.STARTED,
            step_name=step_name,
            mode=dag_node.mode,
            on_error=dag_node.on_error,
            pipeline_scope=dag_node.pipeline,
        )
        dispatch_observers(registrations, ctx)

    def step_completed(
        self,
        dag_node: DagNode,
        step_name: str,
        success_count: int = 0,
        error_count: int = 0,
        completed_all_inputs: bool = True,
    ) -> None:
        registrations = self.resolve_step_observers(dag_node, step_name)
        if not registrations:
            return
        ctx = StepCompletedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=StepEvent.COMPLETED,
            step_name=step_name,
            mode=dag_node.mode,
            on_error=dag_node.on_error,
            success_count=success_count,
            error_count=error_count,
            completed_all_inputs=completed_all_inputs,
            pipeline_scope=dag_node.pipeline,
        )
        dispatch_observers(registrations, ctx)

    def step_failed(
        self,
        dag_node: DagNode,
        step_name: str,
        success_count: int = 0,
        error_count: int = 0,
        completed_all_inputs: bool = True,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self.resolve_step_observers(dag_node, step_name)
        if not registrations:
            return
        ctx = StepFailedContext(
            pipeline_name=self._dag.name,
            run_id=self._run_id,
            event=StepEvent.FAILED,
            step_name=step_name,
            mode=dag_node.mode,
            on_error=dag_node.on_error,
            success_count=success_count,
            error_count=error_count,
            completed_all_inputs=completed_all_inputs,
            exception=exception,
            pipeline_scope=dag_node.pipeline,
        )
        dispatch_observers(registrations, ctx)

    def materialization_started(
        self,
        step_name: str,
        dag_node: DagNode,
        consumer_type: object = None,
        materializer_name: str | None = None,
    ) -> None:
        registrations = self.resolve_step_observers(dag_node, step_name)
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
        dispatch_observers(registrations, ctx)

    def materialization_completed(
        self,
        step_name: str,
        dag_node: DagNode,
        consumer_type: object = None,
        materializer_name: str | None = None,
    ) -> None:
        registrations = self.resolve_step_observers(dag_node, step_name)
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
        dispatch_observers(registrations, ctx)

    def materialization_failed(
        self,
        step_name: str,
        dag_node: DagNode,
        consumer_type: object = None,
        materializer_name: str | None = None,
        exception: BaseException | None = None,
    ) -> None:
        registrations = self.resolve_step_observers(dag_node, step_name)
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
        dispatch_observers(registrations, ctx)

    def handle_error(
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

        if not callable(err_mat):
            raise TypeError(
                f"Error materializer for step '{step_name}' is not callable."
            )

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
        err_mat(error_ctx)
