from synaflow.core.dag import DagNode
from synaflow.core.exceptions import PipelineStopException
from synaflow.execution.stats import StepRunStats
from synaflow.execution.sync_engine.event_dispatch import EventDispatcher


class StepLifecycle:
    def __init__(
        self,
        dag_node: DagNode,
        step_name: str,
        events: EventDispatcher,
        stats: StepRunStats,
    ) -> None:
        self.dag_node: DagNode = dag_node
        self.step_name: str = step_name
        self.events: EventDispatcher = events
        self.stats: StepRunStats = stats
        self.completed_all_inputs: bool = False
        self._started: bool = False

    @property
    def success_count(self) -> int:
        return self.stats.success_count

    @property
    def error_count(self) -> int:
        return self.stats.error_count

    def start(self) -> None:
        if not self._started:
            self.events.step_started(self.dag_node, self.step_name)
            self._started = True

    def record_success(self, count: int = 1) -> None:
        self.stats.record_success(count)

    def record_error(self, count: int = 1) -> None:
        self.stats.record_error(count)

    def set_counts(self, success_count: int, error_count: int) -> None:
        self.stats.set_counts(success_count, error_count)

    def finish(
        self, exception: BaseException | None = None, completed_all_inputs: bool = True
    ) -> None:
        self.completed_all_inputs = completed_all_inputs
        if exception:
            cause = exception
            while isinstance(cause, PipelineStopException) and cause.cause:
                cause = cause.cause
            self.events.step_failed(
                self.dag_node,
                self.step_name,
                success_count=self.stats.success_count,
                error_count=self.stats.error_count,
                completed_all_inputs=self.completed_all_inputs,
                exception=cause,
            )
        else:
            self.events.step_completed(
                self.dag_node,
                self.step_name,
                success_count=self.stats.success_count,
                error_count=self.stats.error_count,
                completed_all_inputs=self.completed_all_inputs,
            )
