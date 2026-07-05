from typing import Any
from synaflow.core.exceptions import PipelineStopException
from synaflow.execution.stats import StepRunStats


class StepLifecycle:
    def __init__(
        self, node: Any, step_name: str, events: Any, stats: StepRunStats
    ) -> None:
        self.node = node
        self.step_name = step_name
        self.events = events
        self.stats = stats
        self.completed_all_inputs = False
        self._started = False

    @property
    def success_count(self) -> int:
        return self.stats.success_count

    @property
    def error_count(self) -> int:
        return self.stats.error_count

    def start(self) -> None:
        if not self._started:
            self.events.step_started(self.node, self.step_name)
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
                self.node,
                self.step_name,
                success_count=self.stats.success_count,
                error_count=self.stats.error_count,
                completed_all_inputs=self.completed_all_inputs,
                exception=cause,
            )
        else:
            self.events.step_completed(
                self.node,
                self.step_name,
                success_count=self.stats.success_count,
                error_count=self.stats.error_count,
                completed_all_inputs=self.completed_all_inputs,
            )
