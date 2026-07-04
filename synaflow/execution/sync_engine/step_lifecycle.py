from synaflow.core.exceptions import PipelineStopException


class StepLifecycle:
    def __init__(self, node, step_name: str, events):
        self.node = node
        self.step_name = step_name
        self.events = events
        self.success_count = 0
        self.error_count = 0
        self.completed_all_inputs = False
        self._started = False

    def start(self):
        if not self._started:
            self.events.step_started(self.node, self.step_name)
            self._started = True

    def record_success(self, count: int = 1):
        self.success_count += count

    def record_error(self, count: int = 1):
        self.error_count += count

    def finish(self, exception=None, completed_all_inputs: bool = True):
        self.completed_all_inputs = completed_all_inputs
        if exception:
            cause = exception
            if isinstance(cause, PipelineStopException):
                cause = cause.cause or cause
            self.events.step_failed(
                self.node,
                self.step_name,
                success_count=self.success_count,
                error_count=self.error_count,
                completed_all_inputs=self.completed_all_inputs,
                exception=cause,
            )
        else:
            self.events.step_completed(
                self.node,
                self.step_name,
                success_count=self.success_count,
                error_count=self.error_count,
                completed_all_inputs=self.completed_all_inputs,
            )
