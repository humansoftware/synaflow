class StepExecutionError(Exception):
    """Raised when user-provided code within a step throws an exception."""

    def __init__(
        self, message: str, step_name: str = "", cause: Exception | None = None
    ):
        super().__init__(message)
        self.step_name = step_name
        self.cause = cause


class PipelineStopException(Exception):
    """Raised to stop the pipeline execution early."""

    def __init__(self, step_name: str = "", cause: Exception | None = None):
        parts = (
            [f"Pipeline stopped at step '{step_name}'"]
            if step_name
            else ["Pipeline stopped"]
        )
        if cause:
            parts.append(f": {cause}")
        super().__init__("".join(parts))
        self.step_name = step_name
        self.cause = cause


class ThresholdExceededException(Exception):
    """Raised when a step's error rate exceeds the configured threshold.

    Always raised AFTER all inputs are consumed, never mid-stream,
    because the final percentage can only be known once all items
    have been processed. Propagates out of run()/async_run() so the
    caller can detect and handle the failure.
    """

    def __init__(
        self,
        step_name: str,
        error_count: int,
        success_count: int,
        threshold_absolute: int | None = None,
        threshold_pct: float | None = None,
    ):
        total = error_count + success_count
        pct = (error_count / total * 100) if total > 0 else 0.0
        msg = (
            f"Step '{step_name}' exceeded error threshold: "
            f"{error_count}/{total} items failed ({pct:.1f}%)"
        )
        if threshold_absolute is not None:
            msg += f" [absolute threshold: {threshold_absolute}]"
        if threshold_pct is not None:
            msg += f" [pct threshold: {threshold_pct * 100:.1f}%]"
        super().__init__(msg)
        self.step_name = step_name
        self.error_count = error_count
        self.success_count = success_count
        self.threshold_absolute = threshold_absolute
        self.threshold_pct = threshold_pct


class InvalidThresholdRaiseInEACHStep(Exception):
    """Raised by the executor when a user manually raises
    ThresholdExceededException from inside an EACH-mode step's fn().

    The executor is the only component that should compute error counts
    and raise ThresholdExceededException -- it has access to the actual
    invocation_count and error_count from the streaming loop.

    To enforce an error threshold, configure it on the Step:
        step("proc", fn=proc, error_threshold_absolute=5)
    """

    def __init__(self, step_name: str, original: ThresholdExceededException):
        super().__init__(
            f"Step '{step_name}' raised ThresholdExceededException manually. "
            f"The executor auto-raises this exception after the loop ends when "
            f"the configured error_threshold_absolute or error_threshold_pct is "
            f"exceeded. Configure the threshold on the step instead of raising "
            f"it from inside fn()."
        )
        self.step_name = step_name
        self.original_exception = original
        self.__cause__ = original
