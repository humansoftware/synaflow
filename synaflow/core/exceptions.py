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
