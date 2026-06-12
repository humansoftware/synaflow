class StepExecutionError(Exception):
    """Raised when user-provided code within a step throws an exception."""

    pass


class PipelineStopException(Exception):
    """Raised to stop the pipeline execution early."""

    pass
