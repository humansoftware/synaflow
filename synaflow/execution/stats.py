class StepRunStats:
    def __init__(self) -> None:
        self.success_count = 0
        self.error_count = 0
        self.invocation_count = 0

    def record_success(self, count: int = 1) -> None:
        self.success_count += count
        self.invocation_count += count

    def record_error(self, count: int = 1) -> None:
        self.error_count += count
        self.invocation_count += count

    def set_counts(self, success_count: int, error_count: int) -> None:
        self.success_count = success_count
        self.error_count = error_count
        self.invocation_count = success_count + error_count
