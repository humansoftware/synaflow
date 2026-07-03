import re

with open("synaflow/execution/sync_engine/executor.py", "r") as f:
    content = f.read()

# Remove early StepEvent.STARTED
content = re.sub(
    r"(\s*)unrolled = self\.dag\.each_inputs\(step_name\)\n\s*self\._dispatch_step_event\(node, StepEvent\.STARTED, step_name\)",
    r"\1unrolled = self.dag.each_inputs(step_name)",
    content,
)

# Add StepEvent.STARTED wrapper
def_run_step = """
    def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        resource_stack = ExitStack()
        arguments = self._build_arguments(step_name, node, resource_stack)
        unrolled = self.dag.each_inputs(step_name)

        started = False
        def fire_started():
            nonlocal started
            if not started:
                self._dispatch_step_event(node, StepEvent.STARTED, step_name)
                started = True

        try:
            if not unrolled:
                import collections.abc
                is_gen = False
                # We can't know for sure if it returns an iterator until we run it,
                # unless we inspect the signature or just run it.
                # If it's a normal function, we MUST fire started before running it,
                # because it blocks. But if it's a generator, we shouldn't fire it yet.
                # Actually, inspect.isgeneratorfunction or inspect.iscoroutinefunction is possible.
"""

# Let's inspect how _unroll_step is called.
