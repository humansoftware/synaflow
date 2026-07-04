import re

with open("synaflow/execution/async_engine/executor.py", "r") as f:
    content = f.read()

content = content.replace(
    "await self._dispatch_materialization_event(\n            step_name,\n            node,\n            MaterializationEvent.STARTED,\n            consumer_type,\n            mat_name,\n        )",
    "await self.events.materialization_started(\n            step_name,\n            node,\n            consumer_type,\n            mat_name,\n        )",
)
content = content.replace(
    "await self._dispatch_materialization_event(\n                step_name,\n                node,\n                MaterializationEvent.COMPLETED,\n                consumer_type,\n                mat_name,\n            )",
    "await self.events.materialization_completed(\n                step_name,\n                node,\n                consumer_type,\n                mat_name,\n            )",
)
content = content.replace(
    "await self._dispatch_materialization_event(\n                step_name,\n                node,\n                MaterializationEvent.FAILED,\n                consumer_type,\n                mat_name,\n                exception=exc,\n            )",
    "await self.events.materialization_failed(\n                step_name,\n                node,\n                consumer_type,\n                mat_name,\n                exception=exc,\n            )",
)

content = content.replace(
    "await self._dispatch_step_event(\n                node,\n                StepEvent.FAILED,\n                step_name,\n                success_count=success,\n                error_count=max(real_error_count, 1),\n                completed_all_inputs=False,\n                exception=exception,\n            )",
    "await self.events.step_failed(\n                node,\n                step_name,\n                success_count=success,\n                error_count=max(real_error_count, 1),\n                completed_all_inputs=False,\n                exception=exception,\n            )",
)
content = content.replace(
    "await self._dispatch_step_event(\n                node,\n                StepEvent.COMPLETED,\n                step_name,\n                success_count=real_invocation_count - real_error_count,\n                error_count=real_error_count,\n                completed_all_inputs=True,\n            )",
    "await self.events.step_completed(\n                node,\n                step_name,\n                success_count=real_invocation_count - real_error_count,\n                error_count=real_error_count,\n                completed_all_inputs=True,\n            )",
)

content = content.replace(
    "await self._dispatch_step_event(\n            node,\n            StepEvent.COMPLETED,\n            step_name,\n            success_count=real_invocation_count - real_error_count,\n            error_count=real_error_count,\n            completed_all_inputs=True,\n        )",
    "await self.events.step_completed(\n            node,\n            step_name,\n            success_count=real_invocation_count - real_error_count,\n            error_count=real_error_count,\n            completed_all_inputs=True,\n        )",
)

# And remove MaterializationEvent, StepEvent, PipelineEvent imports if any left, wait, I already changed that in first script? Oh I missed to delete from synaflow.core.observers line.
content = re.sub(r"from synaflow\.core\.observers import \([\s\S]*?\n\)", "", content)

# Let's run the tests via python instead to avoid another tool call

with open("synaflow/execution/async_engine/executor.py", "w") as f:
    f.write(content)
