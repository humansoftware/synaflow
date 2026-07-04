with open("synaflow/execution/async_engine/executor.py", "r") as f:
    content = f.read()

content = content.replace(
    "await self._dispatch_materialization_event(\n            step_name,\n            node,\n            MaterializationEvent.STARTED,\n            consumer_type,\n            materializer_name,\n        )",
    "await self.events.materialization_started(\n            step_name,\n            node,\n            consumer_type,\n            materializer_name,\n        )",
)
content = content.replace(
    "await self._dispatch_materialization_event(\n                step_name,\n                node,\n                MaterializationEvent.COMPLETED,\n                consumer_type,\n                materializer_name,\n            )",
    "await self.events.materialization_completed(\n                step_name,\n                node,\n                consumer_type,\n                materializer_name,\n            )",
)
content = content.replace(
    "await self._dispatch_materialization_event(\n                step_name,\n                node,\n                MaterializationEvent.FAILED,\n                consumer_type,\n                materializer_name,\n                exception=exc,\n            )",
    "await self.events.materialization_failed(\n                step_name,\n                node,\n                consumer_type,\n                materializer_name,\n                exception=exc,\n            )",
)

content = content.replace(
    "await self._dispatch_step_event(\n                node,\n                StepEvent.FAILED,\n                step_name,\n                success_count=0,\n                error_count=1,\n                completed_all_inputs=True,\n                exception=exc,\n            )",
    "await self.events.step_failed(\n                node,\n                step_name,\n                success_count=0,\n                error_count=1,\n                completed_all_inputs=True,\n                exception=exc,\n            )",
)
content = content.replace(
    "await self._dispatch_step_event(\n                node,\n                StepEvent.COMPLETED,\n                step_name,\n                success_count=1,\n                error_count=0,\n                completed_all_inputs=True,\n            )",
    "await self.events.step_completed(\n                node,\n                step_name,\n                success_count=1,\n                error_count=0,\n                completed_all_inputs=True,\n            )",
)
content = content.replace(
    "await self._dispatch_step_event(\n            node,\n            StepEvent.COMPLETED,\n            step_name,\n            success_count=1,\n            error_count=0,\n            completed_all_inputs=True,\n        )",
    "await self.events.step_completed(\n            node,\n            step_name,\n            success_count=1,\n            error_count=0,\n            completed_all_inputs=True,\n        )",
)

content = content.replace(
    "await self._dispatch_step_event(\n            node,\n            StepEvent.FAILED,\n            step_name,\n            success_count=0,\n            error_count=1,\n            completed_all_inputs=True,\n            exception=exc,\n        )",
    "await self.events.step_failed(\n            node,\n            step_name,\n            success_count=0,\n            error_count=1,\n            completed_all_inputs=True,\n            exception=exc,\n        )",
)

content = content.replace(
    "        self._resolve_step_observers = self._resolve_step_observers\n", ""
)

with open("synaflow/execution/async_engine/executor.py", "w") as f:
    f.write(content)
