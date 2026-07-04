import re

with open("synaflow/execution/async_engine/executor.py", "r") as f:
    content = f.read()

# 1. Update imports
content = re.sub(
    r"from synaflow\.core\.observers import \([\s\S]*?dispatch_observers_async,\n\)",
    "from .event_dispatch import AsyncEventDispatcher",
    content,
)
content = re.sub(
    r"from synaflow\.core\.types import \(\n    ErrorContext,\n",
    "from synaflow.core.types import (\n",
    content,
)

# 2. Remove _handle_error
content = re.sub(
    r"async def _handle_error\([\s\S]*?raise TypeError[^\n]+\n", "", content
)

# 3. Update signatures and calls in module-level functions
content = content.replace(
    "run_id: str,\n) -> tuple[list[Any], bool, BaseException | None]:",
    "events: Any,\n) -> tuple[list[Any], bool, BaseException | None]:",
)
content = content.replace("run_id=run_id,", "run_id=events.run_id,")
content = content.replace(
    "await _handle_error(\n            dag,\n            step_name,\n            exc,\n            run_id=events.run_id,",
    "await events.handle_error(\n            step_name,\n            exc,",
)
content = content.replace(
    "run_id: str,\n    consumer_type: Any = None,\n) -> tuple[Any, bool, BaseException | None]:",
    "events: Any,\n    consumer_type: Any = None,\n) -> tuple[Any, bool, BaseException | None]:",
)
content = content.replace(
    "dag, step_name, value, run_id", "dag, step_name, value, events"
)

content = content.replace(
    "dag: Dag | None = None,\n    run_id: str | None = None,",
    "dag: Dag | None = None,\n    events: Any = None,",
)
content = content.replace(
    "if dag is not None and run_id is not None:\n            await _handle_error(dag, name, cause, run_id=run_id)",
    "if dag is not None and events is not None:\n            await events.handle_error(name, cause)",
)

# 4. AsyncPipelineExecutor.__init__
content = content.replace(
    "self._resource_factories = dict(resource_factories or {})\n        self.run_id = str(uuid.uuid4())",
    "self._resource_factories = dict(resource_factories or {})\n        self.run_id = str(uuid.uuid4())\n        self.events = AsyncEventDispatcher(self.dag, self.run_id, self._overrides)",
)

# 5. Remove observer and dispatch methods
content = re.sub(
    r"    def _resolve_pipeline_observers[\s\S]*?await dispatch_observers_async\(registrations, ctx\)\n",
    "",
    content,
)

# 6. Replace internal dispatch calls
content = content.replace(
    "await self._dispatch_pipeline_event(PipelineEvent.STARTED)",
    "await self.events.pipeline_started()",
)
content = content.replace(
    "await self._dispatch_pipeline_event(PipelineEvent.COMPLETED)",
    "await self.events.pipeline_completed()",
)
content = content.replace(
    "await self._dispatch_pipeline_event(\n                PipelineEvent.FAILED,\n                step_name=exc.step_name,\n                exception=exc.cause or exc,\n            )",
    "await self.events.pipeline_failed(\n                step_name=exc.step_name,\n                exception=exc.cause or exc,\n            )",
)
content = content.replace(
    "await self._dispatch_pipeline_event(\n                PipelineEvent.FAILED,\n                step_name=exc.step_name,\n                exception=exc,\n            )",
    "await self.events.pipeline_failed(\n                step_name=exc.step_name,\n                exception=exc,\n            )",
)
content = content.replace(
    "await self._dispatch_pipeline_event(\n                PipelineEvent.FAILED, step_name=None, exception=exc\n            )",
    "await self.events.pipeline_failed(\n                step_name=None, exception=exc\n            )",
)

content = content.replace(
    "await self._dispatch_step_event(node, StepEvent.STARTED, step_name)",
    "await self.events.step_started(node, step_name)",
)
content = content.replace(
    "await self._dispatch_step_event(\n            node,\n            StepEvent.COMPLETED,\n            step_name,\n            success_count=success_count,\n            error_count=0,\n            completed_all_inputs=True,\n        )",
    "await self.events.step_completed(\n            node,\n            step_name,\n            success_count=success_count,\n            error_count=0,\n            completed_all_inputs=True,\n        )",
)
content = content.replace(
    "await self._dispatch_step_event(\n            node,\n            StepEvent.FAILED,\n            step_name,\n            success_count=success_count,\n            error_count=error_count,\n            completed_all_inputs=completed_all_inputs,\n            exception=cause,\n        )",
    "await self.events.step_failed(\n            node,\n            step_name,\n            success_count=success_count,\n            error_count=error_count,\n            completed_all_inputs=completed_all_inputs,\n            exception=cause,\n        )",
)

content = content.replace(
    "await self._dispatch_step_event(\n                        node,\n                        StepEvent.COMPLETED,\n                        step_name,\n                        success_count=success_count,\n                        error_count=error_count,\n                        completed_all_inputs=True,\n                    )",
    "await self.events.step_completed(\n                        node,\n                        step_name,\n                        success_count=success_count,\n                        error_count=error_count,\n                        completed_all_inputs=True,\n                    )",
)

# Handle error calls in methods
content = content.replace(
    "await _handle_error(\n                    self.dag,\n                    step_name,\n                    exc,\n                    run_id=self.run_id,\n                    success_count=exc.success_count,\n                    error_count=exc.error_count,\n                    completed_all_inputs=completed_all_inputs,\n                )",
    "await self.events.handle_error(\n                    step_name,\n                    exc,\n                    success_count=exc.success_count,\n                    error_count=exc.error_count,\n                    completed_all_inputs=completed_all_inputs,\n                )",
)
content = content.replace(
    "await _handle_error(self.dag, step_name, exc, run_id=self.run_id)",
    "await self.events.handle_error(step_name, exc)",
)
content = content.replace(
    "await _handle_error(\n                            self.dag,\n                            step_name,\n                            wrap_threshold_raise_if_manual(exc, step_name),\n                            run_id=self.run_id,\n                            success_count=invocation_count - error_count,\n                            error_count=error_count,\n                            completed_all_inputs=False,\n                        )",
    "await self.events.handle_error(\n                            step_name,\n                            wrap_threshold_raise_if_manual(exc, step_name),\n                            success_count=invocation_count - error_count,\n                            error_count=error_count,\n                            completed_all_inputs=False,\n                        )",
)

# _dispatch_materialization_event replacements
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


# function calls
content = content.replace(
    "dag, step_name, consumer_queue, run_id", "dag, step_name, consumer_queue, events"
)

# Handle run_id -> events in calls to _collect_async_iterator, _apply_materializer, _pump_iterator
content = content.replace(
    "dag=self.dag,\n                run_id=self.run_id,\n            )",
    "dag=self.dag,\n                events=self.events,\n            )",
)
content = content.replace(
    "self.dag, step_name, queue, self.run_id", "self.dag, step_name, queue, self.events"
)
content = content.replace(
    "self.dag, step_name, val, materializer, self.run_id, type(self)",
    "self.dag, step_name, val, materializer, self.events, type(self)",
)


with open("synaflow/execution/async_engine/executor.py", "w") as f:
    f.write(content)
