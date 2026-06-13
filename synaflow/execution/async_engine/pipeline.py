import asyncio
from typing import Any

from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException

from .dependencies import AsyncDependencyResolver
from .steps import AsyncNodeRunner
from .topology import AsyncStreamManager


class AsyncPipelineExecutor:
    """Executes a compiled Directed Acyclic Graph (DAG) asynchronously."""

    def __init__(self, pipeline: PipelineDef):
        self.pipeline = pipeline
        self.dag = pipeline.dag
        self.context: dict[str, Any] = {}
        self.pump_tasks: list[asyncio.Task] = []

        self.stream_manager = AsyncStreamManager(
            self.pipeline, self.context, self.pump_tasks
        )
        self.resolver = AsyncDependencyResolver(self.context)
        self.runner = AsyncNodeRunner(
            self.pipeline, self.context, self.resolver, self.stream_manager
        )

    async def execute(self, params: Any) -> None:
        self._initialize_context_with_params(params)

        try:
            levels = self.pipeline.get_execution_levels()
            for level in levels:
                tasks = [self.runner.execute_node(name) for name in level]
                if tasks:
                    await asyncio.gather(*tasks)

            if self.pump_tasks:
                await asyncio.gather(*self.pump_tasks)
        except PipelineStopException:
            pass

    def _initialize_context_with_params(self, params: Any) -> None:
        for field, value in params._asdict().items():
            self.stream_manager.store_output(field, value)


async def async_run(pipeline: PipelineDef, params: Any) -> None:
    """Executes a pipeline definition asynchronously."""
    if getattr(pipeline, "requires_sync_runner", False):
        raise RuntimeError(
            "This pipeline contains synchronous streams (Iterator). It must be executed with run() or migrated to AsyncIterator."
        )

    executor = AsyncPipelineExecutor(pipeline)
    await executor.execute(params)
