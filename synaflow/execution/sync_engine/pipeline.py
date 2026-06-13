from collections.abc import Iterator
from typing import Any

from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException

from .dependencies import SyncDependencyResolver
from .steps import SyncNodeRunner
from .topology import SyncStreamManager


class PipelineExecutor:
    """Executes a compiled Directed Acyclic Graph (DAG) for a pipeline."""

    def __init__(self, pipeline: PipelineDef):
        self.pipeline = pipeline
        self.dag = pipeline._dag
        self.context: dict[str, Any] = {}
        self.executed_steps: set[str] = set()

        self.stream_manager = SyncStreamManager(self.pipeline)
        self.resolver = SyncDependencyResolver(self.pipeline, self.context)
        self.runner = SyncNodeRunner(
            self.pipeline,
            self.context,
            self.executed_steps,
            self.resolver,
            self.stream_manager,
        )

    def execute(self, params: Any) -> None:
        self._initialize_context_with_params(params)

        try:
            levels = self.pipeline.get_execution_levels()
            for level in levels:
                self._execute_level(level)
        except PipelineStopException:
            pass

    def _initialize_context_with_params(self, params: Any) -> None:
        for field, value in params._asdict().items():
            self.context[field] = value

    def _execute_level(self, level: list[str]) -> None:
        (
            dep_each_nodes,
            dep_all_nodes,
            independent_nodes,
        ) = self.runner.group_nodes_by_execution_mode(level)

        all_dependencies = set(dep_each_nodes.keys()) | set(dep_all_nodes.keys())

        for dep_name in all_dependencies:
            each_names = dep_each_nodes.get(dep_name, [])
            all_names = dep_all_nodes.get(dep_name, [])
            self.runner.process_grouped_dependencies(
                dep_name, each_names, all_names, independent_nodes
            )

        for name in independent_nodes:
            self.runner.execute_independent_node(name)


def run(pipeline: PipelineDef, params: Any) -> None:
    """Executes a pipeline definition synchronously."""
    if getattr(pipeline, "requires_async_runner", False):
        raise RuntimeError(
            "This pipeline contains async features (async def or AsyncIterator) and must be executed with async_run()."
        )

    executor = PipelineExecutor(pipeline)
    executor.execute(params)
