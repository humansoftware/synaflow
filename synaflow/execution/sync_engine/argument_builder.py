"""
Provides dependency resolution capabilities for the synchronous execution engine.

This module ensures that step functions receive the correct arguments by extracting outputs
from upstream dependencies, resolving required resources, and managing their lifecycles.
"""

from contextlib import ExitStack
from typing import Any

from synaflow.core.dag import Dag
from synaflow.execution.context_managers import (
    is_async_context_manager_instance,
    is_sync_context_manager_instance,
)
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.state import ExecutionState


class ArgumentBuilder:
    """
    Resolves inputs, resources, and materializers for a pipeline step during execution.

    The ArgumentBuilder bridges the static pipeline definition (DAG) with the runtime
    execution context. It is responsible for:
    - Seeding initial runtime inputs into the execution context.
    - Resolving and applying step materializers, respecting execution overrides.
    - Instantiating and managing the lifecycle of resources (via context managers).
    - Building the argument dictionary for step function invocation.
    - Ensuring managed streams are properly closed during cleanup.
    """

    def __init__(
        self,
        dag: Dag,
        outputs: ExecutionState,
        overrides: ExecutionOverrides | None,
        resource_instances: dict[str, Any],
    ):
        self._dag = dag
        self._outputs = outputs
        self._overrides = overrides
        self._resource_instances = resource_instances

    def seed_runtime_inputs(self, params: Any) -> None:
        self._outputs.seed(params)

    def resolve_materializer(self, step_name: str, node: Any) -> Any:
        if self._overrides is None:
            return node.materializer
        return self._overrides.materializers.resolve(step_name, node.materializer)

    def resolve_resource_argument(self, resource_name: str, resource_stack: ExitStack):
        provider = None
        if self._overrides is not None:
            provider = self._overrides.resources.resolve(resource_name)

        if provider is not None:
            # Runtime override: may be a factory (callable) or a plain value.
            # callable() returns True for context managers and MagicMocks too,
            # but those are valid as plain overrides — the user explicitly set them.
            value = provider() if callable(provider) else provider
        else:
            # Base resource instance resolved at design time.
            # The factory has already been called; never call it again.
            value = self._resource_instances.get(resource_name)
            if value is None:
                raise ValueError(
                    f"Pipeline '{self._dag.name}' requires resource '{resource_name}' at runtime."
                )

        if is_async_context_manager_instance(value):
            raise TypeError(
                f"Pipeline '{self._dag.name}': resource '{resource_name}' produced an async context manager in sync run()."
            )
        if is_sync_context_manager_instance(value):
            return resource_stack.enter_context(value)
        return value

    def build_arguments(self, consumer, node) -> tuple[dict[str, Any], ExitStack]:
        resource_stack = ExitStack()
        args = {}
        try:
            for dep_name in node.deps:
                if dep_name in self._dag.resources:
                    value = self.resolve_resource_argument(dep_name, resource_stack)
                else:
                    value = self._outputs.get_output(dep_name, consumer)
                param = node.dataset_param_names.get(dep_name, dep_name)
                args[param] = value
            return args, resource_stack
        except Exception:
            resource_stack.close()
            raise
