"""
Provides dependency resolution capabilities for the synchronous execution engine.

This module ensures that step functions receive the correct arguments by extracting outputs
from upstream dependencies, resolving required resources, and managing their lifecycles.
"""

from collections.abc import Callable
from contextlib import ExitStack
from typing import Any

from synaflow.core.dag import Dag
from synaflow.execution.context_managers import (
    is_async_context_manager_instance,
    is_sync_context_manager_instance,
)
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.state import ExecutionState
from synaflow.execution.sync_handoff import SyncQueueIterator


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
        resource_factories: dict[str, Any],
    ):
        self._dag = dag
        self._outputs = outputs
        self._overrides = overrides
        self._resource_factories = resource_factories

    def seed_runtime_inputs(self, params: Any) -> None:
        self._outputs.seed(params)

    def resolve_materializer(self, step_name: str, node: Any) -> Any:
        if self._overrides is None:
            return node.materializer
        return self._overrides.materializers.resolve(step_name, node.materializer)

    def resolve_resource_argument(
        self, resource_name: str, resource_stack: ExitStack, is_each_mode: bool = False
    ) -> tuple[Any, Callable[[], Any] | None]:
        provider = None
        if self._overrides is not None:
            provider = self._overrides.resources.resolve(resource_name)
        if provider is None:
            provider = self._resource_factories.get(resource_name)
        if provider is None:
            raise ValueError(
                f"Pipeline '{self._dag.name}' requires resource '{resource_name}' at runtime."
            )

        factory = provider if callable(provider) else (lambda: provider)
        value = factory()
        if is_async_context_manager_instance(value):
            raise TypeError(
                f"Pipeline '{self._dag.name}': resource '{resource_name}' produced an async context manager in sync run()."
            )
        if is_sync_context_manager_instance(value):
            if is_each_mode:
                return None, factory
            return resource_stack.enter_context(value), None
        return value, None

    def build_arguments(
        self, consumer: str, node: Any, is_each_mode: bool = False
    ) -> tuple[dict[str, Any], ExitStack, dict[str, Any]]:
        resource_stack = ExitStack()
        args: dict[str, Any] = {}
        deferred_resources: dict[str, Any] = {}
        # Track SyncQueueIterators we obtained from the runtime state so we
        # can close them on failure.  Without this, an exception raised by
        # resolve_resource_argument() AFTER the iterator was fetched leaves
        # the iterator's branch in SyncFanout._active_branches.  The pump
        # thread then deadlocks on the final EOF_MARKER push for that
        # orphaned branch (see Issue #103).
        leaked_iterators: list[SyncQueueIterator] = []
        try:
            for dep_name in node.deps:
                param = node.dataset_param_names.get(dep_name, dep_name)
                if dep_name in self._dag.resource_factories:
                    value, factory = self.resolve_resource_argument(
                        dep_name, resource_stack, is_each_mode=is_each_mode
                    )
                    if factory is not None:
                        deferred_resources[param] = factory
                    else:
                        args[param] = value
                else:
                    value = self._outputs.get_output(dep_name, consumer)
                    if isinstance(value, SyncQueueIterator):
                        leaked_iterators.append(value)
                    args[param] = value
            return args, resource_stack, deferred_resources
        except Exception:
            resource_stack.close()
            for it in leaked_iterators:
                try:
                    it.close()
                except Exception:
                    pass
            raise
