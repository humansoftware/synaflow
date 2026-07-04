"""
Provides dependency resolution capabilities for the synchronous execution engine.

This module ensures that step functions receive the correct arguments by extracting outputs
from upstream dependencies, resolving required resources, and managing their lifecycles.
"""

import dataclasses
from collections.abc import Iterator
from contextlib import ExitStack
from typing import Any

from synaflow.core.dag import Dag
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.sync_handoff import SyncQueueIterator


class DependencyResolver:
    """
    Resolves inputs, resources, and materializers for a pipeline step during execution.

    The DependencyResolver bridges the static pipeline definition (DAG) with the runtime
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
        outputs: dict,
        overrides: ExecutionOverrides | None,
        resource_factories: dict[str, Any],
    ):
        self._dag = dag
        self._outputs = outputs
        self._overrides = overrides
        self._resource_factories = resource_factories

    def seed_runtime_inputs(self, params: Any) -> None:
        if dataclasses.is_dataclass(params):
            param_dict = {
                f.name: getattr(params, f.name) for f in dataclasses.fields(params)
            }
        else:
            param_dict = params._asdict()
        for field, value in param_dict.items():
            self._outputs[field] = value

    def resolve_materializer(self, step_name: str, node: Any) -> Any:
        if self._overrides is None:
            return node.materializer
        return self._overrides.materializers.resolve(step_name, node.materializer)

    def resolve_resource_argument(self, resource_name: str, resource_stack: ExitStack):
        provider = None
        if self._overrides is not None:
            provider = self._overrides.resources.resolve(resource_name)
        if provider is None:
            provider = self._resource_factories.get(resource_name)
        if provider is None:
            raise ValueError(
                f"Pipeline '{self._dag.name}' requires resource '{resource_name}' at runtime."
            )

        value = provider() if callable(provider) else provider
        if hasattr(value, "__aenter__") and hasattr(value, "__aexit__"):
            raise TypeError(
                f"Pipeline '{self._dag.name}': resource '{resource_name}' produced an async context manager in sync run()."
            )
        if hasattr(value, "__enter__") and hasattr(value, "__exit__"):
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
                    key = self._dag.output_key(dep_name, consumer)
                    value = self._outputs.get(key, self._outputs.get(dep_name))
                param = node.dataset_param_names.get(dep_name, dep_name)
                args[param] = value
            return args, resource_stack
        except Exception:
            resource_stack.close()
            raise

    def attach_cleanup(self, output, arguments):
        if not isinstance(output, Iterator):
            return output

        def wrapped():
            try:
                yield from output
            finally:
                self.close_managed_streams(arguments)

        return wrapped()

    def close_managed_streams(self, arguments):
        for value in arguments.values():
            if isinstance(value, SyncQueueIterator):
                try:
                    value.close()
                except Exception:
                    pass
