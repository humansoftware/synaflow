"""
Async dependency resolution logic.

Resolves dependencies, resource arguments, and materializers for the async engine.
"""

import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import AsyncExitStack
from typing import Any

from synaflow.core.dag import Dag
from synaflow.execution.context_managers import (
    is_async_context_manager_instance,
    is_sync_context_manager_instance,
)
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.state import ExecutionState

from .iterator_utils import AsyncQueueBranch, queue_to_async_gen


async def _resolve_queue(
    queue: asyncio.Queue,
) -> Any:
    if isinstance(queue, AsyncQueueBranch):
        return queue
    return queue_to_async_gen(queue)


async def _list_to_async_gen(lst):
    for item in lst:
        yield item


class AsyncArgumentBuilder:
    """Resolves dependencies, resource arguments, and materializers for the async engine."""

    def __init__(
        self,
        dag: Dag,
        outputs: ExecutionState,
        overrides: ExecutionOverrides | None = None,
        resource_factories: dict[str, Any] | None = None,
    ):
        """Initialize the dependency resolver."""
        self._dag = dag
        self._outputs = outputs
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})

    def seed_runtime_inputs(self, params: Any) -> None:
        """Seed initial runtime inputs into the outputs dictionary."""
        self._outputs.seed(params)

    def resolve_materializer(self, step_name: str, node: Any) -> Any:
        """Resolve the materializer for a step."""
        if self._overrides is None:
            return node.materializer
        return self._overrides.materializers.resolve(step_name, node.materializer)

    async def resolve_resource_argument(
        self, resource_name: str, resource_stack: AsyncExitStack
    ) -> Any:
        """Resolve a runtime resource argument."""
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
        if inspect.isawaitable(value):
            value = await value
        if is_async_context_manager_instance(value):
            return await resource_stack.enter_async_context(value)
        if is_sync_context_manager_instance(value):
            return resource_stack.enter_context(value)
        return value

    async def build_arguments(
        self,
        consumer: str,
        node: Any,
        unrolled: set[str] | list[str],
        resource_stack: AsyncExitStack,
    ) -> dict[str, Any]:
        """Build argument dictionary for a step.

        On exception, any ``AsyncQueueBranch`` opened earlier in this call
        is closed to release its slot — mirroring the sync engine's
        leaked-iterator fix (Issue #103).  In the async engine the pump
        still pushes ``EOF_MARKER`` in its ``finally``, so the branch does
        not deadlock, but closing it eagerly avoids leaking a half-open
        slot into ``state.outputs``.
        """
        args: dict[str, Any] = {}
        # Track AsyncQueueBranch objects we touched so we can close them on
        # failure.  See Issue #103 (sync_engine/argument_builder.py).
        opened_branches: list[AsyncQueueBranch] = []
        try:
            for dep_name in node.deps:
                if dep_name in self._dag.resource_factories:
                    value = await self.resolve_resource_argument(
                        dep_name, resource_stack
                    )
                else:
                    value = self._outputs.get_output(dep_name, consumer)
                    if (
                        isinstance(value, (asyncio.Queue, AsyncQueueBranch))
                        and dep_name not in unrolled
                    ):
                        if isinstance(value, AsyncQueueBranch):
                            opened_branches.append(value)
                        value = await _resolve_queue(value)
                    if (
                        isinstance(value, (list, tuple, set))
                        and dep_name not in unrolled
                    ):
                        dep_type = node.deps.get(dep_name)
                        origin = getattr(dep_type, "__origin__", dep_type)
                        if origin in (AsyncIterator, AsyncGenerator):
                            value = _list_to_async_gen(value)
                param = node.dataset_param_names.get(dep_name, dep_name)
                args[param] = value
            return args
        except Exception:
            for branch in opened_branches:
                try:
                    branch.close()
                except Exception:
                    pass
            raise
