"""
Async dependency resolution logic.

Resolves dependencies, resource arguments, and materializers for the async engine.
"""

import asyncio
import dataclasses
import inspect
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import AsyncExitStack
from typing import Any

from synaflow.core.dag import Dag
from synaflow.execution.overrides import ExecutionOverrides

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


class AsyncDependencyResolver:
    """Resolves dependencies, resource arguments, and materializers for the async engine."""

    def __init__(
        self,
        dag: Dag,
        outputs: dict[str, Any],
        overrides: ExecutionOverrides | None = None,
        resource_factories: dict[str, Any] | None = None,
    ):
        """Initialize the dependency resolver."""
        self.dag = dag
        self.outputs = outputs
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})

    def seed_runtime_inputs(self, params: Any) -> None:
        """Seed initial runtime inputs into the outputs dictionary."""
        if dataclasses.is_dataclass(params):
            param_dict = {
                f.name: getattr(params, f.name) for f in dataclasses.fields(params)
            }
        else:
            param_dict = params._asdict()
        for field, value in param_dict.items():
            self.outputs[field] = value

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
                f"Pipeline '{self.dag.name}' requires resource '{resource_name}' at runtime."
            )

        value = provider() if callable(provider) else provider
        if inspect.isawaitable(value):
            value = await value
        if hasattr(value, "__aenter__") and hasattr(value, "__aexit__"):
            return await resource_stack.enter_async_context(value)
        if hasattr(value, "__enter__") and hasattr(value, "__exit__"):
            return resource_stack.enter_context(value)
        return value

    async def build_arguments(
        self,
        consumer: str,
        node: Any,
        unrolled: set[str] | list[str],
        resource_stack: AsyncExitStack,
    ) -> dict[str, Any]:
        """Build argument dictionary for a step."""
        args = {}
        for dep_name in node.deps:
            if dep_name in self.dag.resources:
                value = await self.resolve_resource_argument(dep_name, resource_stack)
            else:
                key = self.dag.output_key(dep_name, consumer)
                value = self.outputs.get(key, self.outputs.get(dep_name))
                if (
                    isinstance(value, (asyncio.Queue, AsyncQueueBranch))
                    and dep_name not in unrolled
                ):
                    value = await _resolve_queue(value)
                if isinstance(value, (list, tuple, set)) and dep_name not in unrolled:
                    dep_type = node.deps.get(dep_name)
                    origin = getattr(dep_type, "__origin__", dep_type)
                    if origin in (AsyncIterator, AsyncGenerator):
                        value = _list_to_async_gen(value)
            param = node.dataset_param_names.get(dep_name, dep_name)
            args[param] = value
        return args

    def attach_cleanup(self, output: Any, arguments: dict[str, Any]) -> Any:
        """Attach argument cleanup to a generator output."""
        if not isinstance(output, (AsyncIterator, AsyncGenerator)):
            return output

        async def wrapped():
            try:
                async for item in output:
                    yield item
            finally:
                await self.close_stream_arguments(arguments)

        return wrapped()

    async def close_stream_arguments(self, arguments: dict[str, Any]) -> None:
        """Close input queues and generators after execution."""
        for value in arguments.values():
            if isinstance(value, AsyncQueueBranch):
                value.close()
                continue
            if inspect.isasyncgen(value):
                try:
                    await value.aclose()
                except Exception:
                    pass
