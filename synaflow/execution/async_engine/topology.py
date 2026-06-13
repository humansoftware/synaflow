import asyncio
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from typing import Any

from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException, StepExecutionError
from synaflow.core.types import MaterializeContext, OnError

from .constants import EOF_MARKER
from .iterator_utils import async_list


class AsyncTeeWrapper:
    def __init__(self, queues: dict[str, asyncio.Queue]):
        self.queues = queues


class AsyncStreamManager:
    def __init__(
        self,
        pipeline: PipelineDef,
        context: dict[str, Any],
        pump_tasks: list[asyncio.Task],
    ):
        self.pipeline = pipeline
        self.dag = pipeline.dag
        self.context = context
        self.pump_tasks = pump_tasks

    async def apply_materializer(self, name: str, iterator: Any) -> Any:
        node = self.dag.get(name, {})
        mat = node.get("materializer")

        if mat is None:
            return await async_list(iterator)

        sig = inspect.signature(mat)
        if (
            len(sig.parameters) > 1
            or "ctx" in sig.parameters
            or "context" in sig.parameters
        ):
            ctx = MaterializeContext(
                pipeline_name=self.dag.name,
                dataset_name=name,
                item_type=node.get("output") if node else Any,
            )
            mat = mat(ctx)

        if inspect.iscoroutinefunction(mat):
            return await mat(iterator)

        if isinstance(iterator, (AsyncIterator, AsyncGenerator)):
            return await async_list(iterator)

        return mat(iterator)

    async def pump_iterator(
        self,
        name: str,
        iterator: Any,
        queues: dict[str, asyncio.Queue],
        needs_materialize: bool = False,
        on_error: Any = None,
    ) -> None:
        try:
            safe_iterator = self._safe_iterate(name, iterator)
            if needs_materialize:
                items = await self.apply_materializer(name, safe_iterator)
                async for item in self._safe_iterate(name, items):
                    for q in queues.values():
                        await q.put(item)
            else:
                async for item in safe_iterator:
                    for q in queues.values():
                        await q.put(item)
        except StepExecutionError as e:
            if on_error == OnError.STOP:
                for q in queues.values():
                    await q.put(PipelineStopException())
                raise PipelineStopException() from e
            for q in queues.values():
                await q.put(e)
        finally:
            for q in queues.values():
                await q.put(EOF_MARKER)

    async def _safe_iterate(self, name: str, iterable: Any):
        if isinstance(iterable, (AsyncIterator, AsyncGenerator)):
            while True:
                try:
                    item = await anext(iterable)
                    yield item
                except StopAsyncIteration:
                    break
                except Exception as e:
                    if isinstance(e, StepExecutionError):
                        raise e
                    raise StepExecutionError(f"Error iterating step '{name}'") from e
        else:
            iterator = iter(iterable)
            while True:
                try:
                    item = next(iterator)
                    yield item
                except StopIteration:
                    break
                except Exception as e:
                    if isinstance(e, StepExecutionError):
                        raise e
                    raise StepExecutionError(f"Error iterating step '{name}'") from e

    def store_output(
        self, name: str, value: Any, needs_materialize: bool = False
    ) -> None:
        consumers = [
            c for c, cnode in self.dag.items() if name in cnode.get("deps", {})
        ]

        if isinstance(value, (Iterator, Generator, AsyncIterator, AsyncGenerator)):
            if consumers:
                queues = {c: asyncio.Queue(maxsize=100) for c in consumers}
                self.context[name] = AsyncTeeWrapper(queues)
                node = self.dag.get(name, {})
                on_error = node.get("on_error")
                task = asyncio.create_task(
                    self.pump_iterator(name, value, queues, needs_materialize, on_error)
                )
                self.pump_tasks.append(task)
            else:
                self.context[name] = value
        else:
            self.context[name] = value
