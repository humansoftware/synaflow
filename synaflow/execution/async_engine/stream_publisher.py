"""
Async stream publication logic.

Publishes stream outputs, applies materialization, and manages fan-out for the async engine.
"""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.exceptions import (
    PipelineStopException,
    StepExecutionError,
    ThresholdExceededException,
)
from synaflow.core.types import OnError, StepMode
from synaflow.execution.threshold import has_threshold
from .constants import EOF_MARKER
from .iterator_utils import AsyncQueueBranch
from .event_dispatch import AsyncEventDispatcher
from .argument_builder import AsyncArgumentBuilder


class AsyncStreamPublisher:
    """Publishes stream outputs, applies materialization, and manages fan-out."""

    def __init__(
        self,
        dag: Dag,
        outputs: dict[str, Any],
        events: AsyncEventDispatcher,
        step_output_observers: list,
        scope: AsyncArgumentBuilder,
    ):
        self.dag = dag
        self.outputs = outputs
        self._events = events
        self._step_output_observers = step_output_observers
        self.scope = scope
        self._pump_tasks: list[asyncio.Task] = []

    def abort(self) -> None:
        """Cancel all active pump tasks."""
        for t in self._pump_tasks:
            t.cancel()

    async def cleanup(self) -> None:
        """Await all pump tasks, suppressing exceptions."""
        if self._pump_tasks:
            try:
                await asyncio.gather(*self._pump_tasks, return_exceptions=True)
            except Exception:
                pass

    @staticmethod
    async def _list_to_async_gen(items: list[Any]) -> AsyncGenerator[Any, None]:
        for item in items:
            yield item

    async def _collect_async_iterator(
        self,
        step_name: str,
        value: Any,
    ) -> tuple[list[Any], bool, BaseException | None]:
        items = []
        try:
            if isinstance(value, (AsyncIterator, AsyncGenerator)):
                while True:
                    try:
                        item = await anext(value)
                        items.append(item)
                    except StopAsyncIteration:
                        break
            else:
                iterator = iter(value)
                while True:
                    try:
                        item = next(iterator)
                        items.append(item)
                    except StopIteration:
                        break
        except BaseException as exc:
            await self._events.handle_error(
                step_name,
                exc,
                success_count=len(items),
                error_count=1,
                completed_all_inputs=False,
            )
            if self.dag[step_name].on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
            return items, True, exc
        return items, False, None

    async def _apply_materializer(
        self,
        step_name: str,
        value: Any,
        materializer: Any,
        consumer_type: Any = None,
    ) -> tuple[Any, bool, BaseException | None]:
        if materializer is None:
            if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
                items, had_error, exc = await self._collect_async_iterator(
                    step_name, value
                )
                return items, had_error, exc
            return value, False, None

        # Materializer is guaranteed to be async by validation.
        # It natively handles consuming the stream if needed.
        try:
            result = await materializer(value)
            return result, False, None
        except Exception as e:
            return None, True, e

    async def _pump_iterator(
        self,
        name: str,
        iterator: Any,
        queues: dict[str, Any],
        on_error: Any,
    ) -> None:
        try:
            async for item in self._safe_iterate(name, iterator):
                for q in queues.values():
                    await q.put(item)
        except StepExecutionError as e:
            cause = e.__cause__ or e
            await self._events.handle_error(name, cause)
            if isinstance(cause, ThresholdExceededException):
                for q in queues.values():
                    await q.put(cause)
                raise PipelineStopException(step_name=name) from e
            if on_error == OnError.STOP:
                for q in queues.values():
                    await q.put(PipelineStopException(step_name=name))
                raise PipelineStopException(step_name=name) from e
        finally:
            for q in queues.values():
                await q.put(EOF_MARKER)

    async def _pump_observer(
        self, name: str, queue: asyncio.Queue, observer: Any
    ) -> None:
        items = []
        while True:
            item = await queue.get()
            if item is EOF_MARKER:
                break
            if isinstance(item, Exception):
                break
            items.append(item)
        observer(name, items)

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

    def _notify_observers(self, step_name: str, output: Any) -> None:
        for observer in self._step_output_observers:
            observer(step_name, output)

    async def _materialize_with_events(
        self, step_name: str, output: Any, node: Any, consumer_type: Any = None
    ) -> tuple[Any, bool, BaseException | None]:
        materializer = self.scope.resolve_materializer(step_name, node)
        mat_name = materializer.__name__ if callable(materializer) else None
        await self._events.materialization_started(
            step_name,
            node,
            consumer_type,
            mat_name,
        )
        try:
            result, had_error, exc = await self._apply_materializer(
                step_name,
                output,
                materializer,
                consumer_type=consumer_type,
            )
            if had_error:
                await self._events.materialization_failed(
                    step_name,
                    node,
                    consumer_type,
                    mat_name,
                    exception=exc,
                )
            else:
                await self._events.materialization_completed(
                    step_name,
                    node,
                    consumer_type,
                    mat_name,
                )
            return result, had_error, exc
        except PipelineStopException:
            raise
        except Exception as exc:
            await self._events.materialization_failed(
                step_name,
                node,
                consumer_type,
                mat_name,
                exception=exc,
            )
            raise

    async def _emit_step_result(
        self,
        node: Any,
        step_name: str,
        output: Any,
        had_error: bool,
        exception: BaseException | None = None,
    ) -> None:
        if has_threshold(node):
            return  # already dispatched by generate()
        success = len(output) if hasattr(output, "__len__") else 1
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", success)
        if had_error:
            await self._events.step_failed(
                node,
                step_name,
                success_count=success,
                error_count=max(real_error_count, 1),
                completed_all_inputs=False,
                exception=exception,
            )
        else:
            await self._events.step_completed(
                node,
                step_name,
                success_count=real_invocation_count - real_error_count,
                error_count=real_error_count,
                completed_all_inputs=True,
            )

    async def _emit_deferred_completion(self, node: Any, step_name: str) -> None:
        if has_threshold(node):
            return  # already dispatched by generate()
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", 0)
        await self._events.step_completed(
            node,
            step_name,
            success_count=real_invocation_count - real_error_count,
            error_count=real_error_count,
            completed_all_inputs=True,
        )

    def _wrap_deferred_output(self, step_name: str, output: Any, node: Any) -> Any:
        if has_threshold(node):
            return output

        if isinstance(output, (AsyncIterator, AsyncGenerator)):

            async def wrapped_async():
                yielded_count = 0
                async for item in output:
                    yielded_count += 1
                    yield item

                if node.mode == StepMode.ALL:
                    node._runtime_invocation_count = yielded_count
                    node._runtime_error_count = 0
                await self._emit_deferred_completion(node, step_name)

            return wrapped_async()

        async def wrapped_sync():
            yielded_count = 0
            for item in output:
                yielded_count += 1
                yield item

            if node.mode == StepMode.ALL:
                node._runtime_invocation_count = yielded_count
                node._runtime_error_count = 0
            await self._emit_deferred_completion(node, step_name)

        return wrapped_sync()

    @staticmethod
    def _is_stream_output(output: Any) -> bool:
        return isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator))

    async def _publish_eager_materialized_stream(
        self,
        step_name: str,
        output: Any,
        node: Any,
        consumers: list[str],
        deferred: bool,
    ) -> None:
        consumer_type = None
        if consumers:
            consumer_type = self.dag[consumers[0]].deps.get(step_name)
        items, had_error, exc = await self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        if had_error:
            await self._handle_stream_publish_error(step_name, node, exc)
        for consumer in consumers:
            self.outputs[self.dag.output_key(step_name, consumer)] = items
        self._notify_observers(step_name, items)
        if deferred:
            await self._emit_step_result(node, step_name, items, had_error, exc)

    async def _handle_stream_publish_error(
        self, step_name: str, node: Any, exc: Exception
    ) -> None:
        await self._events.handle_error(step_name, exc)
        if node.on_error == OnError.STOP:
            raise PipelineStopException(step_name=step_name, cause=exc) from exc

    def _register_observer_pumps(self, step_name: str, queues: dict[str, Any]) -> None:
        if not self._step_output_observers:
            return
        for observer in self._step_output_observers:
            obs_queue = asyncio.Queue(maxsize=100)
            queues["__obs"] = obs_queue
            self._pump_tasks.append(
                asyncio.create_task(self._pump_observer(step_name, obs_queue, observer))
            )

    async def _publish_stream_to_queues(
        self,
        step_name: str,
        output: Any,
        node: Any,
        consumers: list[str],
        deferred: bool,
    ) -> None:
        queue_maxsize = max(1, node.max_in_flight)
        queues = {
            consumer: AsyncQueueBranch(asyncio.Queue(maxsize=queue_maxsize))
            for consumer in consumers
        }
        for consumer, queue in queues.items():
            self.outputs[self.dag.output_key(step_name, consumer)] = queue
        self._register_observer_pumps(step_name, queues)
        task = asyncio.create_task(
            self._pump_iterator(
                step_name,
                output,
                queues,
                node.on_error,
            )
        )
        self._pump_tasks.append(task)

    async def _publish_terminal_stream(
        self, step_name: str, output: Any, node: Any, deferred: bool
    ) -> None:
        if self.dag.needs_materialize(step_name):
            output, had_error, exc = await self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
            if had_error:
                await self._handle_stream_publish_error(step_name, node, exc)
        elif self._step_output_observers:
            output, had_error, exc = await self._collect_async_iterator(
                step_name, output
            )
            # _collect_async_iterator already calls _events.handle_error and raises if STOP
        else:
            self._notify_observers(step_name, output)
            had_error = False
            exc = None
        if self._step_output_observers:
            self._notify_observers(step_name, output)
        if deferred:
            await self._emit_step_result(node, step_name, output, had_error, exc)

    async def _publish_scalar_output(
        self, step_name: str, output: Any, node: Any, deferred: bool
    ) -> None:
        if self.dag.needs_materialize(step_name):
            output, had_error, exc = await self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
            if had_error:
                await self._handle_stream_publish_error(step_name, node, exc)
        else:
            had_error = False
            exc = None
        self.outputs[step_name] = output
        self._notify_observers(step_name, output)
        if deferred:
            await self._emit_step_result(node, step_name, output, had_error, exc)

    async def publish(self, step_name: str, output: Any, node: Any) -> None:
        """Publish the output of a step."""
        deferred = node.mode == StepMode.EACH or (
            node.mode == StepMode.ALL and self._is_stream_output(output)
        )

        if not self._is_stream_output(output):
            await self._publish_scalar_output(step_name, output, node, deferred)
            return

        consumers = self.dag.consumers_of(step_name)

        if self.dag.needs_materialize(step_name):
            try:
                await self._publish_eager_materialized_stream(
                    step_name, output, node, consumers, deferred
                )
            except PipelineStopException:
                raise
            except Exception as exc:
                await self._handle_stream_publish_error(step_name, node, exc)
            return

        if deferred:
            output = self._wrap_deferred_output(step_name, output, node)

        if consumers:
            await self._publish_stream_to_queues(
                step_name, output, node, consumers, deferred
            )
            return

        await self._publish_terminal_stream(step_name, output, node, deferred)
