from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from contextlib import AsyncExitStack
from typing import Any

from synaflow.execution.async_engine.lifecycle_stream import AsyncLifecycleStream

from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_builder import build_dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import (
    PipelineStopException,
    StepExecutionError,
    ThresholdExceededException,
)
from synaflow.core.types import OnError, StepMode
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.threshold import (
    has_threshold,
)
from synaflow.execution.state import ExecutionState

from .argument_builder import AsyncArgumentBuilder
from .constants import EOF_MARKER
from .event_dispatch import AsyncEventDispatcher
from .iterator_utils import AsyncQueueBranch
from synaflow.execution.stats import StepRunStats
from .step_runner import (
    AsyncStepRunner,
    wrap_deferred_output,
    collect_async_iterator,
)


# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


async def _pump_iterator(
    name: str,
    iterator: Any,
    queues: dict[str, Any],
    on_error: Any,
    events: AsyncEventDispatcher | None = None,
) -> None:
    try:
        async for item in _safe_iterate(name, iterator):
            for q in queues.values():
                await q.put(item)
    except StepExecutionError as e:
        cause = e.__cause__ or e
        if events is not None:
            await events.handle_error(name, cause)
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


async def _safe_iterate(name: str, iterable: Any):
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


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class AsyncPipelineExecutor:
    def __init__(
        self,
        dag: Dag,
        *,
        overrides: ExecutionOverrides | None = None,
        resource_factories: dict[str, Any] | None = None,
    ):
        self.dag = dag
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})

        self.state = ExecutionState(self.dag)
        self.scope = AsyncArgumentBuilder(
            self.dag, self.state, self._overrides, self._resource_factories
        )
        self.run_id = str(uuid.uuid4())
        self.events = AsyncEventDispatcher(self.dag, self.run_id, self._overrides)
        self._pump_tasks: list[asyncio.Task] = []

    @property
    def outputs(self) -> dict[str, Any]:
        return self.state.raw_outputs()

    async def _run_graph(self) -> None:
        running_tasks = set()
        finished_tasks = set()
        ready_tasks = set()
        fatal_error = None

        event = asyncio.Event()

        def check_new_ready_steps():
            for s in self.dag.steps:
                if (
                    s not in ready_tasks
                    and s not in running_tasks
                    and s not in finished_tasks
                ):
                    if self.state.inputs_available(s):
                        ready_tasks.add(s)

            while ready_tasks:
                s = ready_tasks.pop()
                running_tasks.add(s)
                task = asyncio.create_task(self._run_step(s))
                task.add_done_callback(lambda t, step_name=s: step_done(t, step_name))

        def step_done(task, step_name):
            nonlocal fatal_error
            running_tasks.remove(step_name)
            finished_tasks.add(step_name)
            try:
                task.result()
            except BaseException as exc:
                if fatal_error is None:
                    fatal_error = exc
                self.abort()

            if fatal_error is None:
                check_new_ready_steps()

            # Issue #103: wake the wait loop both on natural completion
            # and when fatal_error is set, so we exit even if some siblings
            # are blocked on user code we cannot cancel.
            if not running_tasks or fatal_error is not None:
                event.set()

        check_new_ready_steps()
        # Issue #103: same race-with-fatal_error fix as the sync engine:
        # if a step fails before we enter the wait, we must not block.
        if running_tasks and fatal_error is None:
            await event.wait()

        if fatal_error is not None:
            raise fatal_error

    async def execute(self, params: Any) -> None:
        self.scope.seed_runtime_inputs(params)

        await self.events.pipeline_started()
        try:
            await self._run_graph()

            await self.cleanup()
        except PipelineStopException as exc:
            await self.events.pipeline_failed(
                step_name=exc.step_name,
                exception=exc.cause or exc,
            )
            raise
        except ThresholdExceededException as exc:
            await self.events.pipeline_failed(
                step_name=exc.step_name,
                exception=exc,
            )
            raise
        except Exception as exc:
            await self.events.pipeline_failed(step_name=None, exception=exc)
            raise
        else:
            await self.events.pipeline_completed()

    async def _run_step(self, step_name: str) -> None:
        node = self.dag[step_name]
        if not node.fn:
            return

        unrolled = self.dag.each_inputs(step_name)
        resource_stack = AsyncExitStack()
        arguments = await self.scope.build_arguments(
            step_name, node, unrolled, resource_stack
        )

        stats = StepRunStats()

        upstream_max_in_flight = {}
        for dep in unrolled:
            producer_node = self.dag.get(dep)
            if producer_node is not None:
                upstream_max_in_flight[dep] = getattr(producer_node, "max_in_flight", 1)

        runner = AsyncStepRunner(
            step_name=step_name,
            fn=node.fn,
            on_error=node.on_error,
            max_in_flight=node.max_in_flight,
            dataset_param_names=node.dataset_param_names,
            arguments=arguments,
            resource_stack=resource_stack,
            is_each_mode=(node.mode == StepMode.EACH),
            should_drain=(
                node.output_contract is not None
                and node.output_contract.drain_policy != "none"
            ),
            publisher=lambda out: (
                self.publish(step_name, out, node, stats)
                if not self.dag.is_hidden_step(step_name)
                else None
            ),
            state=self.state,
            events=self.events,
            stats=stats,
            each_mode_deps=unrolled,
            dag_node=node,
            upstream_max_in_flight=upstream_max_in_flight,
        )
        await runner.run()

    # ------------------------------------------------------------------
    # Dataflow routing & publishing (formerly StreamPublisher)
    # ------------------------------------------------------------------

    def abort(self) -> None:
        """Cancel all active pump tasks."""
        for t in self._pump_tasks:
            t.cancel()

    async def cleanup(self) -> None:
        """Await all pump tasks with a bounded timeout.

        Issue #103: a pump task may be parked in ``__anext__`` blocked on
        user code (e.g. stuck ``asyncio.Queue.get()`` or ``await``).  asyncio
        ``Task.cancel()`` only schedules a ``CancelledError`` — it cannot
        interrupt arbitrary user code synchronously.  We bound the wait
        with ``asyncio.wait_for`` so cleanup() always returns; the orphan
        pump task is left to be reclaimed by the event loop.
        """
        if not self._pump_tasks:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._pump_tasks, return_exceptions=True),
                timeout=1.0,
            )
        except asyncio.TimeoutError:
            import logging

            logging.getLogger("synaflow").warning(
                "Async pump tasks did not exit within timeout; likely blocked "
                "in user code.  Abandoning the pumps; they will leak.  See "
                "Issue #103."
            )
        except Exception:
            pass

    async def _apply_materializer(
        self,
        step_name: str,
        value: Any,
        materializer: Any,
        consumer_type: Any = None,
    ) -> tuple[Any, bool, BaseException | None]:
        if materializer is None:
            if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
                node = self.dag[step_name]
                items, had_error, exc = await collect_async_iterator(
                    step_name, value, node.on_error, self.events
                )
                return items, had_error, exc
            return value, False, None

        # To preserve partial items in case the stream crashes during materialization,
        # we wrap the stream and record yielded items.
        history = []
        if self._is_stream_output(value):
            value = AsyncLifecycleStream(value, on_item=history.append)

        # Materializer is guaranteed to be async by validation.
        # It natively handles consuming the stream if needed.
        try:
            result = await materializer(value)
            return result, False, None
        except Exception as e:
            return history, True, e

    async def _materialize_with_events(
        self, step_name: str, output: Any, node: DagNode, consumer_type: Any = None
    ) -> tuple[Any, bool, BaseException | None]:
        materializer = self.scope.resolve_materializer(step_name, node)
        mat_name = materializer.__name__ if callable(materializer) else None
        await self.events.materialization_started(
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
                await self.events.materialization_failed(
                    step_name,
                    node,
                    consumer_type,
                    mat_name,
                    exception=exc,
                )
            else:
                await self.events.materialization_completed(
                    step_name,
                    node,
                    consumer_type,
                    mat_name,
                )
            return result, had_error, exc
        except PipelineStopException:
            raise
        except Exception as exc:
            await self.events.materialization_failed(
                step_name,
                node,
                consumer_type,
                mat_name,
                exception=exc,
            )
            raise

    async def _emit_step_result(
        self,
        node: DagNode,
        step_name: str,
        output: Any,
        stats: StepRunStats,
        had_error: bool,
        exception: BaseException | None = None,
    ) -> None:
        if has_threshold(node):
            return  # already dispatched by generate()
        success = len(output) if hasattr(output, "__len__") else 1
        real_error_count = stats.error_count
        real_invocation_count = (
            stats.invocation_count if stats.invocation_count > 0 else success
        )
        if had_error:
            await self.events.step_failed(
                node,
                step_name,
                success_count=success,
                error_count=max(real_error_count, 1),
                completed_all_inputs=False,
                exception=exception,
            )
        else:
            await self.events.step_completed(
                node,
                step_name,
                success_count=real_invocation_count - real_error_count,
                error_count=real_error_count,
                completed_all_inputs=True,
            )

    @staticmethod
    def _is_stream_output(output: Any) -> bool:
        return isinstance(output, (Iterator, Generator, AsyncIterator, AsyncGenerator))

    async def _publish_eager_materialized_stream(
        self,
        step_name: str,
        output: Any,
        node: DagNode,
        stats: StepRunStats,
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
            self.state.set_output(step_name, items, consumer)
        if deferred:
            await self._emit_step_result(node, step_name, items, stats, had_error, exc)

    async def _handle_stream_publish_error(
        self, step_name: str, node: DagNode, exc: Exception
    ) -> None:
        await self.events.handle_error(step_name, exc)
        if node.on_error == OnError.STOP:
            raise PipelineStopException(step_name=step_name, cause=exc) from exc

    async def _publish_stream_to_queues(
        self,
        step_name: str,
        output: Any,
        node: DagNode,
        consumers: list[str],
        deferred: bool,
    ) -> None:
        queue_maxsize = max(1, node.max_in_flight)
        queues = {
            consumer: AsyncQueueBranch(asyncio.Queue(maxsize=queue_maxsize))
            for consumer in consumers
        }
        for consumer, queue in queues.items():
            self.state.set_output(step_name, queue, consumer)
        task = asyncio.create_task(
            _pump_iterator(
                step_name,
                output,
                queues,
                node.on_error,
                self.events,
            )
        )
        self._pump_tasks.append(task)

    async def _publish_terminal_stream(
        self,
        step_name: str,
        output: Any,
        node: DagNode,
        stats: StepRunStats,
        deferred: bool,
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
        if deferred:
            await self._emit_step_result(node, step_name, output, stats, had_error, exc)

    async def _publish_scalar_output(
        self,
        step_name: str,
        output: Any,
        node: DagNode,
        stats: StepRunStats,
        deferred: bool,
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
        self.state.set_output(step_name, output)
        if deferred:
            await self._emit_step_result(node, step_name, output, stats, had_error, exc)

    async def publish(
        self, step_name: str, output: Any, node: DagNode, stats: StepRunStats
    ) -> None:
        """Publish the output of a step."""
        deferred = node.mode == StepMode.EACH or (
            node.mode == StepMode.ALL and self._is_stream_output(output)
        )

        if not self._is_stream_output(output):
            await self._publish_scalar_output(step_name, output, node, stats, deferred)
            return

        consumers = self.dag.consumers_of(step_name)

        if self.dag.needs_materialize(step_name):
            try:
                await self._publish_eager_materialized_stream(
                    step_name, output, node, stats, consumers, deferred
                )
            except PipelineStopException:
                raise
            except Exception as exc:
                await self._handle_stream_publish_error(step_name, node, exc)
            return

        if deferred:
            output = wrap_deferred_output(step_name, output, node, self.events, stats)

        if consumers:
            await self._publish_stream_to_queues(
                step_name, output, node, consumers, deferred
            )
            return

        await self._publish_terminal_stream(step_name, output, node, stats, deferred)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def async_run(
    pipeline: PipelineDef,
    params: Any,
    overrides: ExecutionOverrides | None = None,
) -> None:
    dag = build_dag(pipeline)
    if dag.requires_sync_runner:
        raise RuntimeError(
            "This pipeline contains synchronous streams (Iterator)."
            " It must be executed with run() or migrated to AsyncIterator."
        )
    await AsyncPipelineExecutor(
        dag,
        overrides=overrides,
        resource_factories=dag.resource_factories,
    ).execute(params)
