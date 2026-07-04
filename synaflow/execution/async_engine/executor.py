from __future__ import annotations
import asyncio
import inspect
import uuid
from contextlib import AsyncExitStack
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import (
    PipelineStopException,
    ThresholdExceededException,
)
from .event_dispatch import AsyncEventDispatcher
from synaflow.core.types import (
    OnError,
)
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.threshold import (
    check_threshold,
    wrap_threshold_raise_if_manual,
    compute_completed_all_inputs_for_all,
    has_threshold,
)

from .constants import EOF_MARKER
from .iterator_utils import AsyncQueueBranch
from .dependency_resolver import AsyncDependencyResolver


# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


async def _pump_iterator(*args, **kwargs):
    from .stream_publisher import AsyncStreamPublisher

    class MockEvents:
        async def handle_error(self, *a, **kw):
            pass

    pub = AsyncStreamPublisher(None, None, MockEvents(), [], None)
    return await pub._pump_iterator(*args, **kwargs)


async def _wrap_started_stream(it: Any, fire_started: Any) -> Any:
    if isinstance(it, (AsyncIterator, AsyncGenerator)):
        try:
            async for item in it:
                await fire_started()
                yield item
        finally:
            await fire_started()
    else:
        iterator = iter(it)
        try:
            while True:
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                await fire_started()
                yield item
        finally:
            await fire_started()


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


class AsyncPipelineExecutor:
    def __init__(
        self,
        dag: Dag,
        *,
        step_output_observers: list = None,
        overrides: ExecutionOverrides | None = None,
        resource_factories: dict[str, Any] | None = None,
    ):
        self.dag = dag
        self.outputs = {}
        self._step_output_observers = step_output_observers or []
        self._overrides = overrides
        self._resource_factories = dict(resource_factories or {})

        self.scope = AsyncDependencyResolver(
            self.dag, self.outputs, self._overrides, self._resource_factories
        )
        self.run_id = str(uuid.uuid4())
        self.events = AsyncEventDispatcher(self.dag, self.run_id, self._overrides)
        from .stream_publisher import AsyncStreamPublisher

        self.publisher = AsyncStreamPublisher(
            self.dag,
            self.outputs,
            self.events,
            self._step_output_observers,
            self.scope,
        )

    def _step_inputs_available(self, step_name: str) -> bool:
        node = self.dag[step_name]
        for dep_name in node.deps:
            if dep_name in self.dag.resources:
                continue
            key = self.dag.output_key(dep_name, step_name)
            if key not in self.outputs and dep_name not in self.outputs:
                return False
        return True

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
                    if self._step_inputs_available(s):
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
                self.publisher.abort()

            if fatal_error is None:
                check_new_ready_steps()

            if not running_tasks:
                event.set()

        check_new_ready_steps()
        if running_tasks:
            await event.wait()

        if fatal_error is not None:
            raise fatal_error

    async def execute(self, params: Any) -> None:
        self.scope.seed_runtime_inputs(params)

        await self.events.pipeline_started()
        try:
            await self._run_graph()

            await self.publisher.cleanup()
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
        await self.events.step_started(node, step_name)

        started = False

        async def fire_started():
            nonlocal started
            if not started:
                await self.events.step_started(node, step_name)
                started = True

        try:
            if (
                not unrolled
                and not inspect.isasyncgenfunction(node.fn)
                and not inspect.isgeneratorfunction(node.fn)
            ):
                await fire_started()
            output = await self._execute_step(step_name, node, arguments, unrolled)
            if self.publisher._is_stream_output(output):
                output = _wrap_started_stream(output, fire_started)
            output = self.scope.attach_cleanup(output, arguments)
            await self._emit_immediate_completion(step_name, node, output, unrolled)
            if not self.dag.is_hidden_step(step_name):
                await self.publisher.publish(step_name, output, node)
        except PipelineStopException as exc:
            await self._dispatch_step_failure(node, step_name, exc.cause or exc)
            raise
        except ThresholdExceededException as exc:
            if exc.step_name != step_name:
                # Upstream threshold propagating through this consumer:
                # the producer's generate() already dispatched FAILED.
                pass
            elif unrolled and has_threshold(node):
                # This step's generate() already dispatched FAILED (path A).
                pass
            elif not unrolled:
                # ALL-mode manual raise by this step (path B, escape hatch)
                completed_all_inputs = compute_completed_all_inputs_for_all(
                    node, arguments, exc
                )
                await self.events.handle_error(
                    step_name,
                    exc,
                    success_count=exc.success_count,
                    error_count=exc.error_count,
                    completed_all_inputs=completed_all_inputs,
                )
                await self._dispatch_step_failure(
                    node,
                    step_name,
                    exc,
                    success_count=exc.success_count,
                    error_count=exc.error_count,
                    completed_all_inputs=completed_all_inputs,
                )
            else:
                # EACH mode, no threshold configured (should not reach here
                # per build-time validation, but handle defensively)
                await self._dispatch_step_failure(
                    node,
                    step_name,
                    exc,
                    success_count=exc.success_count,
                    error_count=exc.error_count,
                    completed_all_inputs=True,
                )
            raise
        except Exception as exc:
            await self.events.handle_error(step_name, exc)
            await self._dispatch_step_failure(node, step_name, exc)
            if node.on_error == OnError.STOP:
                raise PipelineStopException(step_name=step_name, cause=exc) from exc
        finally:
            if "output" not in locals() or not self.publisher._is_stream_output(output):
                await self.scope.close_stream_arguments(arguments)
            await resource_stack.aclose()

    async def _execute_step(self, step_name, node, arguments, unrolled):
        if unrolled:
            return await self._unroll_step(step_name, node, arguments, unrolled)
        return await self._call_fn(node.fn, arguments)

    async def _emit_immediate_completion(self, step_name, node, output, unrolled):
        if unrolled or isinstance(
            output, (Iterator, Generator, AsyncIterator, AsyncGenerator)
        ):
            return
        success_count = 1
        if isinstance(output, (list, tuple, set)):
            success_count = len(output)
        await self.events.step_completed(
            node,
            step_name,
            success_count=success_count,
            error_count=0,
            completed_all_inputs=True,
        )

    async def _dispatch_step_failure(
        self,
        node,
        step_name,
        exception,
        success_count: int = 0,
        error_count: int = 1,
        completed_all_inputs: bool = False,
    ):
        cause = exception
        if isinstance(cause, PipelineStopException):
            cause = cause.cause or cause
        await self.events.step_failed(
            node,
            step_name,
            success_count=success_count,
            error_count=error_count,
            completed_all_inputs=completed_all_inputs,
            exception=cause,
        )

    async def _call_fn(self, fn: Any, kwargs: dict) -> Any:
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        return fn(**kwargs)

    async def _unroll_step(self, step_name, node, base_args, unrolled):
        queues = {}
        for dep in unrolled:
            key = self.dag.output_key(dep, step_name)
            value = self.outputs.get(key, self.outputs.get(dep))
            if isinstance(value, (asyncio.Queue, AsyncQueueBranch)):
                queues[dep] = value
            else:
                producer_node = self.dag.get(dep)
                # Non-queue inputs are already fully available in memory, so
                # max_in_flight does not apply here. Size the queue to avoid
                # deadlocking while preloading eager values for EACH-mode use.
                if isinstance(value, (list, tuple, set)):
                    q = asyncio.Queue(maxsize=max(1, len(value)) + 1)
                else:
                    maxsize = 2
                    if producer_node is not None:
                        maxsize = max(2, getattr(producer_node, "max_in_flight", 1) + 1)
                    q = asyncio.Queue(maxsize=maxsize)
                if isinstance(value, (list, tuple, set)):
                    for item in value:
                        await q.put(item)
                elif value is not None:
                    await q.put(value)
                await q.put(EOF_MARKER)
                queues[dep] = q
        completed = set()

        async def generate():
            invocation_count = 0
            error_count = 0
            # Reset runtime stats on the node so multiple executor runs
            # on the same pipeline don't leak counts across runs.
            node._runtime_error_count = 0
            node._runtime_invocation_count = 0
            try:
                while len(completed) < len(unrolled):
                    item_args = dict(base_args)
                    for dep in unrolled:
                        if dep in completed:
                            param = node.dataset_param_names.get(dep, dep)
                            item_args[param] = None
                            continue

                        item = await queues[dep].get()
                        if item is EOF_MARKER:
                            completed.add(dep)
                            param = node.dataset_param_names.get(dep, dep)
                            item_args[param] = None
                        elif isinstance(item, Exception):
                            raise item
                        else:
                            param = node.dataset_param_names.get(dep, dep)
                            item_args[param] = item
                    if len(completed) == len(unrolled):
                        break
                    invocation_count += 1
                    try:
                        yield await self._call_fn(node.fn, item_args)
                    except PipelineStopException:
                        # Propagate STOP from upstream producer so the consumer
                        # also stops, even without forced materialization.
                        raise
                    except Exception as exc:
                        error_count += 1
                        await self.events.handle_error(
                            step_name,
                            wrap_threshold_raise_if_manual(exc, step_name),
                            success_count=invocation_count - error_count,
                            error_count=error_count,
                            completed_all_inputs=False,
                        )
                        if node.on_error == OnError.STOP:
                            raise PipelineStopException(
                                step_name=step_name, cause=exc
                            ) from exc
                # pos-loop, before generator ends
                if has_threshold(node):
                    try:
                        check_threshold(step_name, node, invocation_count, error_count)
                    except ThresholdExceededException as exc:
                        await self._dispatch_step_failure(
                            node,
                            step_name,
                            exc,
                            success_count=exc.success_count,
                            error_count=exc.error_count,
                            completed_all_inputs=True,
                        )
                        raise
                    success_count = invocation_count - error_count
                    await self.events.step_completed(
                        node,
                        step_name,
                        success_count=success_count,
                        error_count=error_count,
                        completed_all_inputs=True,
                    )
                else:
                    check_threshold(step_name, node, invocation_count, error_count)
            finally:
                node._runtime_error_count = error_count
                node._runtime_invocation_count = invocation_count

        if self.dag.is_terminal_step(step_name):
            async for _ in generate():
                pass
            return None
        return generate()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def async_run(
    pipeline: PipelineDef,
    params: Any,
    overrides: ExecutionOverrides | None = None,
) -> None:
    if getattr(pipeline, "requires_sync_runner", False):
        raise RuntimeError(
            "This pipeline contains synchronous streams (Iterator)."
            " It must be executed with run() or migrated to AsyncIterator."
        )
    await AsyncPipelineExecutor(
        pipeline.dag,
        overrides=overrides,
        resource_factories=pipeline.resources,
    ).execute(params)
