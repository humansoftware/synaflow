"""
Manages the publication and routing of step outputs in the synchronous execution engine.

This module handles the complex logic of distributing a step's output to its downstream
consumers. It supports progressive stream handoffs, scalar values, materialization, and
broadcasting outputs to pipeline observers while maintaining execution integrity.
"""

import threading
from collections.abc import Iterator
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.types import StepMode, OnError
from synaflow.core.exceptions import PipelineStopException
from synaflow.execution.sync_handoff import SyncFanout
from synaflow.execution.sync_engine.event_dispatch import EventDispatcher
from synaflow.execution.sync_engine.argument_builder import ArgumentBuilder
from synaflow.execution.threshold import has_threshold
from synaflow.execution.bounded_iterator import BoundedIterator


class StreamPublisher:
    """
    Handles publishing, materialization, and fan-out of step outputs.

    This class coordinates how the output of a step is delivered to its dependents.
    It manages:
    - Wrapping outputs in bounded iterators to control in-flight data.
    - Applying materializers if the DAG definition or overrides require it.
    - Distributing progressive streams to multiple consumers via SyncFanout.
    - Delivering step results to configured output observers in background threads.
    - Propagating pipeline abortion signals to active fanouts.
    """

    def __init__(
        self,
        dag: Dag,
        outputs: dict,
        events: EventDispatcher,
        step_output_observers: list,
        scope: ArgumentBuilder,
    ):
        self._dag = dag
        self._outputs = outputs
        self._events = events
        self._step_output_observers = step_output_observers
        self._scope = scope
        self._active_fanouts: list[SyncFanout] = []
        self._observer_threads: list[threading.Thread] = []

    def _maybe_wrap_stream(self, output: Any, node: Any) -> Any:
        """Wrap a progressive stream output with bounded handoff if needed."""
        if node.max_in_flight <= 1:
            return output
        if not isinstance(output, Iterator):
            return output
        return BoundedIterator(output, node.max_in_flight)

    def _collect_iterator(
        self,
        step_name: str,
        value: Iterator,
    ) -> tuple[list[Any], bool, BaseException | None]:
        items = []
        while True:
            try:
                items.append(next(value))
            except StopIteration:
                return items, False, None
            except Exception as exc:
                self._events.handle_error(
                    step_name,
                    exc,
                    success_count=len(items),
                    error_count=1,
                    completed_all_inputs=False,
                )
                if self._dag[step_name].on_error == OnError.STOP:
                    raise PipelineStopException(step_name=step_name, cause=exc) from exc
                return items, True, exc

    def _apply_materializer(
        self,
        step_name: str,
        value: Any,
        materializer: Any,
        consumer_type: Any = None,
    ) -> tuple[Any, bool, BaseException | None]:
        if materializer is None:
            if isinstance(value, Iterator):
                items, had_error, exc = self._collect_iterator(step_name, value)
                return items, had_error, exc
            return value, False, None

        if isinstance(value, Iterator):
            items, had_error, exc = self._collect_iterator(step_name, value)
            return materializer(items), had_error, exc

        return materializer(value), False, None

    def publish(self, step_name: str, output: Any, node: Any) -> None:
        deferred = node.mode == StepMode.EACH or (
            node.mode == StepMode.ALL and isinstance(output, Iterator)
        )

        if not isinstance(output, Iterator):
            output = self._notify_observers(step_name, output)
            self._publish_scalar_output(step_name, output, node, deferred)
            return

        consumers = self._dag.consumers_of(step_name)

        if self._dag.needs_materialize(step_name):
            self._materialize_stream_output(
                step_name, output, node, consumers, deferred
            )
            return

        if deferred:
            output = self._wrap_deferred_output(step_name, output, node)

        if len(consumers) == 1 and self._step_output_observers:
            self._publish_stream_to_single_consumer(
                step_name, output, node, consumers[0], deferred
            )
            return

        if len(consumers) > 1:
            self._publish_stream_to_multiple_consumers(
                step_name, output, node, consumers
            )
            return

        if len(consumers) == 0 and self._step_output_observers:
            fanout = SyncFanout(
                output,
                max_in_flight=max(1, node.max_in_flight),
                branches=self._observer_branch_names(),
            )
            self._active_fanouts.append(fanout)
            self._start_observer_threads(
                step_name, fanout, self._observer_branch_names()
            )
            fanout.start()
            return

        output = self._notify_observers(step_name, output)
        self._outputs[step_name] = self._maybe_wrap_stream(output, node)

    def abort(self, exception: BaseException | None = None) -> None:
        for fanout in self._active_fanouts:
            fanout.abort(exception)

    def cleanup(self) -> None:
        for fanout in self._active_fanouts:
            fanout.join()
        self._active_fanouts.clear()
        for thread in self._observer_threads:
            thread.join()
        self._observer_threads.clear()

    def _notify_observers(self, step_name, output):
        if not self._step_output_observers:
            return output
        if isinstance(output, Iterator):
            pass
        else:
            for observer in self._step_output_observers:
                observer(step_name, output)
        return output

    def _observer_branch_names(self) -> list[str]:
        return [f"__obs{i}" for i, _observer in enumerate(self._step_output_observers)]

    def _collect_observer_items(self, branch) -> list[Any]:
        items = []
        try:
            for item in branch:
                items.append(item)
        except Exception:
            pass
        return items

    def _start_observer_threads(
        self,
        step_name: str,
        fanout: SyncFanout,
        observer_branch_names: list[str],
    ) -> None:
        for branch_name, observer in zip(
            observer_branch_names, self._step_output_observers
        ):
            iterator = fanout.lazy_iterator(branch_name)

            def run_observer(obs=observer, branch_iter=iterator):
                obs(step_name, self._collect_observer_items(branch_iter))

            thread = threading.Thread(target=run_observer, daemon=True)
            thread.start()
            self._observer_threads.append(thread)

    def _materialize_with_events(self, step_name, output, node, consumer_type=None):
        materializer = self._scope.resolve_materializer(step_name, node)
        mat_name = materializer.__name__ if callable(materializer) else None
        self._events.materialization_started(
            step_name,
            node,
            consumer_type,
            mat_name,
        )
        try:
            result, had_error, exc = self._apply_materializer(
                step_name,
                output,
                materializer,
                consumer_type=consumer_type,
            )
            self._events.materialization_completed(
                step_name,
                node,
                consumer_type,
                mat_name,
            )
            return result, had_error, exc
        except PipelineStopException:
            raise
        except Exception as exc:
            self._events.materialization_failed(
                step_name,
                node,
                consumer_type,
                mat_name,
                exception=exc,
            )
            raise

    def _materialize_stream_output(
        self,
        step_name,
        output,
        node,
        consumers,
        deferred,
    ):
        consumer_type = None
        if consumers:
            consumer_type = self._dag[consumers[0]].deps.get(step_name)
        output, had_error, exc = self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        output = self._notify_observers(step_name, output)
        if deferred:
            self._emit_step_result(node, step_name, output, had_error, exc)
        for consumer in consumers:
            self._outputs[self._dag.output_key(step_name, consumer)] = output

    def _publish_stream_to_single_consumer(
        self,
        step_name,
        output,
        node,
        consumer,
        deferred,
    ):
        consumer_type = self._dag[consumer].deps.get(step_name)

        if self._step_output_observers and not self._dag.needs_materialize(step_name):
            observer_branches = self._observer_branch_names()
            fanout = SyncFanout(
                output,
                max_in_flight=max(1, node.max_in_flight),
                branches=[consumer, *observer_branches],
            )
            self._active_fanouts.append(fanout)
            self._outputs[self._dag.output_key(step_name, consumer)] = (
                fanout.lazy_iterator(consumer)
            )
            self._start_observer_threads(step_name, fanout, observer_branches)
            fanout.start()
            return
        output, had_error, exc = self._materialize_with_events(
            step_name, output, node, consumer_type=consumer_type
        )
        output = self._notify_observers(step_name, output)
        if deferred:
            self._emit_step_result(node, step_name, output, had_error, exc)
        output = self._maybe_wrap_stream(output, node)
        self._outputs[self._dag.output_key(step_name, consumer)] = output

    def _publish_stream_to_multiple_consumers(self, step_name, output, node, consumers):
        fanout = SyncFanout(
            output,
            max_in_flight=max(1, node.max_in_flight),
            branches=consumers + self._observer_branch_names(),
        )
        self._active_fanouts.append(fanout)
        for consumer in consumers:
            self._outputs[self._dag.output_key(step_name, consumer)] = (
                fanout.lazy_iterator(consumer)
            )
        self._start_observer_threads(step_name, fanout, self._observer_branch_names())
        fanout.start()

    def _publish_scalar_output(self, step_name, output, node, deferred):
        if self._dag.needs_materialize(step_name):
            output, _, _ = self._materialize_with_events(
                step_name, output, node, consumer_type=node.output
            )
        self._outputs[step_name] = output
        if deferred:
            self._emit_step_result(
                node, step_name, output, had_error=False, exception=None
            )

    def _emit_step_result(self, node, step_name, output, had_error, exception=None):
        if has_threshold(node):
            return  # already dispatched by generate()
        success = len(output) if hasattr(output, "__len__") else 1
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", success)
        if had_error:
            self._events.step_failed(
                node,
                step_name,
                success_count=success,
                error_count=max(real_error_count, 1),
                completed_all_inputs=False,
                exception=exception,
            )
        else:
            self._events.step_completed(
                node,
                step_name,
                success_count=real_invocation_count - real_error_count,
                error_count=real_error_count,
                completed_all_inputs=True,
            )

    def _emit_deferred_completion(self, node, step_name):
        if has_threshold(node):
            return  # already dispatched by generate()
        real_error_count = getattr(node, "_runtime_error_count", 0)
        real_invocation_count = getattr(node, "_runtime_invocation_count", 0)
        self._events.step_completed(
            node,
            step_name,
            success_count=real_invocation_count - real_error_count,
            error_count=real_error_count,
            completed_all_inputs=True,
        )

    def _wrap_deferred_output(self, step_name, output, node):
        if has_threshold(node):
            return output

        def wrapped():
            yielded_count = 0
            for item in output:
                yielded_count += 1
                yield item

            if node.mode == StepMode.ALL:
                node._runtime_invocation_count = yielded_count
                node._runtime_error_count = 0
            self._emit_deferred_completion(node, step_name)

        return wrapped()
