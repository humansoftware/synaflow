import itertools
from collections.abc import Callable, Iterator
from typing import Any

from synaflow.core.definition import PipelineDef
from synaflow.core.exceptions import PipelineStopException, StepExecutionError
from synaflow.core.types import OnError

from .dependencies import SyncDependencyResolver
from .iterator_utils import InterleavedIterator
from .topology import SyncStreamManager, TeeWrapper


class SyncNodeRunner:
    def __init__(
        self,
        pipeline: PipelineDef,
        context: dict[str, Any],
        executed_steps: set[str],
        resolver: SyncDependencyResolver,
        stream_manager: SyncStreamManager,
    ):
        self.pipeline = pipeline
        self.dag = pipeline._dag
        self.context = context
        self.executed_steps = executed_steps
        self.resolver = resolver
        self.stream_manager = stream_manager

    def group_nodes_by_execution_mode(
        self, level: list[str]
    ) -> tuple[dict, dict, list]:
        dep_each_nodes: dict[str, list[str]] = {}
        dep_all_nodes: dict[str, list[str]] = {}
        independent_nodes: list[str] = []

        for name in level:
            if name in self.executed_steps:
                continue

            node = self.dag.get(name)
            if not node or node.get("fn") is None:
                continue

            deps = node.get("deps", {})
            if not deps:
                independent_nodes.append(name)
                continue

            first_dep_name = next(iter(deps))
            if self.resolver.is_each_mode_execution(deps, first_dep_name):
                dep_each_nodes.setdefault(first_dep_name, []).append(name)
            else:
                consumer_type = deps[first_dep_name]
                if self.resolver.is_lazy_iterator_type(consumer_type):
                    dep_all_nodes.setdefault(first_dep_name, []).append(name)
                else:
                    independent_nodes.append(name)

        return dep_each_nodes, dep_all_nodes, independent_nodes

    def process_grouped_dependencies(
        self,
        dep_name: str,
        each_names: list[str],
        all_names: list[str],
        independent_nodes: list[str],
    ) -> None:
        eager_each_names, lazy_each_names = self._split_eager_and_lazy_each_nodes(
            each_names
        )

        eager_callbacks = self._create_eager_callbacks(eager_each_names, dep_name)

        independent_nodes.extend(lazy_each_names)

        if eager_callbacks:
            self._execute_eager_callbacks(
                eager_callbacks, dep_name, all_names, independent_nodes
            )
        else:
            independent_nodes.extend(all_names)

    def execute_independent_node(self, name: str) -> None:
        if name in self.executed_steps:
            return

        node = self.dag.get(name)
        if not node or node.get("fn") is None:
            return

        fn = node["fn"]
        deps = node.get("deps", {})
        kwargs = self.resolver.resolve_node_arguments(name, node)

        if deps and self.resolver.is_each_mode_execution(deps, next(iter(deps))):
            self._execute_independent_each_node(name, fn, deps, kwargs, node)
        else:
            self._execute_standard_node(name, fn, kwargs, node)

        self.executed_steps.add(name)

    def _split_eager_and_lazy_each_nodes(
        self, each_names: list[str]
    ) -> tuple[list[str], list[str]]:
        eager = []
        lazy = []
        for name in each_names:
            consumers = [
                cn for cn, cnode in self.dag.items() if name in cnode.get("deps", {})
            ]
            if name.startswith("_") or not consumers:
                eager.append(name)
            else:
                lazy.append(name)
        return eager, lazy

    def _create_eager_callbacks(
        self, eager_names: list[str], dep_name: str
    ) -> list[Callable]:
        callbacks = []
        for name in eager_names:
            node = self.dag[name]
            fn = node["fn"]
            kwargs = self.resolver.resolve_node_arguments(name, node)
            on_error = node.get("on_error")

            def make_callback(fn, kwargs, dep_name, on_error):
                def cb(item):
                    try:
                        item_kwargs = dict(kwargs)
                        item_kwargs[dep_name] = item
                        fn(**item_kwargs)
                    except Exception as e:
                        if on_error and on_error == OnError.STOP:
                            raise PipelineStopException() from e

                return cb

            callbacks.append(make_callback(fn, kwargs, dep_name, on_error))
            self.executed_steps.add(name)

        return callbacks

    def _execute_eager_callbacks(
        self,
        callbacks: list[Callable],
        dep_name: str,
        all_names: list[str],
        independent_nodes: list[str],
    ) -> None:
        items_source = self.context.get(dep_name)

        if all_names:
            first_all = all_names[0]
            self._execute_interleaved_node(first_all, dep_name, callbacks)
            independent_nodes.extend(all_names[1:])
        else:
            self._execute_lockstep_loop(items_source, callbacks, dep_name)

    def _execute_interleaved_node(
        self, node_name: str, dep_name: str, callbacks: list[Callable]
    ) -> None:
        node = self.dag[node_name]
        fn = node["fn"]
        kwargs = self.resolver.resolve_node_arguments(node_name, node)

        dep_val = kwargs.get(dep_name)
        if isinstance(dep_val, TeeWrapper):
            dep_val = dep_val.tees[node_name]

        kwargs[dep_name] = InterleavedIterator(dep_val, callbacks)

        try:
            output = fn(**kwargs)
            if node_name and not node_name.startswith("_"):
                self.context[node_name] = output
        except Exception:
            if node.get("on_error") and node["on_error"] == OnError.STOP:
                raise PipelineStopException()

        self.executed_steps.add(node_name)

    def _execute_lockstep_loop(
        self, items_source: Any, callbacks: list[Callable], dep_name: str
    ) -> None:
        dep_val = items_source
        if isinstance(dep_val, TeeWrapper):
            consumers = [
                cn
                for cn, cnode in self.dag.items()
                if dep_name in cnode.get("deps", {})
            ]
            first_tee_name = next(c for c in consumers if c in dep_val.tees)
            dep_val = dep_val.tees[first_tee_name]

        for item in dep_val:
            for cb in callbacks:
                cb(item)

    def _execute_independent_each_node(
        self, name: str, fn: Callable, deps: dict, kwargs: dict, node: dict
    ) -> None:
        first_dep = next(iter(deps))
        items = self.context.get(first_dep)

        if isinstance(items, TeeWrapper):
            items = items.tees[name]

        consumers = [
            cn for cn, cnode in self.dag.items() if name in cnode.get("deps", {})
        ]
        is_sink = name.startswith("_") or len(consumers) == 0

        if is_sink:
            for item in items:
                try:
                    item_kwargs = dict(kwargs)
                    item_kwargs[first_dep] = item
                    fn(**item_kwargs)
                except Exception:
                    if node.get("on_error") and node["on_error"] == OnError.STOP:
                        raise PipelineStopException()
        else:

            def each_generator(items, kwargs, first_dep, fn, on_error):
                for item in items:
                    try:
                        item_kwargs = dict(kwargs)
                        item_kwargs[first_dep] = item
                        yield fn(**item_kwargs)
                    except Exception as e:
                        if on_error and on_error == OnError.STOP:
                            raise PipelineStopException() from e

            output = each_generator(items, kwargs, first_dep, fn, node.get("on_error"))

            if node.get("needs_materialize"):
                output = self.stream_manager.apply_materializer(name, output)

            if len(consumers) > 1:
                tees = itertools.tee(output, len(consumers))
                output = TeeWrapper(dict(zip(consumers, tees)))

            self.context[name] = output

    def _execute_standard_node(
        self, name: str, fn: Callable, kwargs: dict, node: dict
    ) -> None:
        try:
            output = fn(**kwargs)

            if name and not name.startswith("_"):
                if isinstance(output, Iterator) and node.get("needs_materialize"):
                    output = self.stream_manager.apply_materializer(name, output)
                elif isinstance(output, Iterator):
                    output = self.stream_manager.tee_iterator_for_consumers(
                        name, output
                    )

                self.context[name] = output

        except Exception:
            if node.get("on_error") and node["on_error"] == OnError.STOP:
                raise PipelineStopException()
