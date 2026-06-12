import inspect
import itertools
from collections.abc import Callable, Generator, Iterator
from typing import Any

from .iterator_utils import InterleavedIterator
from .pipeline import PipelineDef
from .type_compatibility import is_iterable_type, is_scalar
from .types import OnError


class TeeWrapper:
    def __init__(self, tees: dict[str, Iterator]):
        self.tees = tees


class PipelineStopException(Exception):
    """Raised to stop the pipeline execution early."""

    pass


class PipelineExecutor:
    """Executes a compiled Directed Acyclic Graph (DAG) for a pipeline."""

    def __init__(self, pipeline: PipelineDef, materialize_fn: Callable = list):
        self.pipeline = pipeline
        self.dag = pipeline._dag
        self.materialize_fn = materialize_fn
        self.context: dict[str, Any] = {}
        self.executed_steps: set[str] = set()

    def execute(self, params: Any) -> None:
        self._initialize_context_with_params(params)

        try:
            levels = self.pipeline.get_execution_levels()
            for level in levels:
                self._execute_level(level)
        except PipelineStopException:
            pass

    def _initialize_context_with_params(self, params: Any) -> None:
        for field, value in params._asdict().items():
            node = self.dag.get(field, {})
            needs_materialization = node.get("needs_materialize", False)

            if needs_materialization and isinstance(value, Iterator):
                value = self.materialize_fn(value)
            elif isinstance(value, Iterator):
                value = self._tee_iterator_for_consumers(field, value)

            self.context[field] = value

    def _tee_iterator_for_consumers(
        self, producer_name: str, iterator_value: Iterator
    ) -> Any:
        consumers = [
            consumer_name
            for consumer_name, node in self.dag.items()
            if producer_name in node.get("deps", {})
        ]
        if len(consumers) > 1:
            tees = itertools.tee(iterator_value, len(consumers))
            return TeeWrapper(dict(zip(consumers, tees)))
        return iterator_value

    def _execute_level(self, level: list[str]) -> None:
        (
            dep_each_nodes,
            dep_all_nodes,
            independent_nodes,
        ) = self._group_nodes_by_execution_mode(level)

        all_dependencies = set(dep_each_nodes.keys()) | set(dep_all_nodes.keys())

        for dep_name in all_dependencies:
            each_names = dep_each_nodes.get(dep_name, [])
            all_names = dep_all_nodes.get(dep_name, [])
            self._process_grouped_dependencies(
                dep_name, each_names, all_names, independent_nodes
            )

        for name in independent_nodes:
            self._execute_independent_node(name)

    def _group_nodes_by_execution_mode(
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
            if self._is_each_mode_execution(deps, first_dep_name):
                dep_each_nodes.setdefault(first_dep_name, []).append(name)
            else:
                consumer_type = (
                    inspect.signature(node["fn"]).parameters[first_dep_name].annotation
                )
                if self._is_lazy_iterator_type(consumer_type):
                    dep_all_nodes.setdefault(first_dep_name, []).append(name)
                else:
                    independent_nodes.append(name)

        return dep_each_nodes, dep_all_nodes, independent_nodes

    def _process_grouped_dependencies(
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
            kwargs = self._resolve_node_arguments(name, node)
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
        kwargs = self._resolve_node_arguments(node_name, node)

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

    def _execute_independent_node(self, name: str) -> None:
        if name in self.executed_steps:
            return

        node = self.dag.get(name)
        if not node or node.get("fn") is None:
            return

        fn = node["fn"]
        deps = node.get("deps", {})
        kwargs = self._resolve_node_arguments(name, node)

        if deps and self._is_each_mode_execution(deps, next(iter(deps))):
            self._execute_independent_each_node(name, fn, deps, kwargs, node)
        else:
            self._execute_standard_node(name, fn, kwargs, node)

        self.executed_steps.add(name)

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
                output = self.materialize_fn(output)

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
                    output = self.materialize_fn(output)
                elif isinstance(output, Iterator):
                    output = self._tee_iterator_for_consumers(name, output)

                self.context[name] = output

        except Exception:
            if node.get("on_error") and node["on_error"] == OnError.STOP:
                raise PipelineStopException()

    def _resolve_node_arguments(self, consumer_name: str, node: dict) -> dict[str, Any]:
        sig = inspect.signature(node["fn"])
        deps = node.get("deps", {})
        kwargs: dict[str, Any] = {}

        for param_name in sig.parameters:
            if param_name in self.context:
                value = self.context.get(param_name)

                if isinstance(value, TeeWrapper):
                    value = value.tees[consumer_name]

                if param_name in deps:
                    consumer_type = deps[param_name]
                    value = self._adapt_argument_to_consumer_type(value, consumer_type)

                kwargs[param_name] = value

        return kwargs

    def _adapt_argument_to_consumer_type(self, value: Any, consumer_type: Any) -> Any:
        is_lazy_iterator = self._is_lazy_iterator_type(consumer_type)
        needs_materialization = self._needs_materialize_for(consumer_type)

        if is_lazy_iterator or needs_materialization:
            if not isinstance(value, (list, set, tuple, Iterator, Generator)):
                value = [value]

            if isinstance(value, Iterator) and needs_materialization:
                value = self.materialize_fn(value)

            origin = getattr(consumer_type, "__origin__", consumer_type)
            if origin is set:
                value = set(value)
            elif origin is tuple:
                value = tuple(value)
            elif origin in (Iterator, Generator):
                value = iter(value)

        return value

    def _is_each_mode_execution(self, deps: dict, first_dep_name: str) -> bool:
        if not deps:
            return False

        first_type = deps[first_dep_name]
        producer = self.dag.get(first_dep_name)
        if not producer or producer.get("output") is None:
            return False

        producer_output = producer.get("output")
        return is_iterable_type(producer_output) and is_scalar(first_type)

    def _is_lazy_iterator_type(self, tp: Any) -> bool:
        if tp is Iterator:
            return True
        origin = getattr(tp, "__origin__", tp)
        return origin in (Iterator, Generator)

    def _needs_materialize_for(self, tp: Any) -> bool:
        if tp is None:
            return False
        if tp in (list, set, tuple):
            return True
        origin = getattr(tp, "__origin__", None)
        return origin in (list, set, tuple)


def run(pipeline: PipelineDef, params: Any, *, materialize: Callable = list) -> None:
    """Executes a pipeline definition synchronously."""
    if getattr(pipeline, "requires_async_runner", False):
        raise RuntimeError(
            "This pipeline contains async features (async def or AsyncIterator) and must be executed with async_run()."
        )

    executor = PipelineExecutor(pipeline, materialize)
    executor.execute(params)
