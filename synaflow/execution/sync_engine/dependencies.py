import inspect
from collections.abc import Generator, Iterator
from typing import Any

from synaflow.core.type_compatibility import is_iterable_type, is_scalar

from .topology import TeeWrapper


class SyncDependencyResolver:
    def __init__(self, pipeline: Any, context: dict[str, Any]):
        self.dag = pipeline._dag
        self.context = context

    def resolve_node_arguments(self, consumer_name: str, node: dict) -> dict[str, Any]:
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
                    value = self.adapt_argument_to_consumer_type(value, consumer_type)

                kwargs[param_name] = value

        return kwargs

    def adapt_argument_to_consumer_type(self, value: Any, consumer_type: Any) -> Any:
        is_lazy_iterator = self.is_lazy_iterator_type(consumer_type)
        needs_materialization = self.needs_materialize_for(consumer_type)

        if is_lazy_iterator or needs_materialization:
            if not isinstance(value, (list, set, tuple, Iterator, Generator)):
                value = [value]

            if isinstance(value, Iterator) and needs_materialization:
                value = list(value)  # Default fallback materialization

            origin = getattr(consumer_type, "__origin__", consumer_type)
            if origin is set:
                value = set(value)
            elif origin is tuple:
                value = tuple(value)
            elif origin in (Iterator, Generator):
                value = iter(value)

        return value

    def is_each_mode_execution(self, deps: dict, first_dep_name: str) -> bool:
        if not deps:
            return False

        first_type = deps[first_dep_name]
        producer = self.dag.get(first_dep_name)
        if not producer or producer.get("output") is None:
            return False

        producer_output = producer.get("output")
        return is_iterable_type(producer_output) and is_scalar(first_type)

    def is_lazy_iterator_type(self, tp: Any) -> bool:
        if tp is Iterator:
            return True
        origin = getattr(tp, "__origin__", tp)
        return origin in (Iterator, Generator)

    def needs_materialize_for(self, tp: Any) -> bool:
        if tp is None:
            return False
        if tp in (list, set, tuple):
            return True
        origin = getattr(tp, "__origin__", None)
        return origin in (list, set, tuple)
