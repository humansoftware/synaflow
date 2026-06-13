import inspect
from collections.abc import Generator, Iterator
from typing import Any

from synaflow.core.type_compatibility import is_iterable_type, is_scalar

from .stream_routing import resolve_dependency


class SyncDependencyResolver:
    def __init__(self, pipeline: Any, context: dict[str, Any]):
        self.dag = pipeline.dag
        self.context = context

    def resolve_node_arguments(self, consumer_name: str, node: dict) -> dict[str, Any]:
        sig = inspect.signature(node["fn"])
        deps = node.get("deps", {})
        kwargs: dict[str, Any] = {}

        for param_name in sig.parameters:
            if param_name in self.context:
                value = self.context.get(param_name)

                value = resolve_dependency(value, consumer_name)

                kwargs[param_name] = value

        return kwargs

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
