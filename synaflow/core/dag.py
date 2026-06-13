from dataclasses import dataclass, field
from typing import Any, Callable

from synaflow.core.types import OnError


@dataclass
class DagNode:
    fn: Callable | None = None
    deps: dict[str, Any] = field(default_factory=dict)
    output: Any = None
    on_error: OnError | None = None
    materializer: Callable | None = None
    materialized_deps: list[str] = field(default_factory=list)
    needs_materialize: bool = False
    pipeline: str | None = None
    parent_pipeline: str | None = None

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def to_serializable(self) -> dict:
        from synaflow.core.type_compatibility import get_type_name

        mat = self.materializer
        return {
            "deps": {k: get_type_name(v) for k, v in self.deps.items()},
            "output": get_type_name(self.output),
            "fn": self.fn.__name__ if self.fn else None,
            "on_error": self.on_error.value if self.on_error else None,
            "needs_materialize": self.needs_materialize,
            "materializer": mat.__name__ if callable(mat) else None,
            "materialized_deps": self.materialized_deps,
            "pipeline": self.pipeline,
            "parent_pipeline": self.parent_pipeline,
        }


@dataclass
class Dag:
    nodes: dict[str, DagNode] = field(default_factory=dict)
    requires_sync_runner: bool = False
    requires_async_runner: bool = False

    def __getitem__(self, key):
        return self.nodes[key]

    def __setitem__(self, key, value):
        self.nodes[key] = value

    def __contains__(self, key):
        return key in self.nodes

    def __len__(self):
        return len(self.nodes)

    def __iter__(self):
        return iter(self.nodes)

    def items(self):
        return self.nodes.items()

    def values(self):
        return self.nodes.values()

    def get(self, key, default=None):
        return self.nodes.get(key, default)

    def pop(self, key, *args):
        return self.nodes.pop(key, *args)

    def to_dict(self) -> dict:
        return {name: node.to_serializable() for name, node in self.nodes.items()}

    def get_execution_levels(self) -> list[list[str]]:
        in_degree: dict[str, int] = {name: 0 for name in self.nodes}
        for name, node in self.nodes.items():
            for dep in node.deps:
                if dep in in_degree:
                    in_degree[name] += 1

        levels: list[list[str]] = []
        processed: set[str] = set()

        while len(processed) < len(in_degree):
            level = [
                name
                for name, degree in in_degree.items()
                if degree == 0 and name not in processed
            ]
            if not level:
                break
            levels.append(level)
            processed.update(level)

            for name in level:
                for other_name, node in self.nodes.items():
                    if name in node.deps:
                        in_degree[other_name] -= 1

        return levels
