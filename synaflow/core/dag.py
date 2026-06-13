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
    force_materialize: bool = False
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
            "materializer": mat.__name__ if callable(mat) else None,
            "materialized_deps": self.materialized_deps,
            "pipeline": self.pipeline,
            "parent_pipeline": self.parent_pipeline,
        }


@dataclass
class Dag:
    name: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    steps: dict[str, DagNode] = field(default_factory=dict)
    requires_sync_runner: bool = False
    requires_async_runner: bool = False
    error_materializer_factory: Any = None

    def __getitem__(self, key):
        return self.steps[key]

    def __setitem__(self, key, value):
        self.steps[key] = value

    def __contains__(self, key):
        return key in self.steps

    def __len__(self):
        return len(self.steps)

    def __iter__(self):
        return iter(self.steps)

    def items(self):
        return self.steps.items()

    def values(self):
        return self.steps.values()

    def get(self, key, default=None):
        if key in self.steps:
            return self.steps[key]
        if key in self.params:
            return DagNode(output=self.params[key])
        return default

    def pop(self, key, *args):
        return self.steps.pop(key, *args)

    def to_dict(self) -> dict:
        from synaflow.core.type_compatibility import get_type_name

        result = {
            "name": self.name,
            "params": {k: get_type_name(v) for k, v in self.params.items()},
            "steps": {
                name: node.to_serializable() for name, node in self.steps.items()
            },
        }
        if self.error_materializer_factory is not None:
            result[
                "error_materializer_factory"
            ] = self.error_materializer_factory.__name__
        return result

    def consumers_of(self, step_name: str) -> list[str]:
        return [name for name, node in self.steps.items() if step_name in node.deps]

    def get_execution_levels(self) -> list[list[str]]:
        in_degree: dict[str, int] = {name: 0 for name in self.steps}
        for name, node in self.steps.items():
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
                for other_name, node in self.steps.items():
                    if name in node.deps:
                        in_degree[other_name] -= 1

        return levels
