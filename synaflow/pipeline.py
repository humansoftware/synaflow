from dataclasses import dataclass, field
from typing import Any

from synaflow.types import MaterializerFactory

from .step import Step


@dataclass
class PipelineDef:
    """
    Defines a Pipeline workflow.
    """

    name: str
    params: Any
    steps: list[Step]
    default_materializer_factory: MaterializerFactory | None = None
    _dag: dict[str, dict[str, Any]] = field(default_factory=dict)
    _compiled: bool = False
    description: str = ""

    def __post_init__(self) -> None:
        from .validator import validate_and_build_dag

        self._dag = validate_and_build_dag(self.name, self.steps, self.params)
        metadata = self._dag.pop("__metadata__", {})
        self.requires_sync_runner = metadata.get("requires_sync_runner", False)
        self.requires_async_runner = metadata.get("requires_async_runner", False)

    def to_dict(self) -> dict:
        """Exports the compiled DAG structure to a JSON-serializable dictionary."""
        from .type_compatibility import get_type_name

        serialized = {}
        for name, node in self._dag.items():
            serialized[name] = {
                "deps": {k: get_type_name(v) for k, v in node["deps"].items()},
                "output": get_type_name(node["output"]),
                "fn": node["fn"].__name__ if node["fn"] else None,
                "on_error": node["on_error"].value if node["on_error"] else None,
                "needs_materialize": node["needs_materialize"],
            }
        return serialized

    def get_execution_levels(self) -> list[list[str]]:
        """
        Returns the steps grouped into topological levels.
        Steps in the same level have no dependencies on each other and could theoretically be executed in parallel.
        """
        in_degree: dict[str, int] = {name: 0 for name in self._dag}
        for name, node in self._dag.items():
            for dep in node.get("deps", {}):
                if dep in in_degree:
                    in_degree[name] += 1

        levels: list[list[str]] = []
        processed: set[str] = set()

        while len(processed) < len(self._dag):
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
                for other_name, node in self._dag.items():
                    if name in node.get("deps", {}):
                        in_degree[other_name] -= 1

        return levels


pipeline = PipelineDef
