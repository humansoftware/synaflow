from __future__ import annotations

import importlib
from collections.abc import Iterator, Mapping

from synaflow.core.dag import Dag
from synaflow.core.dag_builder import build_dag
from synaflow.core.definition import IncludeStep, PipelineDef


class PipelineRegistry(Mapping[str, PipelineDef]):
    """Validated, name-addressable pipelines and their compiled Dags.

    ``add(pipeline)`` is the only registration operation. It recursively
    collects included pipelines, compiles every candidate, and commits the
    resulting ``(PipelineDef, Dag)`` pairs atomically. A registry therefore
    never exposes a registered definition without its validated Dag.

    Registered definitions must not be mutated. Re-adding the same instance
    is a no-op; adding a different instance with an existing name is a
    configuration error.

    ``registry[name]`` returns the definition and ``get_dag(name)`` returns
    its already-compiled Dag.
    """

    def __init__(self) -> None:
        self._pipelines: dict[str, PipelineDef] = {}
        self._dags: dict[str, Dag] = {}

    def __getitem__(self, name: str) -> PipelineDef:
        return self._pipelines[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._pipelines)

    def __len__(self) -> int:
        return len(self._pipelines)

    def add(self, pipeline: PipelineDef) -> None:
        """Compile and atomically register ``pipeline`` and its includes."""
        if not isinstance(pipeline, PipelineDef):
            raise TypeError(f"expected PipelineDef, got {type(pipeline).__name__}")

        candidates = _collect_pipeline_tree(pipeline)
        _validate_registration_collisions(self._pipelines, candidates)

        new_candidates = {
            name: candidate
            for name, candidate in candidates.items()
            if name not in self._pipelines
        }
        compiled = {
            name: build_dag(candidate) for name, candidate in new_candidates.items()
        }

        self._pipelines.update(new_candidates)
        self._dags.update(compiled)

    def get_dag(self, name: str) -> Dag:
        return self._dags[name]

    @classmethod
    def from_module(
        cls, module_name: str, *, attr: str = "catalog"
    ) -> "PipelineRegistry":
        """Import ``module_name`` and return its PipelineRegistry ``attr``."""
        module = importlib.import_module(module_name)
        value = getattr(module, attr)
        if not isinstance(value, PipelineRegistry):
            raise TypeError(
                f"{module_name}.{attr} must be a PipelineRegistry, "
                f"got {type(value).__name__}"
            )
        return value


def _collect_pipeline_tree(root: PipelineDef) -> dict[str, PipelineDef]:
    """Return every distinct named definition reachable through ``include``."""
    pipelines: dict[str, PipelineDef] = {}
    visiting: set[int] = set()

    def visit(pipeline: PipelineDef) -> None:
        pipeline_id = id(pipeline)
        if pipeline_id in visiting:
            raise ValueError(f"Pipeline include cycle detected at {pipeline.name!r}")

        existing = pipelines.get(pipeline.name)
        if existing is not None:
            if existing is not pipeline:
                raise ValueError(
                    f"Pipeline name {pipeline.name!r} refers to multiple instances"
                )
            return

        pipelines[pipeline.name] = pipeline
        visiting.add(pipeline_id)
        try:
            for declared_step in pipeline.steps:
                if isinstance(declared_step, IncludeStep):
                    visit(declared_step.pipeline)
        finally:
            visiting.remove(pipeline_id)

    visit(root)
    return pipelines


def _validate_registration_collisions(
    registered: Mapping[str, PipelineDef],
    candidates: Mapping[str, PipelineDef],
) -> None:
    for name, candidate in candidates.items():
        existing = registered.get(name)
        if existing is not None and existing is not candidate:
            raise ValueError(
                f"Pipeline {name!r} is already registered with a different instance"
            )
