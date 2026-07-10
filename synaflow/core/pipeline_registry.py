from __future__ import annotations

import importlib
from collections.abc import Iterator, MutableMapping

from synaflow.core.dag import Dag
from synaflow.core.dag_builder import build_dag
from synaflow.core.definition import PipelineDef


class PipelineRegistry(MutableMapping[str, PipelineDef]):
    """Single source of truth for named PipelineDefs and their compiled Dags.

    ``registry[name]`` returns the PipelineDef. ``registry.get_dag(name)``
    returns the compiled Dag, building it on first access and caching it
    for subsequent calls.

    Re-registering a key invalidates the cached Dag (next ``get_dag`` call
    rebuilds). ``invalidate(name)`` drops the cached Dag explicitly.
    ``clear()`` drops everything.

    **Layering**: ``PipelineRegistry`` is core / public. It does NOT
    depend on the CLI layer and does NOT translate exceptions into
    CLI-shaped errors. ``from_module`` raises the standard Python
    exceptions you'd get from ``importlib.import_module`` and
    ``getattr``:

    - ``ModuleNotFoundError`` if the module cannot be imported
      (also raised when a top-level dependency of the module is
      missing -- Python surfaces these as the module itself being
      unfindable).
    - ``AttributeError`` if the module exists but has no such attr.
    - ``TypeError`` if the attribute is not a PipelineRegistry. The
      exception message embeds the actual value type name
      (``type(value).__name__``) so callers can format the message
      verbatim without inspecting the exception instance.

    Callers that want CLI-friendly messaging must adapt these
    exceptions themselves (see ``synaflow.cli._load_catalog``).

    **Mutability contract**: ``PipelineDef`` instances are mutable. If
    you mutate a PipelineDef (e.g. add/remove a step) after registering
    it, you must call ``registry.invalidate(name)`` before
    ``get_dag(name)`` to ensure the cache reflects the change. The
    registry only invalidates automatically on re-registration via
    ``__setitem__``.
    """

    def __init__(self) -> None:
        self._pipelines: dict[str, PipelineDef] = {}
        self._dags: dict[str, Dag] = {}

    def __getitem__(self, name: str) -> PipelineDef:
        return self._pipelines[name]

    def __setitem__(self, name: str, pipeline: PipelineDef) -> None:
        if not isinstance(pipeline, PipelineDef):
            # Raised before touching .name, so the caller gets a
            # clear message even if `name` is the wrong type too.
            raise TypeError(f"expected PipelineDef, got {type(pipeline).__name__}")
        if name != pipeline.name:
            raise ValueError(
                f"registry key {name!r} must match pipeline.name {pipeline.name!r}"
            )
        self._pipelines[name] = pipeline
        # Re-registering invalidates the cached Dag so the next
        # get_dag(name) call rebuilds.
        self._dags.pop(name, None)

    def __delitem__(self, name: str) -> None:
        del self._pipelines[name]
        # Drop the cached Dag for this name as well.
        self._dags.pop(name, None)

    def __iter__(self) -> Iterator[str]:
        return iter(self._pipelines)

    def __len__(self) -> int:
        return len(self._pipelines)

    def get_dag(self, name: str) -> Dag:
        if name not in self._pipelines:
            raise KeyError(name)
        cached = self._dags.get(name)
        if cached is not None:
            return cached
        dag = build_dag(self._pipelines[name])
        self._dags[name] = dag
        return dag

    def invalidate(self, name: str) -> None:
        if name not in self._pipelines:
            raise KeyError(name)
        self._dags.pop(name, None)

    def clear(self) -> None:
        self._pipelines.clear()
        self._dags.clear()

    @classmethod
    def from_module(
        cls, module_name: str, *, attr: str = "catalog"
    ) -> "PipelineRegistry":
        """Import ``module_name`` and return ``getattr(module, attr)``.

        The attribute must be a PipelineRegistry instance. Raises the
        standard Python exceptions from ``importlib.import_module`` and
        ``getattr`` (see class docstring). The core does NOT depend on
        the CLI layer; callers that want CLI-friendly messaging must
        adapt these exceptions themselves.
        """
        module = importlib.import_module(module_name)
        value = getattr(module, attr)
        if not isinstance(value, PipelineRegistry):
            raise TypeError(
                f"{module_name}.{attr} must be a PipelineRegistry, "
                f"got {type(value).__name__}"
            )
        return value
