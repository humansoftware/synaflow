from collections.abc import Iterator, MutableMapping
from dataclasses import dataclass
from typing import Any

from synaflow.core.constants import PIPELINE_SCOPE
from synaflow.core.naming import Scope
from synaflow.core.observers import Observer, ResolvedObserver
from synaflow.core.dag import Dag


class PipelineRegistry(MutableMapping[str, Any]):
    def __init__(
        self,
        *,
        contract_keys: set[str],
        fallback_values: dict[str, Any] | None = None,
    ) -> None:
        self._contract_keys = set(contract_keys)
        self._fallback_values = dict(fallback_values or {})
        self._overrides: dict[str, Any] = {}

    def __getitem__(self, key: str | Scope) -> Any:
        normalized_key = self._normalize_key(key)
        self._validate_key(normalized_key)
        if normalized_key in self._overrides:
            return self._overrides[normalized_key]
        if normalized_key in self._fallback_values:
            return self._fallback_values[normalized_key]
        raise KeyError(normalized_key)

    def __setitem__(self, key: str | Scope, value: Any) -> None:
        normalized_key = self._normalize_key(key)
        self._validate_key(normalized_key)
        self._overrides[normalized_key] = self._normalize_value(normalized_key, value)

    def __delitem__(self, key: str | Scope) -> None:
        normalized_key = self._normalize_key(key)
        self._validate_key(normalized_key)
        if normalized_key not in self._overrides:
            raise KeyError(normalized_key)
        del self._overrides[normalized_key]

    def __iter__(self) -> Iterator[str]:
        return iter(sorted(self._contract_keys))

    def __len__(self) -> int:
        return len(self._contract_keys)

    def resolve(self, key: str | Scope, default: Any = None) -> Any:
        normalized_key = self._normalize_key(key)
        if normalized_key in self._overrides:
            return self._overrides[normalized_key]
        if normalized_key in self._fallback_values:
            return self._fallback_values[normalized_key]
        return default

    def _normalize_key(self, key: str | Scope) -> str:
        if isinstance(key, Scope):
            return str(key)
        return key

    def _validate_key(self, key: str) -> None:
        if key not in self._contract_keys:
            valid = ", ".join(sorted(self._contract_keys)) or "<none>"
            raise KeyError(f"Unknown override key '{key}'. Valid keys: {valid}.")

    def _validate_value(self, key: str, value: Any) -> None:
        return None

    def _normalize_value(self, key: str, value: Any) -> Any:
        self._validate_value(key, value)
        return value


def _get_dag(pipeline_or_dag: Any) -> Dag:
    if hasattr(pipeline_or_dag, "dag"):
        return pipeline_or_dag.dag
    return pipeline_or_dag


class MaterializerRegistry(PipelineRegistry):
    @classmethod
    def empty(cls, pipeline: Any) -> "MaterializerRegistry":
        return cls(
            contract_keys=_materializer_contract_keys(pipeline),
            fallback_values=_materializer_fallback_values(pipeline),
        )

    @classmethod
    def from_production(cls, pipeline: Any) -> "MaterializerRegistry":
        return cls.empty(pipeline)

    def _validate_value(self, key: str, value: Any) -> None:
        if not callable(value):
            raise TypeError(f"Materializer override for step '{key}' must be callable.")


class ObserverRegistry(PipelineRegistry):
    @classmethod
    def empty(cls, pipeline: Any) -> "ObserverRegistry":
        contract_keys = _observer_contract_keys(pipeline)
        return cls(
            contract_keys=contract_keys,
            fallback_values={key: [] for key in contract_keys},
        )

    @classmethod
    def from_production(cls, pipeline: Any) -> "ObserverRegistry":
        return cls(
            contract_keys=_observer_contract_keys(pipeline),
            fallback_values=_observer_fallback_values(pipeline),
        )

    def _normalize_value(self, key: str, value: Any) -> list[ResolvedObserver]:
        if not isinstance(value, list):
            raise TypeError(
                f"Observer override for scope '{key}' must be a list of observers."
            )

        source = "pipeline" if key == PIPELINE_SCOPE else "step"
        normalized: list[ResolvedObserver] = []
        for item in value:
            if isinstance(item, ResolvedObserver):
                normalized.append(item)
            elif isinstance(item, Observer):
                normalized.append(ResolvedObserver(handler=item.handler, source=source))
            elif callable(item):
                normalized.append(ResolvedObserver(handler=item, source=source))
            else:
                raise TypeError(
                    f"Observer override for scope '{key}' must contain only callables or Observer registrations."
                )
        return normalized


class ResourceRegistry(PipelineRegistry):
    @classmethod
    def empty(cls, pipeline: Any) -> "ResourceRegistry":
        return cls(contract_keys=_resource_contract_keys(pipeline))

    @classmethod
    def from_production(cls, pipeline: Any) -> "ResourceRegistry":
        return cls.empty(pipeline)

    def _validate_value(self, key: str, value: Any) -> None:
        if value is None:
            raise TypeError(f"Resource override for key '{key}' cannot be None.")


@dataclass(frozen=True)
class ExecutionOverrides:
    materializers: MaterializerRegistry
    observers: ObserverRegistry
    resources: ResourceRegistry

    @classmethod
    def empty(cls, pipeline: Any) -> "ExecutionOverrides":
        return cls(
            materializers=MaterializerRegistry.empty(pipeline),
            observers=ObserverRegistry.empty(pipeline),
            resources=ResourceRegistry.empty(pipeline),
        )

    @classmethod
    def from_production(cls, pipeline: Any) -> "ExecutionOverrides":
        return cls(
            materializers=MaterializerRegistry.from_production(pipeline),
            observers=ObserverRegistry.from_production(pipeline),
            resources=ResourceRegistry.from_production(pipeline),
        )


def _materializer_contract_keys(pipeline: Any) -> set[str]:
    dag = _get_dag(pipeline)
    return {
        step_name
        for step_name, node in dag.steps.items()
        if node.materializer is not None
    }


def _materializer_fallback_values(pipeline: Any) -> dict[str, Any]:
    dag = _get_dag(pipeline)
    return {
        step_name: node.materializer
        for step_name, node in dag.steps.items()
        if node.materializer is not None
    }


def _observer_contract_keys(pipeline: Any) -> set[str]:
    dag = _get_dag(pipeline)
    keys = set()
    if dag.pipeline_observers:
        keys.add(PIPELINE_SCOPE)
    keys.update(step_name for step_name, node in dag.steps.items() if node.observers)
    return keys


def _observer_fallback_values(
    pipeline: Any,
) -> dict[str, list[ResolvedObserver]]:
    dag = _get_dag(pipeline)
    values: dict[str, list[ResolvedObserver]] = {}
    if dag.pipeline_observers:
        values[PIPELINE_SCOPE] = list(dag.pipeline_observers)
    for step_name, node in dag.steps.items():
        step_local = [
            observer for observer in node.observers if observer.source == "step"
        ]
        if step_local:
            values[step_name] = step_local
    return values


def _resource_contract_keys(pipeline: Any) -> set[str]:
    dag = _get_dag(pipeline)
    return set(dag.resources)
