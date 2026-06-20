from collections.abc import Iterator, MutableMapping
from dataclasses import dataclass
from typing import Any

from synaflow.core.definition import PipelineDef


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

    def __getitem__(self, key: str) -> Any:
        self._validate_key(key)
        if key in self._overrides:
            return self._overrides[key]
        if key in self._fallback_values:
            return self._fallback_values[key]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._validate_key(key)
        self._validate_value(key, value)
        self._overrides[key] = value

    def __delitem__(self, key: str) -> None:
        self._validate_key(key)
        if key not in self._overrides:
            raise KeyError(key)
        del self._overrides[key]

    def __iter__(self) -> Iterator[str]:
        return iter(sorted(self._contract_keys))

    def __len__(self) -> int:
        return len(self._contract_keys)

    def resolve(self, key: str, default: Any = None) -> Any:
        if key in self._overrides:
            return self._overrides[key]
        if key in self._fallback_values:
            return self._fallback_values[key]
        return default

    def _validate_key(self, key: str) -> None:
        if key not in self._contract_keys:
            valid = ", ".join(sorted(self._contract_keys)) or "<none>"
            raise KeyError(f"Unknown override key '{key}'. Valid keys: {valid}.")

    def _validate_value(self, key: str, value: Any) -> None:
        return None


class MaterializerRegistry(PipelineRegistry):
    @classmethod
    def empty(cls, pipeline: PipelineDef) -> "MaterializerRegistry":
        return cls(
            contract_keys=_materializer_contract_keys(pipeline),
            fallback_values=_materializer_fallback_values(pipeline),
        )

    @classmethod
    def from_production(cls, pipeline: PipelineDef) -> "MaterializerRegistry":
        return cls.empty(pipeline)

    def _validate_value(self, key: str, value: Any) -> None:
        if not callable(value):
            raise TypeError(f"Materializer override for step '{key}' must be callable.")


@dataclass(frozen=True)
class ExecutionOverrides:
    materializers: MaterializerRegistry

    @classmethod
    def empty(cls, pipeline: PipelineDef) -> "ExecutionOverrides":
        return cls(materializers=MaterializerRegistry.empty(pipeline))

    @classmethod
    def from_production(cls, pipeline: PipelineDef) -> "ExecutionOverrides":
        return cls(materializers=MaterializerRegistry.from_production(pipeline))


def _materializer_contract_keys(pipeline: PipelineDef) -> set[str]:
    return {
        step_name
        for step_name, node in pipeline.dag.steps.items()
        if node.materializer is not None
    }


def _materializer_fallback_values(pipeline: PipelineDef) -> dict[str, Any]:
    return {
        step_name: node.materializer
        for step_name, node in pipeline.dag.steps.items()
        if node.materializer is not None
    }
