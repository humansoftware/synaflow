from __future__ import annotations

import dataclasses
from typing import Any

from synaflow.core.dag import Dag


class ExecutionState:
    """Owns the intermediate output dictionary of one execution.

    Encapsulates the producer/consumer key scheme of the DAG so
    executors never touch raw keys."""

    def __init__(self, dag: Dag) -> None:
        self._dag = dag
        self._outputs: dict[str, Any] = {}

    def seed(self, params: Any) -> None:
        """Seed the pipeline's initial input parameters."""
        if dataclasses.is_dataclass(params):
            param_dict = {
                f.name: getattr(params, f.name) for f in dataclasses.fields(params)
            }
        elif hasattr(params, "_asdict"):
            param_dict = params._asdict()
        elif isinstance(params, dict):
            param_dict = dict(params)
        else:
            raise TypeError(
                f"Pipeline params must be a NamedTuple, dataclass or dict,"
                f" got {type(params).__name__}."
            )
        for field, value in param_dict.items():
            self._outputs[field] = value

    def set_output(
        self, producer: str, value: Any, consumer: str | None = None
    ) -> None:
        """Store a step's output, optionally scoped to one consumer."""
        if consumer:
            key = self._dag.output_key(producer, consumer)
            self._outputs[key] = value
        else:
            self._outputs[producer] = value

    def get_output(self, producer: str, consumer: str) -> Any:
        """Read a producer's output as seen by one consumer."""
        key = self._dag.output_key(producer, consumer)
        return self._outputs.get(key, self._outputs.get(producer))

    def raw_outputs(self) -> dict[str, Any]:
        """Direct read access for callers that need the raw key space."""
        return self._outputs

    def inputs_available(self, step_name: str) -> bool:
        """True when every data dependency of the step has been produced."""
        node = self._dag.steps[step_name]
        for dep_name in node.deps:
            if dep_name in self._dag.resource_factories:
                continue
            key = self._dag.output_key(dep_name, step_name)
            if key not in self._outputs and dep_name not in self._outputs:
                return False
        return True
