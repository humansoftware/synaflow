from __future__ import annotations

import dataclasses
from typing import Any

from synaflow.core.dag import Dag


class ExecutionState:
    """Gerencia e encapsula o dicionário de saídas intermediárias de uma execução.

    Abstrai a geração de chaves complexas da DAG.
    """

    def __init__(self, dag: Dag) -> None:
        self._dag = dag
        self._outputs: dict[str, Any] = {}

    def seed(self, params: Any) -> None:
        """Popula os parâmetros de entrada iniciais da pipeline."""
        if dataclasses.is_dataclass(params):
            param_dict = {
                f.name: getattr(params, f.name) for f in dataclasses.fields(params)
            }
        else:
            param_dict = params._asdict()
        for field, value in param_dict.items():
            self._outputs[field] = value

    def set_output(
        self, producer: str, value: Any, consumer: str | None = None
    ) -> None:
        """Salva a saída de um passo, opcionalmente associada a um consumidor específico."""
        if consumer:
            key = self._dag.output_key(producer, consumer)
            self._outputs[key] = value
        else:
            self._outputs[producer] = value

    def get_output(self, producer: str, consumer: str) -> Any:
        """Obtém a saída de um produtor para um consumidor específico."""
        key = self._dag.output_key(producer, consumer)
        return self._outputs.get(key, self._outputs.get(producer))

    def raw_outputs(self) -> dict[str, Any]:
        """Mantém compatibilidade de leitura direta caso necessário."""
        return self._outputs

    def inputs_available(self, step_name: str) -> bool:
        """Verifica se todas as dependências de dados de um passo já foram produzidas."""
        node = self._dag.steps[step_name]
        for dep_name in node.deps:
            if dep_name in self._dag.resource_factories:
                continue
            key = self._dag.output_key(dep_name, step_name)
            if key not in self._outputs and dep_name not in self._outputs:
                return False
        return True
