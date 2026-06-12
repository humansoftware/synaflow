from dataclasses import dataclass
from typing import Any

from synaflow.core.pipeline import PipelineDef


@dataclass(kw_only=True)
class PipelinePack:
    pipeline: PipelineDef
    input_params: Any
    step_results: dict[str, Any]
    expected_dag: dict[str, Any] | None = None
    expected_call_order: list[str] | None = None
    expected_execution_levels: list[list[str]] | None = None
    is_valid: bool = True
    exception_match: str | None = None
