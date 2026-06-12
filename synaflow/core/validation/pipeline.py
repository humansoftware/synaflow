from typing import Any, NamedTuple

from synaflow.core.step import Step
from synaflow.core.validation.dependencies import DependencyValidator
from synaflow.core.validation.steps import StepValidator
from synaflow.core.validation.topology import TopologyValidator


class PipelineValidator:
    @staticmethod
    def validate_params_is_namedtuple(params: Any, pipeline_name: str) -> None:
        if not hasattr(params, "_fields"):
            raise ValueError(
                f"Pipeline '{pipeline_name}': 'params' must be a NamedTuple, got {type(params).__name__}"
            )

    @staticmethod
    def add_parameter_nodes_to_dag(dag: dict, produced: dict) -> None:
        """Nodes that are parameters need to be explicitly added to the final DAG so they can be processed."""
        for name, info in list(produced.items()):
            if name not in dag:
                info["fn"] = None
                info["deps"] = {}
                info["on_error"] = None
                info["needs_materialize"] = False
                dag[name] = info

    @classmethod
    def validate_pipeline(
        cls,
        pipeline_name: str,
        params: type[NamedTuple],
        steps: list[Any],
        default_materializer_factory: Any = None,
    ) -> dict:
        cls.validate_params_is_namedtuple(params, pipeline_name)
        dag: dict[str, dict] = {}

        from .macro_expansion import MacroExpander

        expanded_steps = MacroExpander.expand(steps)

        produced = DependencyValidator.initialize_parameters(params)

        for step in expanded_steps:
            StepValidator.validate_step_is_callable(step, pipeline_name)
            StepValidator.validate_unique_step_name(step.name, dag, pipeline_name)

            compiled_step = StepValidator.validate_and_compile_step(
                step, produced, pipeline_name
            )
            dag[step.name] = compiled_step
            produced[step.name] = compiled_step

        cls.add_parameter_nodes_to_dag(dag, produced)

        TopologyValidator.check_circular_dependencies(dag, pipeline_name)
        TopologyValidator.compute_needs_materialize(dag)

        StepValidator.validate_sync_async_consistency(
            dag, pipeline_name, steps, default_materializer_factory
        )

        return dag
