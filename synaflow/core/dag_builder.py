from collections.abc import MutableMapping, MutableSequence, MutableSet
from typing import Any, NamedTuple

from synaflow.core.dag import Dag, DagNode
from synaflow.core.dag_dependencies import DependencyValidator
from synaflow.core.dag_steps import StepValidator
from synaflow.core.dag_topology import TopologyValidator
from synaflow.core.type_compatibility import is_iterable_type
from synaflow.core.types import MaterializeContext


def default_materializer_factory(ctx: MaterializeContext):
    tp = getattr(ctx.consumer_type, "__origin__", None) or ctx.consumer_type
    if tp is not None:
        try:
            if issubclass(tp, (list, MutableSequence)):
                return list
            if issubclass(tp, (set, MutableSet)):
                return set
            if issubclass(tp, (dict, MutableMapping)):
                return dict
            if issubclass(tp, tuple):
                return tuple
        except TypeError:
            pass
    return list


class DagBuilder:
    @staticmethod
    def _validate_params_is_namedtuple(params: Any, pipeline_name: str) -> None:
        if not hasattr(params, "_fields"):
            raise ValueError(
                f"Pipeline '{pipeline_name}': 'params' must be a NamedTuple, got {type(params).__name__}"
            )

    @staticmethod
    def _add_parameter_nodes_to_dag(
        dag: dict[str, DagNode], produced: dict[str, DagNode]
    ) -> None:
        for name, info in list(produced.items()):
            if name not in dag:
                dag[name] = info

    @classmethod
    def _resolve_materializers(
        cls, dag: dict[str, DagNode], pipeline_factory: Any
    ) -> None:
        for node in dag.values():
            if node.output and is_iterable_type(node.output) and node.fn:
                mat = node.materializer or pipeline_factory
                if mat is None:
                    raise ValueError(
                        f"Node: no materializer resolved for iterable output"
                    )
                node.materializer = mat
            else:
                node.materializer = None

    @classmethod
    def _compute_materialized_deps(cls, dag: dict[str, DagNode]) -> None:
        from synaflow.core.type_compatibility import is_materialized_consumer
        from synaflow.core.types import OnError

        for node in dag.values():
            if node.fn is None:
                node.materialized_deps = []
                continue

            materialized_deps = []
            for dep_name, dep_type in node.deps.items():
                if is_materialized_consumer(dep_type):
                    materialized_deps.append(dep_name)
                elif dep_name in dag and dag[dep_name].on_error == OnError.STOP:
                    materialized_deps.append(dep_name)
            node.materialized_deps = materialized_deps

        for name, node in dag.items():
            consumers = [
                other_name
                for other_name, other_node in dag.items()
                if name in other_node.materialized_deps
            ]
            needs_mat = len(consumers) > 0 or node.on_error == OnError.STOP
            node.needs_materialize = needs_mat

    @classmethod
    def build(
        cls,
        pipeline_name: str,
        params: type[NamedTuple],
        steps: list[Any],
        default_materializer_factory: Any = None,
        is_default_factory: bool = False,
    ) -> Dag:
        cls._validate_params_is_namedtuple(params, pipeline_name)

        factory = default_materializer_factory

        dag: dict[str, DagNode] = {}
        dag_obj = Dag()

        from synaflow.core.dag_expansion import expand_macros

        expanded_steps = expand_macros(steps, current_pipeline_name=pipeline_name)

        produced = DependencyValidator.initialize_parameters(params)

        for step in expanded_steps:
            StepValidator.validate_step_is_callable(step, pipeline_name)
            StepValidator.validate_unique_step_name(step.name, dag, pipeline_name)

            compiled_step = StepValidator.validate_and_compile_step(
                step, produced, pipeline_name
            )
            dag[step.name] = compiled_step
            produced[step.name] = compiled_step

        cls._add_parameter_nodes_to_dag(dag, produced)
        cls._resolve_materializers(dag, factory)
        cls._compute_materialized_deps(dag)

        dag_obj.nodes = dag

        TopologyValidator.check_circular_dependencies(dag_obj, pipeline_name)

        StepValidator.validate_sync_async_consistency(
            dag_obj,
            pipeline_name,
            steps,
            default_materializer_factory,
            is_default_factory=is_default_factory,
        )

        return dag_obj
