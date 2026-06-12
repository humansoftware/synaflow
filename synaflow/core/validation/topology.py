from synaflow.core.type_compatibility import is_materialized_consumer
from synaflow.core.types import OnError


class TopologyValidator:
    @staticmethod
    def check_circular_dependencies(dag: dict, pipeline_name: str) -> None:
        visited = set()
        stack = set()

        def dfs(node: str) -> bool:
            if node in stack:
                return True
            if node in visited:
                return False

            visited.add(node)
            stack.add(node)

            for dep in dag.get(node, {}).get("deps", {}):
                if dfs(dep):
                    return True

            stack.remove(node)
            return False

        for node in dag:
            if dfs(node):
                raise ValueError(
                    f"Pipeline '{pipeline_name}' has a circular dependency involving '{node}'"
                )

    @staticmethod
    def compute_needs_materialize(dag: dict) -> None:
        for name, node in dag.items():
            consumers = [
                other_name
                for other_name, other_node in dag.items()
                if name in other_node.get("deps", {})
            ]

            node["needs_materialize"] = (
                any(
                    is_materialized_consumer(dag[consumer_name]["deps"][name])
                    for consumer_name in consumers
                    if consumer_name in dag
                    and name in dag[consumer_name].get("deps", {})
                )
                or node.get("on_error") == OnError.STOP
            )
