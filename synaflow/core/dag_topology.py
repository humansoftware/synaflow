from synaflow.core.dag import Dag, DagNode


def check_circular_dependencies(dag: Dag, pipeline_name: str) -> None:
    visited = set()
    stack = set()

    def dfs(node: str) -> bool:
        if node in stack:
            return True
        if node in visited:
            return False

        visited.add(node)
        stack.add(node)

        for dep in dag.steps.get(node, DagNode()).deps:
            if dfs(dep):
                return True

        stack.remove(node)
        return False

    for node in dag.steps:
        if dfs(node):
            raise ValueError(
                f"Pipeline '{pipeline_name}' has a circular dependency involving '{node}'"
            )
