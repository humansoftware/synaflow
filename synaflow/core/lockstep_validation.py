import collections
from synaflow.core.dag import DagNode
from synaflow.core.type_compatibility import is_iterable_type


def validate_lockstep_symmetry(dag: dict[str, DagNode], pipeline_name: str) -> None:
    # 1. Build adjacency
    children = {u: [] for u in dag}
    for v, node in dag.items():
        for u in node.deps:
            if u in dag:
                children[u].append(v)

    # 2. For each node F that is a stream fanout
    for f_name, f_node in dag.items():
        if f_node.materialize_output:
            continue
        if not (f_node.output and is_iterable_type(f_node.output)):
            continue

        if len(children[f_name]) <= 1:
            continue  # not a fanout

        # Find all paths from f_name
        paths_to = collections.defaultdict(list)

        def dfs(current: str, current_path: list[str], has_barrier: bool):
            for child in children[current]:
                new_path = current_path + [child]
                new_has_barrier = has_barrier
                # If current is NOT the start node, check if it acts as a barrier
                if current != f_name and dag[current].materialize_output:
                    new_has_barrier = True

                paths_to[child].append((new_has_barrier, new_path))
                dfs(child, new_path, new_has_barrier)

        dfs(f_name, [f_name], False)

        # Check for asymmetry
        for d_name, paths in paths_to.items():
            if len(paths) <= 1:
                continue

            barriers = [b for b, p in paths]
            if any(barriers) and not all(barriers):
                raise ValueError(
                    f"Pipeline '{pipeline_name}': Asymmetric lockstep materialization detected "
                    f"between fanout '{f_name}' and join '{d_name}'. "
                    "Some paths have materialization barriers, while others are purely lazy. "
                    "This topology guarantees a deadlock in lockstep execution. "
                    f"To fix this, force materialization on '{f_name}' or ensure all paths have a barrier."
                )
