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

        # Track, per descendant, the set of barrier statuses observed among
        # all paths from f_name. The downstream check only needs to know
        # whether both True and False appear (asymmetry), so we never need
        # the full paths themselves.
        barriers_seen: dict[str, set[bool]] = collections.defaultdict(set)

        # Memoize the walk on (current, barrier_status): re-entering the
        # same subtree with the same barrier produces the same set of
        # descendant barriers, so we can skip it. barrier_status is bool
        # → at most 2 entries per node per fanout, turning the original
        # O(2^N)-in-diamond-depth path enumeration into O(N).
        visited: set[tuple[str, bool]] = set()

        def dfs(current: str, has_barrier: bool) -> None:
            key = (current, has_barrier)
            if key in visited:
                return
            visited.add(key)
            for child in children[current]:
                new_has_barrier = has_barrier
                if current != f_name:
                    if dag[current].materialize_output or not is_iterable_type(
                        dag[current].output
                    ):
                        new_has_barrier = True
                barriers_seen[child].add(new_has_barrier)
                dfs(child, new_has_barrier)

        dfs(f_name, False)

        # Check for asymmetry
        for d_name, statuses in barriers_seen.items():
            if len(statuses) <= 1:
                continue
            # Both True and False present → some paths barrier, others not.
            if True in statuses and False in statuses:
                raise ValueError(
                    f"Pipeline '{pipeline_name}': Asymmetric lockstep materialization detected "
                    f"between fanout '{f_name}' and join '{d_name}'. "
                    "Some paths have materialization barriers, while others are purely lazy. "
                    "This topology guarantees a deadlock in lockstep execution. "
                    f"To fix this, force materialization on '{f_name}' or ensure all paths have a barrier."
                )
