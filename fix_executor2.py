import re
from pathlib import Path

p_sync = Path("synaflow/execution/sync_engine/executor.py")
content = p_sync.read_text()

# Fix single consumer wrapper
content = re.sub(
    r'if len\(consumers\) == 1:\n\s+output = BoundedStreamWrapper\(output, getattr\(node, "max_in_flight", 1\)\)',
    'if len(consumers) == 1 and getattr(node, "max_in_flight", 1) > 1:\n            output = BoundedStreamWrapper(output, getattr(node, "max_in_flight", 1))',
    content
)

# Fix bounded tee
bounded_tee_new = """        def bounded_tee(iterable, n, max_in_flight):
            it = iter(iterable)
            deques = [collections.deque() for _ in range(n)]

            def gen(mydeque):
                while True:
                    if not mydeque:
                        for d in deques:
                            if d is not mydeque and len(d) >= max_in_flight:
                                raise RuntimeError(
                                    f"max_in_flight bound of {max_in_flight} exceeded during sync fan-out."
                                )
                        try:
                            newval = next(it)
                        except StopIteration:
                            return
                        for d in deques:
                            d.append(newval)
                    yield mydeque.popleft()

            return tuple(gen(d) for d in deques)

        max_in_flight = getattr(node, "max_in_flight", 1)
        if max_in_flight == 1:
            branches = itertools.tee(output, len(consumers))
        else:
            branches = bounded_tee(output, len(consumers), max_in_flight)

        for consumer, branch in zip(consumers, branches):
            consumer_node = self.dag[consumer]
            if step_name in consumer_node.materialized_deps:
                branch, _, _ = self._materialize_with_events(
                    step_name,
                    branch,
                    node,
                    consumer_type=consumer_node.deps.get(step_name),
                )
            else:
                if max_in_flight > 1:
                    branch = BoundedStreamWrapper(branch, max_in_flight)
            self.outputs[self.dag.output_key(step_name, consumer)] = branch"""

content = re.sub(r'        def bounded_tee\(iterable, n, max_in_flight\):.*?(?=\n    def _publish_scalar_output)', bounded_tee_new, content, flags=re.DOTALL)

p_sync.write_text(content)
