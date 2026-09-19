"""Built-in in-memory materializer factory (list/set/dict/tuple by
consumer protocol)."""

from synaflow.core.dag_builder import memory_materializer_factory


def memory_materializer():
    return memory_materializer_factory
