import pytest
from collections.abc import Iterator, AsyncIterator
from synaflow.core.dag_steps import validate_sync_async_consistency
from synaflow.core.dag import Dag, DagNode


def test_given_sync_only_dag_when_validated_then_sets_sync_runner():
    def sync_fn() -> Iterator[int]:
        yield 1

    node = DagNode(fn=sync_fn, output=Iterator[int], deps={})
    dag = Dag(name="test", params={}, resource_factories={}, steps={"s1": node})

    validate_sync_async_consistency(dag, "test")

    assert dag.requires_sync_runner is True
    assert dag.requires_async_runner is False


def test_given_async_only_dag_when_validated_then_sets_async_runner():
    async def async_fn() -> AsyncIterator[int]:
        yield 1

    node = DagNode(fn=async_fn, output=AsyncIterator[int], deps={})
    dag = Dag(name="test", params={}, resource_factories={}, steps={"s1": node})

    validate_sync_async_consistency(dag, "test")

    assert dag.requires_sync_runner is False
    assert dag.requires_async_runner is True


def test_given_mixed_dag_when_validated_then_raises_value_error():
    async def async_fn() -> AsyncIterator[int]:
        yield 1

    def sync_fn() -> Iterator[int]:
        yield 1

    sync_node = DagNode(fn=sync_fn, output=Iterator[int], deps={})
    async_node = DagNode(fn=async_fn, output=AsyncIterator[int], deps={})

    dag = Dag(
        name="test",
        params={},
        resource_factories={},
        steps={"s1": sync_node, "s2": async_node},
    )

    with pytest.raises(ValueError, match="UNRUNNABLE"):
        validate_sync_async_consistency(dag, "test")
