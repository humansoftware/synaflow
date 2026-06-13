import pytest


from ._dag_builder_data import COMPATIBILITY_TABLE
from .conftest import build_minimal_dag


@pytest.mark.parametrize("case", COMPATIBILITY_TABLE, ids=lambda c: c["label"])
def test_given_producer_and_consumer_pair_when_dag_built_then_materialized_deps_correct(
    case,
):
    p = build_minimal_dag(
        producer_fn=case["producer_fn"],
        consumer_fn=case["consumer_fn"],
        params=case.get("params"),
    )
    consumer_node = p.dag.steps["consumer"]
    expected = case.get("expected_materialized_deps", [])
    assert consumer_node.materialized_deps == expected


@pytest.mark.parametrize("case", COMPATIBILITY_TABLE, ids=lambda c: c["label"])
def test_given_producer_and_consumer_pair_when_dag_built_then_materializer_is_set(case):
    p = build_minimal_dag(
        producer_fn=case["producer_fn"],
        consumer_fn=case["consumer_fn"],
        params=case.get("params"),
    )
    producer_node = p.dag.steps["producer"]
    if producer_node.fn is None:
        return
    from synaflow.core.type_compatibility import is_iterable_type

    if is_iterable_type(producer_node.output):
        assert producer_node.materializer is not None
