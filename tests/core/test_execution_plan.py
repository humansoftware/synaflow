from collections.abc import Iterator
from concurrent.futures import Future
from typing import NamedTuple

from synaflow import pipeline, step
from synaflow.core.dag import ConsumerContract


class Empty(NamedTuple):
    pass


def test_given_scalar_step_when_dag_built_then_output_contract_and_publish_plan_are_compiled():
    def scalar() -> int:
        return 1

    p = pipeline(name="test", params=Empty, steps=[step("scalar", fn=scalar)])

    node = p.dag.steps["scalar"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "value"
    assert node.output_contract.completion_policy == "immediate"
    assert node.output_contract.drain_policy == "none"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_value"
    assert node.publish_plan.handoff == "none"


def test_given_lazy_fanout_when_dag_built_then_consumer_contracts_and_publish_plan_are_compiled():
    def producer() -> Iterator[int]:
        yield from range(3)

    def a(producer: Iterator[int]) -> list[int]:
        return list(producer)

    def b(producer: Iterator[int]) -> list[int]:
        return list(producer)

    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step("producer", fn=producer, max_in_flight=2),
            step("a", fn=a),
            step("b", fn=b),
        ],
    )

    node = p.dag.steps["producer"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "sync_stream"
    assert node.output_contract.completion_policy == "on_exhaustion"
    assert node.output_contract.drain_policy == "none"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_sync_fanout"
    assert node.publish_plan.handoff == "sync_fanout"
    assert sorted(
        (contract.consumer_name, contract.consumption)
        for contract in node.consumer_contracts
    ) == [("a", "stream"), ("b", "stream")]


def test_given_materialized_consumer_when_dag_built_then_publish_plan_is_materialized():
    def producer() -> Iterator[int]:
        yield from range(3)

    def consumer(producer: list[int]) -> int:
        return len(producer)

    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("producer", fn=producer), step("consumer", fn=consumer)],
    )

    node = p.dag.steps["producer"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "sync_stream"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_materialized"
    assert node.consumer_contracts == [
        ConsumerContract(consumer_name="consumer", consumption="materialized")
    ]


def test_given_barrier_only_deferred_step_when_dag_built_then_drain_policy_is_compiled():
    def source() -> Iterator[int]:
        yield from [1, 2]

    def submit_a(source: int) -> Future:
        future: Future = Future()
        future.set_result(source * 10)
        return future

    def submit_b(source: int) -> Future:
        future: Future = Future()
        future.set_result(source * 100)
        return future

    def await_a(submit_a: Future) -> None:
        submit_a.result()

    def await_b(submit_b: Future) -> None:
        submit_b.result()

    def done(await_a, await_b) -> None:
        pass

    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step("source", fn=source),
            step("submit_a", fn=submit_a, max_in_flight=2),
            step("submit_b", fn=submit_b, max_in_flight=2),
            step("await_a", fn=await_a),
            step("await_b", fn=await_b),
            step("done", fn=done),
        ],
    )

    for step_name in ("await_a", "await_b"):
        node = p.dag.steps[step_name]
        assert node.output_contract is not None
        assert node.output_contract.runtime_kind == "sync_stream"
        assert node.output_contract.drain_policy == "barrier_only"
        assert node.publish_plan is not None
        assert node.publish_plan.strategy == "publish_value"
        assert node.consumer_contracts == [
            ConsumerContract(consumer_name="done", consumption="barrier_only")
        ]


def test_given_dag_exported_when_serialized_then_execution_plan_debug_fields_are_present():
    def producer() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    exported = pipeline(
        name="test",
        params=Empty,
        steps=[step("producer", fn=producer), step("consumer", fn=consumer)],
    ).dag.to_dict()

    producer_export = exported["steps"]["producer"]
    assert producer_export["output_contract"] == {
        "runtime_kind": "sync_stream",
        "completion_policy": "on_exhaustion",
        "drain_policy": "none",
    }
    assert producer_export["publish_plan"] == {
        "strategy": "publish_materialized",
        "handoff": "none",
        "max_in_flight": 1,
    }
    assert producer_export["consumer_contracts"] == [
        {"consumer_name": "consumer", "consumption": "materialized"}
    ]
