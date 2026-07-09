from synaflow.core.dag_builder import build_dag
from collections.abc import Iterator
from concurrent.futures import Future
from typing import AsyncIterator, NamedTuple
from synaflow import pipeline, step
from synaflow.core.dag import ConsumerContract
from synaflow.core.types import StepMode


class Empty(NamedTuple):
    pass


def test_given_scalar_step_when_dag_built_then_output_contract_and_publish_plan_are_compiled():

    def scalar() -> int:
        return 1

    p = pipeline(name="test", params=Empty, steps=[step("scalar", fn=scalar)])
    node = build_dag(p).steps["scalar"]
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
    node = build_dag(p).steps["producer"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "sync_stream"
    assert node.output_contract.completion_policy == "on_exhaustion"
    assert node.output_contract.drain_policy == "none"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_sync_fanout"
    assert node.publish_plan.handoff == "sync_fanout"
    assert sorted(
        (
            (contract.consumer_name, contract.consumption)
            for contract in node.consumer_contracts
        )
    ) == [("a", "stream"), ("b", "stream")]


def test_given_single_sync_stream_with_default_parallelism_when_dag_built_then_publish_stream_has_no_handoff():

    def producer() -> Iterator[int]:
        yield from range(3)

    def consumer(producer: Iterator[int]) -> list[int]:
        return list(producer)

    p = pipeline(
        name="test",
        params=Empty,
        steps=[step("producer", fn=producer), step("consumer", fn=consumer)],
    )
    node = build_dag(p).steps["producer"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "sync_stream"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_stream"
    assert node.publish_plan.handoff == "none"


def test_given_single_sync_stream_with_parallelism_when_dag_built_then_publish_stream_uses_bounded_handoff():

    def producer() -> Iterator[int]:
        yield from range(3)

    def consumer(producer: Iterator[int]) -> list[int]:
        return list(producer)

    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step("producer", fn=producer, max_in_flight=4),
            step("consumer", fn=consumer),
        ],
    )
    node = build_dag(p).steps["producer"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "sync_stream"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_stream"
    assert node.publish_plan.handoff == "bounded_iterator"
    assert node.publish_plan.max_in_flight == 4


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
    node = build_dag(p).steps["producer"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "sync_stream"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_materialized"
    assert node.consumer_contracts == [
        ConsumerContract(consumer_name="consumer", consumption="materialized")
    ]


async def _async_producer() -> AsyncIterator[int]:
    for i in range(3):
        yield i


async def _async_consumer(producer: AsyncIterator[int]) -> list[int]:
    return [item async for item in producer]


async def _async_consumer_a(producer: AsyncIterator[int]) -> list[int]:
    return [item async for item in producer]


async def _async_consumer_b(producer: AsyncIterator[int]) -> list[int]:
    return [item async for item in producer]


def test_given_async_stream_single_consumer_when_dag_built_then_publish_stream_contract_is_compiled():
    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step("producer", fn=_async_producer),
            step("consumer", fn=_async_consumer),
        ],
    )
    node = build_dag(p).steps["producer"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "async_stream"
    assert node.output_contract.completion_policy == "on_exhaustion"
    assert node.output_contract.drain_policy == "none"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_stream"
    assert node.publish_plan.handoff == "none"


def test_given_async_stream_fanout_when_dag_built_then_async_fanout_plan_is_compiled():
    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step("producer", fn=_async_producer),
            step("a", fn=_async_consumer_a),
            step("b", fn=_async_consumer_b),
        ],
    )
    node = build_dag(p).steps["producer"]
    assert node.output_contract is not None
    assert node.output_contract.runtime_kind == "async_stream"
    assert node.publish_plan is not None
    assert node.publish_plan.strategy == "publish_async_fanout"
    assert node.publish_plan.handoff == "async_queue"
    assert sorted(
        (
            (contract.consumer_name, contract.consumption)
            for contract in node.consumer_contracts
        )
    ) == [("a", "stream"), ("b", "stream")]


def test_given_each_consumer_when_dag_built_then_consumer_contract_is_item():

    def producer() -> Iterator[int]:
        yield from range(3)

    def consume(producer: int) -> None:
        return None

    p = pipeline(
        name="test",
        params=Empty,
        steps=[
            step("producer", fn=producer),
            step("consume", fn=consume, mode=StepMode.EACH),
        ],
    )
    node = build_dag(p).steps["producer"]
    assert node.consumer_contracts == [
        ConsumerContract(consumer_name="consume", consumption="item")
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
        node = build_dag(p).steps[step_name]
        assert node.output_contract is not None
        assert node.output_contract.runtime_kind == "sync_stream"
        assert node.output_contract.drain_policy == "barrier_only"
        assert node.publish_plan is not None
        assert node.publish_plan.strategy == "publish_value"
        assert node.consumer_contracts == [
            ConsumerContract(consumer_name="done", consumption="barrier_only")
        ]


def test_given_scalar_step_when_dag_built_then_dag_does_not_drain_it():

    def scalar() -> int:
        return 1

    p = pipeline(name="test", params=Empty, steps=[step("scalar", fn=scalar)])
    assert build_dag(p).should_drain_deferred_step("scalar") is False


def test_given_dag_exported_when_serialized_then_execution_plan_debug_fields_are_present():

    def producer() -> Iterator[int]:
        yield 1

    def consumer(producer: list[int]) -> int:
        return len(producer)

    exported = build_dag(
        pipeline(
            name="test",
            params=Empty,
            steps=[step("producer", fn=producer), step("consumer", fn=consumer)],
        )
    ).to_dict()
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
