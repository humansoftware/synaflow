from collections.abc import Iterator
from typing import NamedTuple

import pytest

from synaflow import pipeline, step, run
from synaflow.core.types import OnError
from synaflow.execution.sync_engine.executor import PipelineExecutor
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS

SYNC_PACK_NAMES = (
    "sync_explicit_modes",
    "sync_linear",
    "sync_diamond",
    "sync_complex_parallel",
    "sync_fibonacci",
    "sync_complex_parallel_mixed",
    "sync_mixed_fanout",
    "sync_max_in_flight_threadpool",
    "sync_sub_pipelines",
    "sync_deep_sub_pipelines",
    "sync_error_handling",
)


def _concrete(value):
    """Convert generators to lists; leave scalars and tuples alone."""
    if isinstance(value, Iterator):
        items = []
        try:
            for item in value:
                items.append(item)
        except Exception:
            return items
        return items
    return value


def _read_step_output(outputs, dag, step_name):
    value = outputs.get(step_name)
    if value is None:
        consumers = dag.consumers_of(step_name)
        if consumers:
            key = f"{step_name}__{consumers[0]}"
            value = outputs.get(key)
    if value is None:
        return None
    return _concrete(value)


@pytest.mark.parametrize("pack_name", SYNC_PACK_NAMES)
def test_step_results(pack_name):
    pack = SYNC_PACKS[pack_name]

    recorded = {}

    def record_step_output(step_name, output):
        recorded[step_name] = _concrete(output)

    executor = PipelineExecutor(
        pack.pipeline.dag, step_output_observers=[record_step_output]
    )

    if pack.exception_match:
        with pytest.raises(Exception, match=pack.exception_match):
            executor.execute(pack.input_params)
        return

    executor.execute(pack.input_params)

    for step_name, expected in pack.step_results.items():
        actual = _read_step_output(recorded, pack.pipeline.dag, step_name)
        assert actual == expected


def test_error_handling_corpus_registers_error():
    from tests.execution.sync_engine.corpus.error_handling import (
        error_pipeline,
        ErrorHandlingParams,
        errors_list,
    )

    errors_list.clear()
    run(error_pipeline, ErrorHandlingParams())
    assert errors_list == ["gen failed"]


def test_given_mixed_fanout_when_observed_then_producer_observer_sees_stream_values_once():
    class P(NamedTuple):
        count: int = 3

    def gen(count: int):
        yield from range(count)

    def lazy(gen: Iterator[int]):
        return list(gen)

    def eager(gen: list[int]):
        return gen

    recorded = {}

    def record_step_output(step_name, output):
        recorded[step_name] = output

    my_pipeline = pipeline(
        name="observer_mixed_fanout_sync",
        params=P,
        steps=[
            step("gen", fn=gen),
            step("lazy", fn=lazy),
            step("eager", fn=eager),
        ],
    )

    executor = PipelineExecutor(
        my_pipeline.dag, step_output_observers=[record_step_output]
    )
    executor.execute(P())

    assert _concrete(recorded["gen"]) == [0, 1, 2]
    assert recorded["lazy"] == [0, 1, 2]
    assert recorded["eager"] == [0, 1, 2]


def test_given_partial_stream_iteration_error_with_continue_when_observed_then_observer_sees_preserved_items():
    class P(NamedTuple):
        pass

    recorded = {}

    def record_step_output(step_name, output):
        recorded[step_name] = output

    def source():
        yield 1
        raise ValueError("iterboom")

    def sink(source: list[int]):
        return source

    my_pipeline = pipeline(
        name="observer_partial_sync",
        params=P,
        steps=[
            step("source", fn=source, on_error=OnError.CONTINUE),
            step("sink", fn=sink),
        ],
    )

    executor = PipelineExecutor(
        my_pipeline.dag, step_output_observers=[record_step_output]
    )
    executor.execute(P())

    assert _concrete(recorded["source"]) == [1]
    assert recorded["sink"] == [1]
