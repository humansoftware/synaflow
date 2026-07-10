from synaflow.core.dag_builder import build_dag
from collections.abc import Iterator
import pytest
from synaflow import run
from synaflow.execution.sync_engine.executor import PipelineExecutor
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS
from tests.execution.sync_engine.corpus.error_handling import (
    error_pipeline,
    ErrorHandlingParams,
    errors_list,
)

SYNC_PACK_NAMES = (
    "sync_explicit_modes",
    "sync_linear",
    "sync_diamond",
    "sync_fibonacci",
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
    executor = PipelineExecutor(build_dag(pack.pipeline))
    if pack.exception_match:
        with pytest.raises(Exception, match=pack.exception_match):
            executor.execute(pack.input_params)
        return
    executor.execute(pack.input_params)
    for step_name, expected in pack.step_results.items():
        if build_dag(pack.pipeline).consumers_of(step_name) and (
            not build_dag(pack.pipeline).needs_materialize(step_name)
        ):
            continue
        actual = _read_step_output(
            executor.outputs, build_dag(pack.pipeline), step_name
        )
        assert actual == expected


def test_error_handling_corpus_registers_error():
    errors_list.clear()
    run(build_dag(error_pipeline), ErrorHandlingParams())
    assert errors_list == ["gen failed"]
