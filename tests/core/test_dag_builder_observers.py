from typing import NamedTuple
import pytest

from synaflow import (
    pipeline,
    step,
    include,
    PipelineEvent,
    StepEvent,
    Observer,
)


class SimpleParams(NamedTuple):
    x: int = 1


def dummy_handler(ctx):
    pass


def dummy_step(x: int) -> int:
    return x


def test_given_pipeline_with_observers_when_compiled_then_observers_resolved():
    obs = Observer(StepEvent.FAILED, dummy_handler)
    p = pipeline(
        name="test_pipeline",
        params=SimpleParams,
        observers=[obs],
        steps=[step("s1", fn=dummy_step)],
    )

    # The pipeline-level StepEvent observer must propagate to all step nodes as source="pipeline"
    node = p.dag["s1"]
    assert len(node.observers) == 1
    resolved = node.observers[0]
    assert resolved.event == StepEvent.FAILED
    assert resolved.handler == dummy_handler
    assert resolved.source == "pipeline"


def test_given_step_with_observers_when_compiled_then_observers_resolved():
    obs = Observer(StepEvent.COMPLETED, dummy_handler)
    p = pipeline(
        name="test_pipeline",
        params=SimpleParams,
        steps=[step("s1", fn=dummy_step, observers=[obs])],
    )

    # Step-level observer must be on the node as source="step"
    node = p.dag["s1"]
    assert len(node.observers) == 1
    resolved = node.observers[0]
    assert resolved.event == StepEvent.COMPLETED
    assert resolved.handler == dummy_handler
    assert resolved.source == "step"


def test_given_both_pipeline_and_step_observers_when_compiled_then_union_resolved():
    p_obs = Observer(StepEvent.FAILED, dummy_handler)
    s_obs = Observer(StepEvent.COMPLETED, dummy_handler)
    p = pipeline(
        name="test_pipeline",
        params=SimpleParams,
        observers=[p_obs],
        steps=[step("s1", fn=dummy_step, observers=[s_obs])],
    )

    node = p.dag["s1"]
    assert len(node.observers) == 2
    assert node.observers[0].event == StepEvent.FAILED
    assert node.observers[0].source == "pipeline"
    assert node.observers[1].event == StepEvent.COMPLETED
    assert node.observers[1].source == "step"


def test_given_sub_pipeline_when_compiled_then_observers_propagated_correctly():
    p_obs = Observer(StepEvent.FAILED, dummy_handler)
    sub_obs = Observer(StepEvent.COMPLETED, dummy_handler)

    sub_pipe = pipeline(
        name="sub",
        params=SimpleParams,
        observers=[sub_obs],
        steps=[step("s1", fn=dummy_step)],
        exports="s1",
    )

    def adapter(x: int) -> SimpleParams:
        return SimpleParams(x=x)

    main_pipe = pipeline(
        name="main",
        params=SimpleParams,
        observers=[p_obs],
        steps=[include("incl", pipeline=sub_pipe, fn=adapter)],
    )

    # Sub-pipeline step must have both the sub-pipeline's and parent's observers
    node = main_pipe.dag["incl"]
    assert len(node.observers) == 2
    # Parent (main) pipeline observer first
    assert node.observers[0].event == StepEvent.FAILED
    assert node.observers[0].source == "pipeline"
    # Sub pipeline observer second
    assert node.observers[1].event == StepEvent.COMPLETED
    assert node.observers[1].source == "pipeline"


def test_given_dag_when_to_dict_then_observer_metadata_serialized():
    obs = Observer(StepEvent.FAILED, dummy_handler)
    p = pipeline(
        name="test_pipeline",
        params=SimpleParams,
        observers=[obs],
        steps=[step("s1", fn=dummy_step)],
    )

    serialized = p.to_dict()
    node_data = serialized["steps"]["s1"]
    assert "observers" in node_data
    assert node_data["observers"] == [{"event": "step_failed", "source": "pipeline"}]


def test_given_invalid_observer_when_compiled_then_raises_validation_error():
    # 1. Non-Observer in list
    with pytest.raises(TypeError):
        pipeline(
            name="test_pipeline",
            params=SimpleParams,
            observers=[lambda x: x],
            steps=[step("s1", fn=dummy_step)],
        )

    # 2. PipelineEvent on a step
    with pytest.raises(ValueError):
        pipeline(
            name="test_pipeline",
            params=SimpleParams,
            steps=[
                step(
                    "s1",
                    fn=dummy_step,
                    observers=[Observer(PipelineEvent.STARTED, dummy_handler)],
                )
            ],
        )
