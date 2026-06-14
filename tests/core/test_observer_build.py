from typing import NamedTuple

import pytest

from synaflow import (
    Observer,
    pipeline,
    step,
)
from synaflow.core.dag import _serialize_observers, _serialize_pipeline_observers


class Params(NamedTuple):
    x: int


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------


def _make_handler(name="handler"):
    def handler(ctx):
        pass

    handler.__name__ = name
    return handler


def _make_async_handler(name="async_handler"):
    async def handler(ctx):
        pass

    handler.__name__ = name
    return handler


# ---------------------------------------------------------------------------
# Normalization: pipeline-level observers inherited by all steps
# ---------------------------------------------------------------------------


def test_given_pipeline_observers_when_build_then_dag_stores_them():
    h = _make_handler("on_event")
    p = pipeline(
        name="p",
        params=Params,
        steps=[step("s", fn=lambda x: x + 1)],
        observers=[Observer(h)],
    )
    dag = p.dag
    assert len(dag.pipeline_observers) == 1
    assert dag.pipeline_observers[0].handler is h


def test_given_pipeline_observers_when_build_then_all_steps_inherit_them():
    h = _make_handler("on_event")
    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("a", fn=lambda x: x + 1),
            step("b", fn=lambda x: x + 2),
        ],
        observers=[Observer(h)],
    )
    for name in ("a", "b"):
        node_obs = p.dag[name].observers
        assert len(node_obs) >= 1
        assert any(o.handler is h for o in node_obs)


# ---------------------------------------------------------------------------
# Normalization: step-level observers
# ---------------------------------------------------------------------------


def test_given_step_observers_when_build_then_dagnode_has_them():
    h = _make_handler("on_event")
    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("a", fn=lambda x: x + 1, observers=[Observer(h)]),
        ],
    )
    node_obs = p.dag["a"].observers
    assert len(node_obs) >= 1
    assert any(o.handler is h for o in node_obs)


# ---------------------------------------------------------------------------
# Effective observer union (pipeline + step)
# ---------------------------------------------------------------------------


def test_given_pipeline_and_step_observers_when_build_then_effective_is_union():
    h_pipe = _make_handler("pipe_handler")
    h_step = _make_handler("step_handler")
    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("a", fn=lambda x: x + 1, observers=[Observer(h_step)]),
        ],
        observers=[Observer(h_pipe)],
    )
    node_obs = p.dag["a"].observers
    assert len(node_obs) == 2
    assert node_obs[0].handler is h_pipe
    assert node_obs[1].handler is h_step


def test_given_duplicate_registrations_when_build_then_not_deduplicated():
    h = _make_handler("same")
    p = pipeline(
        name="p",
        params=Params,
        steps=[
            step("a", fn=lambda x: x + 1, observers=[Observer(h)]),
        ],
        observers=[Observer(h)],
    )
    node_obs = p.dag["a"].observers
    assert len(node_obs) == 2


# ---------------------------------------------------------------------------
# DAG JSON serialization
# ---------------------------------------------------------------------------


def test_given_observers_when_dag_to_dict_then_steps_include_metadata():
    h = _make_handler("on_step")
    p = pipeline(
        name="p",
        params=Params,
        steps=[step("a", fn=lambda x: x + 1)],
        observers=[Observer(h)],
    )
    d = p.to_dict()
    step_obs = d["steps"]["a"]["observers"]
    assert step_obs == [{"handler_name": "on_step", "source": "step"}]


def test_given_observers_when_dag_to_dict_then_no_callables_serialized():
    h = _make_handler("on_step")
    p = pipeline(
        name="p",
        params=Params,
        steps=[step("a", fn=lambda x: x + 1)],
        observers=[Observer(h)],
    )
    d = p.to_dict()
    for obs in d["steps"]["a"].get("observers", []):
        assert "handler" not in obs
        assert "callable" not in str(obs)


def test_given_pipeline_observers_when_dag_to_dict_then_at_dag_level():
    h = _make_handler("on_pipe")
    p = pipeline(
        name="p",
        params=Params,
        steps=[step("a", fn=lambda x: x + 1)],
        observers=[Observer(h)],
    )
    d = p.to_dict()
    assert "pipeline_observers" in d
    assert d["pipeline_observers"] == [
        {"handler_name": "on_pipe", "source": "pipeline"}
    ]


def test_given_no_observers_when_dag_to_dict_then_no_observers_field():
    p = pipeline(
        name="p",
        params=Params,
        steps=[step("a", fn=lambda x: x + 1)],
    )
    d = p.to_dict()
    assert "observers" not in d["steps"]["a"]
    assert "pipeline_observers" not in d


# ---------------------------------------------------------------------------
# _serialize_observers and _serialize_pipeline_observers helpers
# ---------------------------------------------------------------------------


def test_serialize_observers_returns_handler_name_and_source():
    h = _make_handler("my_handler")
    obs_list = [Observer(h)]
    result = _serialize_observers(obs_list)
    assert result == [{"handler_name": "my_handler", "source": "step"}]


def test_serialize_pipeline_observers_returns_handler_name_and_pipeline_source():
    h = _make_handler("pipe_handler")
    obs_list = [Observer(h)]
    result = _serialize_pipeline_observers(obs_list)
    assert result == [{"handler_name": "pipe_handler", "source": "pipeline"}]


# ---------------------------------------------------------------------------
# Async handler validation in sync pipelines
# ---------------------------------------------------------------------------


def test_given_async_handler_in_sync_pipeline_when_build_then_validation_error():
    h = _make_async_handler("async_h")
    with pytest.raises(ValueError, match="async"):
        pipeline(
            name="p",
            params=Params,
            steps=[step("a", fn=lambda x: x + 1)],
            observers=[Observer(h)],
        )


def test_given_async_partial_handler_in_sync_pipeline_when_build_then_validation_error():
    import functools

    async def async_h(ctx):
        pass

    partial_h = functools.partial(async_h)
    with pytest.raises(ValueError, match="async"):
        pipeline(
            name="p",
            params=Params,
            steps=[step("a", fn=lambda x: x + 1)],
            observers=[Observer(partial_h)],
        )


def test_given_sync_handler_in_sync_pipeline_when_build_then_ok():
    h = _make_handler()
    p = pipeline(
        name="p",
        params=Params,
        steps=[step("a", fn=lambda x: x + 1)],
        observers=[Observer(h)],
    )
    assert p.dag is not None


def test_given_sync_pipeline_without_observers_when_build_then_ok():
    p = pipeline(
        name="p",
        params=Params,
        steps=[step("a", fn=lambda x: x + 1)],
    )
    assert p.dag is not None
