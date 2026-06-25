"""Build-time validation for error_threshold_absolute and error_threshold_pct."""

from typing import NamedTuple

import pytest

from synaflow import OnError, StepMode, pipeline, step


class IntListParams(NamedTuple):
    items: list[int] = [1, 2, 3]


# Each test step used in validation tests
def _each_step(items: int) -> int:
    return items


# ---------------------------------------------------------------------------
# Storage and serialization
# ---------------------------------------------------------------------------


def test_given_no_threshold_when_compiled_then_dag_node_fields_are_none():
    def fn(items: int) -> int:
        return items

    p = pipeline(
        name="test",
        params=IntListParams,
        steps=[step("s", fn=fn)],
    )
    assert p.dag.steps["s"].error_threshold_absolute is None
    assert p.dag.steps["s"].error_threshold_pct is None


def test_given_absolute_threshold_when_compiled_then_stored():
    p = pipeline(
        name="test",
        params=IntListParams,
        steps=[step("s", fn=_each_step, error_threshold_absolute=5)],
    )
    assert p.dag.steps["s"].error_threshold_absolute == 5
    assert p.dag.steps["s"].error_threshold_pct is None


def test_given_pct_threshold_when_compiled_then_stored():
    p = pipeline(
        name="test",
        params=IntListParams,
        steps=[step("s", fn=_each_step, error_threshold_pct=0.3)],
    )
    assert p.dag.steps["s"].error_threshold_absolute is None
    assert p.dag.steps["s"].error_threshold_pct == 0.3


def test_given_both_thresholds_when_compiled_then_stored():
    p = pipeline(
        name="test",
        params=IntListParams,
        steps=[
            step(
                "s",
                fn=_each_step,
                error_threshold_absolute=5,
                error_threshold_pct=0.3,
            )
        ],
    )
    assert p.dag.steps["s"].error_threshold_absolute == 5
    assert p.dag.steps["s"].error_threshold_pct == 0.3


def test_given_absolute_threshold_when_to_dict_then_present():
    p = pipeline(
        name="test",
        params=IntListParams,
        steps=[step("s", fn=_each_step, error_threshold_absolute=5)],
    )
    d = p.to_dict()
    assert d["steps"]["s"]["error_threshold_absolute"] == 5
    assert "error_threshold_pct" not in d["steps"]["s"]


def test_given_pct_threshold_when_to_dict_then_present():
    p = pipeline(
        name="test",
        params=IntListParams,
        steps=[step("s", fn=_each_step, error_threshold_pct=0.5)],
    )
    d = p.to_dict()
    assert d["steps"]["s"]["error_threshold_pct"] == 0.5
    assert "error_threshold_absolute" not in d["steps"]["s"]


def test_given_no_threshold_when_to_dict_then_fields_absent():
    p = pipeline(
        name="test",
        params=IntListParams,
        steps=[step("s", fn=_each_step)],
    )
    d = p.to_dict()
    assert "error_threshold_absolute" not in d["steps"]["s"]
    assert "error_threshold_pct" not in d["steps"]["s"]


def test_given_pct_threshold_at_1_when_to_dict_then_present():
    """1.0 is the upper bound (100% error rate)."""
    p = pipeline(
        name="test",
        params=IntListParams,
        steps=[step("s", fn=_each_step, error_threshold_pct=1.0)],
    )
    d = p.to_dict()
    assert d["steps"]["s"]["error_threshold_pct"] == 1.0


# ---------------------------------------------------------------------------
# Validation: value range
# ---------------------------------------------------------------------------


def test_given_pct_threshold_zero_when_compiled_then_raises():
    with pytest.raises(ValueError, match="error_threshold_pct must be in"):
        pipeline(
            name="test",
            params=IntListParams,
            steps=[step("s", fn=_each_step, error_threshold_pct=0.0)],
        )


def test_given_pct_threshold_negative_when_compiled_then_raises():
    with pytest.raises(ValueError, match="error_threshold_pct must be in"):
        pipeline(
            name="test",
            params=IntListParams,
            steps=[step("s", fn=_each_step, error_threshold_pct=-0.1)],
        )


def test_given_pct_threshold_above_1_when_compiled_then_raises():
    with pytest.raises(ValueError, match="error_threshold_pct must be in"):
        pipeline(
            name="test",
            params=IntListParams,
            steps=[step("s", fn=_each_step, error_threshold_pct=1.5)],
        )


def test_given_absolute_threshold_zero_when_compiled_then_raises():
    with pytest.raises(ValueError, match="error_threshold_absolute must be >= 1"):
        pipeline(
            name="test",
            params=IntListParams,
            steps=[step("s", fn=_each_step, error_threshold_absolute=0)],
        )


def test_given_absolute_threshold_negative_when_compiled_then_raises():
    with pytest.raises(ValueError, match="error_threshold_absolute must be >= 1"):
        pipeline(
            name="test",
            params=IntListParams,
            steps=[step("s", fn=_each_step, error_threshold_absolute=-3)],
        )


# ---------------------------------------------------------------------------
# Validation: STOP conflict
# ---------------------------------------------------------------------------


def test_given_absolute_threshold_with_on_error_stop_when_compiled_then_raises():
    with pytest.raises(ValueError, match="on_error=STOP"):
        pipeline(
            name="test",
            params=IntListParams,
            steps=[
                step(
                    "s",
                    fn=_each_step,
                    error_threshold_absolute=3,
                    on_error=OnError.STOP,
                )
            ],
        )


def test_given_pct_threshold_with_on_error_stop_when_compiled_then_raises():
    with pytest.raises(ValueError, match="on_error=STOP"):
        pipeline(
            name="test",
            params=IntListParams,
            steps=[
                step(
                    "s",
                    fn=_each_step,
                    error_threshold_pct=0.3,
                    on_error=OnError.STOP,
                )
            ],
        )


# ---------------------------------------------------------------------------
# Validation: ALL-mode conflict (explicit and resolved)
# ---------------------------------------------------------------------------


def test_given_explicit_all_mode_with_threshold_when_compiled_then_raises():
    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def all_step(items: list[int]) -> int:
        return sum(items)

    with pytest.raises(ValueError, match="mode=ALL"):
        pipeline(
            name="test",
            params=P,
            steps=[
                step(
                    "s",
                    fn=all_step,
                    mode=StepMode.ALL,
                    error_threshold_absolute=3,
                )
            ],
        )


def test_given_auto_resolved_to_all_with_threshold_when_compiled_then_raises():
    """A scalar step (no each-mode deps) resolves AUTO to ALL; threshold
    with ALL should still be rejected."""

    class P(NamedTuple):
        x: int = 1

    def scalar_step(x: int) -> int:
        return x + 1

    with pytest.raises(ValueError, match="mode=ALL"):
        pipeline(
            name="test",
            params=P,
            steps=[
                step(
                    "s",
                    fn=scalar_step,
                    error_threshold_absolute=3,
                )
            ],
        )


# ---------------------------------------------------------------------------
# Sub-pipeline propagation
# ---------------------------------------------------------------------------


def test_given_sub_pipeline_step_with_threshold_when_expanded_then_propagated():
    """Thresholds on sub-pipeline steps must be propagated during expansion."""

    from collections.abc import Iterator
    from synaflow import include

    class SubParams(NamedTuple):
        items: list[int] = [1, 2, 3]

    def sub_adapter(items: list[int]) -> Iterator[SubParams]:
        for x in items:
            yield SubParams(items=[x])

    def sub_step_a(items: int) -> int:
        return items

    def sub_step_b(suba: int) -> int:
        return suba

    sub_pipeline = pipeline(
        name="sub",
        params=SubParams,
        steps=[
            step("suba", fn=sub_step_a),
            step("subb", fn=sub_step_b, error_threshold_absolute=3),
        ],
        exports="subb",
    )

    class ParentParams(NamedTuple):
        items: list[int] = [1, 2, 3]

    parent = pipeline(
        name="parent",
        params=ParentParams,
        steps=[
            include("sub", pipeline=sub_pipeline, fn=sub_adapter),
        ],
    )
    # The exported step "subb" is renamed to "sub" in the parent, and the
    # threshold from the inner step is preserved on the expanded name
    assert parent.dag.steps["sub"].error_threshold_absolute == 3
    # Serialized as well
    d = parent.to_dict()
    assert d["steps"]["sub"]["error_threshold_absolute"] == 3
