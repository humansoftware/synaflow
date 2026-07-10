"""Tests for the synaflow CLI.

Two layers:

1. **Subcommand tests** (subprocess via ``python -m synaflow``): verify
   the full CLI path -- argument parsing, catalog loading, subcommand
   dispatch, and output formatting.

2. **Error-translation tests** (in-process via ``main(argv) -> int``):
   verify that user-input errors become friendly messages + exit 1,
   and that internal exceptions propagate as tracebacks (regression
   guard).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import types
from pathlib import Path
from typing import NamedTuple
from unittest import mock

import pytest

from synaflow import PipelineRegistry, cli, pipeline, step
from synaflow.cli import CLIUsageError, main
from tests.cli.conftest import SYNATEST_CATALOG_NAME


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_subprocess(*args: str, tmp_path: Path) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(tmp_path) + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.run(
        [sys.executable, "-m", "synaflow", "--catalog", "my_catalog", *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
    )


def _run_subprocess_inprocess_catalog(
    *args: str,
) -> subprocess.CompletedProcess:
    """Run CLI using the synthetic in-process catalog module."""
    return subprocess.run(
        [sys.executable, "-m", "synaflow", "--catalog", SYNATEST_CATALOG_NAME, *args],
        capture_output=True,
        text=True,
    )


def _write_params_file(tmp_path: Path, body: dict) -> Path:
    p = tmp_path / "params.json"
    p.write_text(json.dumps(body))
    return p


# ---------------------------------------------------------------------------
# Subcommand tests (subprocess)
# ---------------------------------------------------------------------------


def test_given_list_then_prints_pipeline_names(tmp_catalog_dir):
    result = _run_subprocess("list", tmp_path=tmp_catalog_dir)
    assert result.returncode == 0, result.stderr
    assert "hello" in result.stdout


def test_given_list_json_then_outputs_json(tmp_catalog_dir):
    result = _run_subprocess("list", "--json", tmp_path=tmp_catalog_dir)
    assert result.returncode == 0, result.stderr
    rows = json.loads(result.stdout)
    assert isinstance(rows, list)
    assert any(r["name"] == "hello" for r in rows)


def test_given_info_then_prints_declared_steps_not_expanded(tmp_catalog_dir):
    result = _run_subprocess("info", "hello", tmp_path=tmp_catalog_dir)
    assert result.returncode == 0, result.stderr
    # One declared step, before expansion.
    assert "s" in result.stdout
    # The declared step count is 1, not whatever the expansion produces.
    assert "1" in result.stdout


def test_given_dag_then_outputs_dag_json(tmp_catalog_dir):
    result = _run_subprocess("dag", "hello", tmp_path=tmp_catalog_dir)
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["name"] == "hello"
    assert "s" in data["steps"]


def test_given_validate_on_valid_then_exits_zero(tmp_catalog_dir):
    result = _run_subprocess("validate", "hello", tmp_path=tmp_catalog_dir)
    assert result.returncode == 0, result.stderr


def test_given_validate_on_invalid_then_exits_one(tmp_catalog_dir):
    # Unknown pipeline name -> CLI exits 1 with a friendly message.
    result = _run_subprocess("validate", "nope", tmp_path=tmp_catalog_dir)
    assert result.returncode == 1
    assert "nope" in result.stderr


def test_given_run_with_params_file_then_executes(tmp_catalog_dir):
    params_path = _write_params_file(tmp_catalog_dir, {"x": 7})
    result = _run_subprocess(
        "run", "hello", "--params-file", str(params_path), tmp_path=tmp_catalog_dir
    )
    assert result.returncode == 0, result.stderr


def test_given_run_with_param_flag_overrides_file_then_executes(tmp_catalog_dir):
    # file says x=1, --param x=99 overrides to 99.
    params_path = _write_params_file(tmp_catalog_dir, {"x": 1})
    result = _run_subprocess(
        "run",
        "hello",
        "--params-file",
        str(params_path),
        "--param",
        "x=99",
        tmp_path=tmp_catalog_dir,
    )
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Error-translation tests (in-process)
# ---------------------------------------------------------------------------


def test_given_missing_catalog_module_then_exits_one_with_friendly_message(capsys):
    result = main(["--catalog", "definitely_not_a_real_module_xyz", "list"])
    captured = capsys.readouterr()
    assert result == 1
    assert "definitely_not_a_real_module_xyz" in captured.err


def test_given_catalog_module_without_catalog_attr_then_exits_one(capsys):
    # `os` is a real module that exists but has no `catalog` attribute.
    result = main(["--catalog", "os", "list"])
    captured = capsys.readouterr()
    assert result == 1
    assert "catalog" in captured.err


def test_given_unknown_pipeline_name_then_exits_one(capsys):
    result = main(["--catalog", SYNATEST_CATALOG_NAME, "info", "nope"])
    captured = capsys.readouterr()
    assert result == 1
    assert "nope" in captured.err
    assert "hello" in captured.err  # available list mentions the only registered


def test_given_missing_params_file_then_exits_one(capsys, tmp_path):
    nonexistent = tmp_path / "does_not_exist.json"
    result = main(
        [
            "--catalog",
            SYNATEST_CATALOG_NAME,
            "run",
            "hello",
            "--params-file",
            str(nonexistent),
        ]
    )
    captured = capsys.readouterr()
    assert result == 1
    assert "params file" in captured.err


def test_given_invalid_params_json_then_exits_one(capsys, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ this is not json")
    result = main(
        [
            "--catalog",
            SYNATEST_CATALOG_NAME,
            "run",
            "hello",
            "--params-file",
            str(bad),
        ]
    )
    captured = capsys.readouterr()
    assert result == 1
    assert "params file" in captured.err


def test_given_internal_exception_then_propagates_with_traceback():
    """Regression guard: programmer errors must NOT be swallowed by the
    CLI's CLIUsageError catch. They should propagate as tracebacks so
    they can be diagnosed."""

    class FakeRegistry(PipelineRegistry):
        def get_dag(self, name):  # type: ignore[override]
            raise RuntimeError("internal programmer error")

    def fn() -> int:
        return 1

    boom_pipeline = pipeline(name="boom", params=None, steps=[step("s", fn=fn)])

    fake_module = types.ModuleType("fake_module_with_boom")
    fake_module.catalog = FakeRegistry()
    fake_module.catalog["boom"] = boom_pipeline
    sys.modules["fake_module_with_boom"] = fake_module
    try:
        with pytest.raises(RuntimeError, match="internal programmer error"):
            main(["--catalog", "fake_module_with_boom", "dag", "boom"])
    finally:
        del sys.modules["fake_module_with_boom"]


# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------


def test_cli_module_exposes_main_callable():
    assert callable(cli.main)
    assert issubclass(CLIUsageError, Exception)


# ---------------------------------------------------------------------------
# Coverage tests (in-process)
#
# These exercise the subcommand handlers and helpers directly so they
# show up in the test process's coverage report. The subprocess tests
# above verify end-to-end CLI behavior but don't contribute to coverage.
# ---------------------------------------------------------------------------


def test_given_list_handler_then_prints_names(capsys):
    """In-process: cli._cmd_list text path."""

    args = argparse.Namespace(json=False)
    rc = cli._cmd_list(_fake_catalog(), args)
    captured = capsys.readouterr()
    assert rc == 0
    assert "hello" in captured.out


def test_given_list_handler_json_then_outputs_json(capsys):
    """In-process: cli._cmd_list --json path."""

    args = argparse.Namespace(json=True)
    rc = cli._cmd_list(_fake_catalog(), args)
    captured = capsys.readouterr()
    assert rc == 0
    rows = json.loads(captured.out)
    assert any(r["name"] == "hello" for r in rows)


def test_given_info_handler_text_then_prints_declared_steps(capsys):
    """In-process: cli._cmd_info text path."""

    args = argparse.Namespace(name="hello", json=False)
    rc = cli._cmd_info(_fake_catalog(), args)
    captured = capsys.readouterr()
    assert rc == 0
    assert "hello" in captured.out
    assert "s" in captured.out


def test_given_info_handler_json_then_outputs_json(capsys):
    """In-process: cli._cmd_info --json path."""

    args = argparse.Namespace(name="hello", json=True)
    rc = cli._cmd_info(_fake_catalog(), args)
    captured = capsys.readouterr()
    assert rc == 0
    info = json.loads(captured.out)
    assert info["name"] == "hello"
    assert "s" in info["steps"]


def test_given_dag_handler_then_prints_dag_json(capsys):
    """In-process: cli._cmd_dag path."""

    args = argparse.Namespace(name="hello")
    rc = cli._cmd_dag(_fake_catalog(), args)
    captured = capsys.readouterr()
    assert rc == 0
    data = json.loads(captured.out)
    assert data["name"] == "hello"


def test_given_validate_handler_then_exits_zero(capsys):
    """In-process: cli._cmd_validate success path."""

    args = argparse.Namespace(name="hello")
    rc = cli._cmd_validate(_fake_catalog(), args)
    assert rc == 0


def test_given_run_handler_sync_then_executes(capsys, tmp_path):
    """In-process: cli._cmd_run sync path."""

    params_path = _write_params_file(tmp_path, {"x": 7})
    args = argparse.Namespace(name="hello", params_file=str(params_path), param=[])
    rc = cli._cmd_run(_fake_catalog(), args)
    assert rc == 0


def test_given_load_catalog_type_error_then_raises_cli_usage_error():
    """In-process: cli._load_catalog with a wrong-typed catalog attribute."""

    fake_module = types.ModuleType("fake_wrong_type")
    fake_module.catalog = "not a registry"  # type: ignore[attr-defined]
    sys.modules["fake_wrong_type"] = fake_module
    try:
        with pytest.raises(CLIUsageError, match="PipelineRegistry"):
            cli._load_catalog("fake_wrong_type")
    finally:
        del sys.modules["fake_wrong_type"]


def test_given_load_params_file_non_dict_then_raises_cli_usage_error(tmp_path):

    bad = tmp_path / "list_only.json"
    bad.write_text("[1, 2, 3]")
    with pytest.raises(CLIUsageError, match="JSON object"):
        cli._load_params_file(str(bad))


def test_given_parse_param_flags_missing_equals_then_raises_cli_usage_error():

    with pytest.raises(CLIUsageError, match="key=value"):
        cli._parse_param_flags(["nokv"])


def test_given_parse_param_flags_empty_key_then_raises_cli_usage_error():

    with pytest.raises(CLIUsageError, match="empty key"):
        cli._parse_param_flags(["=value"])


def test_given_parse_param_flags_json_value_then_parses_value():

    result = cli._parse_param_flags(["count=42", 'name="x"', "raw=hello"])
    assert result == {"count": 42, "name": "x", "raw": "hello"}


def test_given_build_params_dataclass_then_respects_defaults():
    """Dataclass path: defaults make fields optional."""

    @dataclasses.dataclass
    class P:
        x: int = 0
        y: int = 1

    # No values at all -> both defaults apply.
    result = cli._build_params(P, {}, {})
    assert result == P(x=0, y=1)


def test_given_build_params_named_tuple_then_respects_required_and_default():
    """NamedTuple path: _field_defaults make fields optional."""

    class P(NamedTuple):
        x: int  # required
        y: int = 5  # default

    # Only x provided -> y uses default.
    result = cli._build_params(P, {"x": 1}, {})
    assert result == P(x=1, y=5)


def test_given_build_params_unknown_field_then_raises_cli_usage_error():

    class P(NamedTuple):
        x: int = 0

    with pytest.raises(CLIUsageError, match="Unknown"):
        cli._build_params(P, {"unknown_field": 1}, {})


def test_given_build_params_missing_required_then_raises_cli_usage_error():

    class P(NamedTuple):
        x: int  # required, no default

    with pytest.raises(CLIUsageError, match="Missing"):
        cli._build_params(P, {}, {})


def test_given_build_params_non_namedtuple_or_dataclass_then_raises_cli_usage_error():

    class NotParams:
        pass

    with pytest.raises(CLIUsageError, match="NamedTuple"):
        cli._build_params(NotParams, {}, {})


def test_given_resolve_dag_validation_failure_then_raises_cli_usage_error():
    """Dag with invalid structure -> ValueError from build_dag -> CLIUsageError."""

    def fn() -> int:
        return 1

    bad = pipeline(name="bad", params=None, steps=[step("s", fn=fn)])
    # Mutate to an invalid state: pass None instead of a real DagNode.
    # Easiest: use a Dag with two steps sharing an output key that
    # collides. Simpler: directly assert the ValueError path.

    class FakeRegistry(PipelineRegistry):
        def get_dag(self, name):  # type: ignore[override]
            # Simulate build_dag raising ValueError.
            raise ValueError("simulated design-time validation failure")

    reg = FakeRegistry()
    reg["bad"] = bad
    with pytest.raises(CLIUsageError, match="failed validation"):
        cli._resolve_dag(reg, "bad")


def test_given_resolve_pipeline_unknown_then_raises_cli_usage_error():

    reg = PipelineRegistry()
    with pytest.raises(CLIUsageError, match="not registered"):
        cli._resolve_pipeline(reg, "missing")


def test_given_load_catalog_transitive_dep_missing_then_propagates():
    """Module exists but a transitive dep of the module is missing ->
    Python raises ModuleNotFoundError with exc.name != module_name ->
    the CLI propagates (not a 'catalog missing' error).

    We simulate this by monkeypatching importlib.import_module to
    raise ModuleNotFoundError with a name that doesn't match the
    module we're loading.
    """
    fake_exc = ModuleNotFoundError("No module named 'some_unrelated_dep'")
    fake_exc.name = "some_unrelated_dep"
    # The actual import happens in PipelineRegistry.from_module, which
    # uses synaflow.core.pipeline_registry.importlib.import_module.
    with mock.patch(
        "synaflow.core.pipeline_registry.importlib.import_module",
        side_effect=fake_exc,
    ):
        with pytest.raises(ModuleNotFoundError, match="some_unrelated_dep"):
            cli._load_catalog("myproject.pipelines")


# ---------------------------------------------------------------------------
# Helper: a fresh PipelineRegistry seeded with the in-process catalog
# ---------------------------------------------------------------------------


def _fake_catalog() -> PipelineRegistry:
    """Build a fresh registry from the in-process synthetic catalog.

    Returns a NEW registry each call so tests can't accidentally
    share cached dags across runs.
    """
    mod = sys.modules[SYNATEST_CATALOG_NAME]
    reg = PipelineRegistry()
    reg["hello"] = mod._synatest_pipeline  # type: ignore[attr-defined]
    return reg
