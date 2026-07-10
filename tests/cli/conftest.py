"""Shared fixtures for CLI tests.

The subprocess-based tests write a temp catalog module to ``tmp_path``
and invoke ``python -m synaflow --catalog my_catalog ...`` with
``PYTHONPATH`` set to ``tmp_path``.

The in-process error-translation tests need a real module to be
importable, so we install a synthetic module into ``sys.modules`` here.
This module is visible to any test that imports it by the constant
name ``SYNATEST_CATALOG_NAME``.
"""

from __future__ import annotations

import sys
import textwrap
import types
from collections.abc import Iterator
from pathlib import Path

import pytest


SYNATEST_CATALOG_NAME = "synaflow_cli_test_catalog"
SYNATEST_CATALOG_BODY = textwrap.dedent(
    """
    from typing import NamedTuple
    from synaflow import PipelineRegistry, pipeline, step


    class P(NamedTuple):
        x: int = 0


    def fn(x: int) -> int:
        return x


    _synatest_pipeline = pipeline(
        name="hello", params=P, steps=[step("s", fn=fn)]
    )

    catalog = PipelineRegistry()
    catalog["hello"] = _synatest_pipeline
    """
)


def _install_synthetic_catalog() -> types.ModuleType:
    mod = types.ModuleType(SYNATEST_CATALOG_NAME)
    exec(
        compile(SYNATEST_CATALOG_BODY, f"<{SYNATEST_CATALOG_NAME}>", "exec"),
        mod.__dict__,
    )
    sys.modules[SYNATEST_CATALOG_NAME] = mod
    return mod


# Install at import time so in-process error-translation tests
# can reference the module without any fixture wiring.
_install_synthetic_catalog()


@pytest.fixture
def tmp_catalog_dir(tmp_path: Path) -> Iterator[Path]:
    """Write a catalog module to ``tmp_path`` and yield the path.

    Use this with subprocess invocations that need to import the
    catalog via ``--catalog my_catalog``. The subprocess is expected
    to be launched with ``cwd=tmp_path`` and ``PYTHONPATH=tmp_path``.
    """
    (tmp_path / "my_catalog.py").write_text(SYNATEST_CATALOG_BODY)
    yield tmp_path
