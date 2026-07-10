"""Tests for the ``synaflow`` console script entry point.

Three guarantees:

1. pyproject.toml declares ``[project.scripts] synaflow = "synaflow.cli:main"``.
   Text-based assertion (Python 3.10+ compat; tomllib is 3.11+).

2. The installed package exposes a ``console_scripts`` entry point named
   ``synaflow`` whose ``load()`` resolves to the SAME callable as
   ``synaflow.cli.main``. This is what ``pip install`` / ``uv sync``
   registers when the project is installed.

3. The entry point function (== ``synaflow.cli.main``) honors argparse's
   ``--help`` contract by raising ``SystemExit(0)``.
"""

from __future__ import annotations

import importlib.metadata
import re
from pathlib import Path

import pytest

from synaflow.cli import main


PYPROJECT_TOML = Path(__file__).resolve().parents[2] / "pyproject.toml"


def test_given_pyproject_toml_then_synaflow_console_script_declared():
    """pyproject.toml [project.scripts] declares the entry point.

    Text-based assertion so the test works on Python 3.10+ without
    depending on the 3.11-only ``tomllib`` module.
    """
    body = PYPROJECT_TOML.read_text()
    assert "[project.scripts]" in body, (
        "pyproject.toml is missing the [project.scripts] table"
    )
    match = re.search(
        r'^\s*synaflow\s*=\s*"synaflow\.cli:main"\s*$',
        body,
        re.MULTILINE,
    )
    assert match is not None, (
        "pyproject.toml [project.scripts] does not declare "
        'synaflow = "synaflow.cli:main"'
    )


def _find_synaflow_entry_point() -> object | None:
    """Return the ``synaflow`` console_scripts entry point, or None.

    Looks at the installed ``synaflow`` distribution's metadata directly
    because editable installs (uv's PEP 660 mode) don't always register
    entry points in the global ``entry_points()`` index.
    """
    try:
        dist = importlib.metadata.distribution("synaflow")
    except importlib.metadata.PackageNotFoundError:
        return None
    for ep in dist.entry_points:
        if ep.group == "console_scripts" and ep.name == "synaflow":
            return ep
    return None


def test_given_installed_package_then_synaflow_entry_point_loads_to_main():
    """After installation (``uv sync`` / ``pip install -e .``),
    the installed ``synaflow`` distribution exposes a ``synaflow``
    console_scripts entry point whose ``load()`` returns the same
    callable object as ``synaflow.cli.main``.
    """
    ep = _find_synaflow_entry_point()
    assert ep is not None, (
        "no console_scripts entry point named 'synaflow' found on the "
        "installed synaflow distribution. pyproject.toml must declare "
        "'[project.scripts] synaflow = \"synaflow.cli:main\"' and the "
        "package must be installed in this env (`uv sync`)."
    )
    loaded = ep.load()
    assert loaded is main, (
        f"entry point 'synaflow' resolves to {loaded!r}, "
        f"expected {main!r} (synaflow.cli.main)"
    )


def test_given_synaflow_entry_point_help_then_systemexit_zero():
    """End-to-end: the entry-point function (== ``synaflow.cli.main``)
    honors argparse's ``--help`` contract by raising ``SystemExit(0)``.
    """
    ep = _find_synaflow_entry_point()
    if ep is None:
        pytest.skip(
            "synaflow console script not installed in this env; run `uv sync` first"
        )
    main_fn = ep.load()
    with pytest.raises(SystemExit) as exc_info:
        main_fn(["--help"])
    assert exc_info.value.code == 0
