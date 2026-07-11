"""Command-line interface for synaflow.

Layering: this module is a thin adapter over the public Python API.
It does NOT live inside ``synaflow.core``; importing it from the core
package would invert the dependency direction. Callers that want a
CLI-friendly error vocabulary use ``main()``; code that wants raw
Python exceptions uses ``PipelineRegistry.from_module`` directly.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import sys
from collections.abc import Sequence
from typing import Any

from synaflow.core.dag import Dag
from synaflow.core.definition import PipelineDef
from synaflow.core.pipeline_registry import PipelineRegistry
from synaflow.execution.async_engine.executor import async_run
from synaflow.execution.overrides import ExecutionOverrides
from synaflow.execution.sync_engine.executor import run


class CLIUsageError(Exception):
    """User-input error in the CLI.

    Caught at the ``main()`` boundary, printed as ``synaflow: <message>``,
    and converted to exit code 1. Internal exceptions (programmer
    errors) are NOT caught here -- they propagate as tracebacks.
    """


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


class SynaflowCli:
    """Run a fixed :class:`PipelineRegistry` through the standard CLI.

    Projects use this class when their catalog is known in application code.
    The module-level ``main`` function remains the adapter for the packaged
    command, where ``--catalog`` selects that registry dynamically.
    """

    def __init__(self, *, catalog: PipelineRegistry) -> None:
        if not isinstance(catalog, PipelineRegistry):
            raise TypeError(
                f"catalog must be a PipelineRegistry, got {type(catalog).__name__}"
            )
        self._catalog = catalog

    def main(self, argv: Sequence[str] | None = None) -> int:
        """Parse ``argv`` and run a command against the fixed catalog."""
        tokens = list(argv) if argv is not None else sys.argv[1:]
        return _main_with_catalog(tokens, self._catalog, catalog_required=False)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the packaged CLI, which requires ``--catalog MODULE``.

    Returns 0 for success, 1 for ``CLIUsageError``. Argparse's own
    ``--help`` (SystemExit 0) and parse errors (SystemExit 2) propagate
    naturally -- that's standard argparse behavior.
    """
    try:
        tokens = list(argv) if argv is not None else sys.argv[1:]
        bootstrap = argparse.ArgumentParser(add_help=False)
        bootstrap.add_argument("--catalog")
        provisional, _ = bootstrap.parse_known_args(tokens)
        if provisional.catalog is None:
            _build_parser(params_type=None, catalog_required=True).parse_args(tokens)
            raise AssertionError("argparse should have rejected missing --catalog")
        return _main_with_catalog(
            tokens,
            _load_catalog(provisional.catalog),
            catalog_required=True,
        )
    except CLIUsageError as exc:
        print(f"synaflow: {exc}", file=sys.stderr)
        return 1


def _main_with_catalog(
    tokens: list[str],
    catalog: PipelineRegistry,
    *,
    catalog_required: bool,
) -> int:
    try:
        run_pipeline = _resolve_run_pipeline(tokens, catalog)
        params_type = run_pipeline.params if run_pipeline else None
        args = _build_parser(
            params_type=params_type,
            catalog_required=catalog_required,
        ).parse_args(tokens)
        return _dispatch(args, catalog=catalog)
    except CLIUsageError as exc:
        print(f"synaflow: {exc}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser(
    *,
    params_type: Any | None = None,
    catalog_required: bool = True,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="synaflow",
        description="Inspect and run synaflow pipelines.",
        allow_abbrev=False,
    )
    if catalog_required:
        parser.add_argument(
            "--catalog",
            required=True,
            metavar="MODULE",
            help=(
                "Python module exposing `catalog = PipelineRegistry()` at top "
                "level. The module must be importable from the current "
                "PYTHONPATH."
            ),
        )

    sub = parser.add_subparsers(dest="subcommand", required=True)

    p_list = sub.add_parser(
        "list", help="List registered pipelines.", allow_abbrev=False
    )
    p_list.add_argument("--json", action="store_true", help="Output as JSON.")

    p_info = sub.add_parser(
        "info", help="Show declared pipeline details.", allow_abbrev=False
    )
    p_info.add_argument("name")
    p_info.add_argument("--json", action="store_true", help="Output as JSON.")

    p_dag = sub.add_parser(
        "dag",
        help="Show the compiled Dag as JSON (compiles on demand).",
        allow_abbrev=False,
    )
    p_dag.add_argument("name")

    p_run = sub.add_parser("run", help="Run the pipeline.", allow_abbrev=False)
    p_run.add_argument("name")
    p_run.add_argument(
        "--params-file",
        metavar="PATH",
        default=None,
        help="Path to a JSON object with params field values.",
    )
    p_run.add_argument(
        "--no-observers",
        action="store_true",
        help="Disable all pipeline and step observers for this run.",
    )
    if params_type is not None:
        _add_direct_param_flags(p_run, params_type)
    return parser


# ---------------------------------------------------------------------------
# Subcommand dispatch
# ---------------------------------------------------------------------------


def _dispatch(
    args: argparse.Namespace,
    catalog: PipelineRegistry,
) -> int:
    sub = args.subcommand
    if sub == "list":
        return _cmd_list(catalog, args)
    if sub == "info":
        return _cmd_info(catalog, args)
    if sub == "dag":
        return _cmd_dag(catalog, args)
    if sub == "run":
        return _cmd_run(catalog, args)
    raise CLIUsageError(f"unknown subcommand: {sub!r}")  # pragma: no cover


# ---------------------------------------------------------------------------
# Catalog loader (the CLI-side adapter for from_module exceptions)
# ---------------------------------------------------------------------------


def _load_catalog(module_name: str) -> PipelineRegistry:
    """Adapt ``PipelineRegistry.from_module`` exceptions to CLIUsageError.

    Module-not-found rule:
        For --catalog myproject.pipelines, Python may raise
        ModuleNotFoundError with exc.name == "myproject" (the parent
        package is missing), not "myproject.pipelines". Both cases are
        "catalog absent" from the user's perspective. We treat the
        catalog as missing when:

            exc.name == module_name
            or module_name.startswith(f"{exc.name}.")  # parent pkg

        Only propagate when the missing module is unrelated to the
        catalog (a transitive dependency of the catalog module).
    """
    try:
        return PipelineRegistry.from_module(module_name)
    except ModuleNotFoundError as exc:
        missing_catalog_module = exc.name == module_name or module_name.startswith(
            f"{exc.name}."
        )
        if missing_catalog_module:
            raise CLIUsageError(f"cannot find catalog module {module_name!r}") from exc
        raise  # real error -- a transitive dependency is missing
    except AttributeError as exc:
        raise CLIUsageError(
            f"module {module_name!r} has no attribute 'catalog'"
        ) from exc
    except TypeError as exc:
        # The core raised TypeError with the actual value type name in
        # the message ("...got {type(value).__name__}"). Reuse it
        # verbatim; do NOT recompute type(exc).__name__ -- that would
        # print "TypeError" since `exc` IS a TypeError.
        raise CLIUsageError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def _cmd_list(catalog: PipelineRegistry, args: argparse.Namespace) -> int:
    rows = [{"name": name, "steps": len(catalog[name].steps)} for name in catalog]
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        for row in rows:
            print(f"{row['name']}\t{row['steps']} steps")
    return 0


def _cmd_info(catalog: PipelineRegistry, args: argparse.Namespace) -> int:
    p = _resolve_pipeline(catalog, args.name)
    params_repr = _format_params_type(p.params)
    step_names = [s.name for s in p.steps]
    exports_str = p.exports if p.exports else "<none>"
    info: dict[str, Any] = {
        "name": p.name,
        "params": params_repr,
        "exports": p.exports,
        "steps": step_names,
    }
    if args.json:
        print(json.dumps(info, indent=2))
    else:
        print(f"name: {p.name}")
        print(f"params: {params_repr}")
        print(f"exports: {exports_str}")
        print(f"steps ({len(step_names)}): {', '.join(step_names)}")
    return 0


def _cmd_dag(catalog: PipelineRegistry, args: argparse.Namespace) -> int:
    dag = _resolve_dag(catalog, args.name)
    print(json.dumps(dag.to_dict(), indent=2))
    return 0


def _cmd_run(catalog: PipelineRegistry, args: argparse.Namespace) -> int:
    p = _resolve_pipeline(catalog, args.name)
    dag = _resolve_dag(catalog, args.name)
    file_values = _load_params_file(args.params_file) if args.params_file else {}
    direct_flag_values = _direct_param_values(p.params, args)
    params = _build_params(p.params, file_values, direct_flag_values)
    overrides = (
        ExecutionOverrides.without_observers(p)
        if getattr(args, "no_observers", False)
        else None
    )
    if dag.requires_async_runner:
        asyncio.run(async_run(dag, params, overrides=overrides))
    else:
        run(dag, params, overrides=overrides)
    return 0


# ---------------------------------------------------------------------------
# Catalog / Dag resolution (shared by info, dag, run)
# ---------------------------------------------------------------------------


def _resolve_run_pipeline(
    tokens: list[str],
    catalog: PipelineRegistry,
) -> PipelineDef | None:
    """Load the selected run pipeline before building its direct flags.

    The bootstrap parser intentionally knows only the command shape. That
    lets ``run NAME --help`` discover ``NAME`` and render the final parser
    with the pipeline's own parameter flags.
    """
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--catalog")
    bootstrap.add_argument("subcommand", nargs="?")
    bootstrap.add_argument("name", nargs="?")
    provisional, _ = bootstrap.parse_known_args(tokens)
    if provisional.subcommand != "run" or provisional.name is None:
        return None
    return _resolve_pipeline(catalog, provisional.name)


def _resolve_pipeline(catalog: PipelineRegistry, name: str) -> PipelineDef:
    try:
        return catalog[name]
    except KeyError as exc:
        available = ", ".join(sorted(catalog)) or "<none>"
        raise CLIUsageError(
            f"pipeline {name!r} not registered. Available: {available}"
        ) from exc


def _resolve_dag(catalog: PipelineRegistry, name: str) -> Dag:
    try:
        return catalog.get_dag(name)
    except KeyError as exc:
        available = ", ".join(sorted(catalog)) or "<none>"
        raise CLIUsageError(
            f"pipeline {name!r} not registered. Available: {available}"
        ) from exc


# ---------------------------------------------------------------------------
# Params parsing
# ---------------------------------------------------------------------------


_RUN_RESERVED_PARAM_FIELDS = {"no_observers", "params_file"}


def _add_direct_param_flags(
    parser: argparse.ArgumentParser,
    params_type: Any,
) -> None:
    """Add ``--field-name VALUE`` flags for a pipeline's params fields."""
    valid, _ = _params_field_names(params_type)
    reserved = sorted(valid & _RUN_RESERVED_PARAM_FIELDS)
    if reserved:
        raise CLIUsageError(
            f"Pipeline params cannot use reserved CLI field(s): {reserved}"
        )
    for field_name in sorted(valid):
        parser.add_argument(
            f"--{field_name.replace('_', '-')}",
            dest=field_name,
            default=argparse.SUPPRESS,
            metavar="VALUE",
            type=_parse_param_value,
        )


def _direct_param_values(params_type: Any, args: argparse.Namespace) -> dict:
    """Extract explicitly supplied direct flags from an argparse namespace."""
    if params_type is None:
        return {}
    valid, _ = _params_field_names(params_type)
    values = vars(args)
    return {field_name: values[field_name] for field_name in valid & values.keys()}


def _load_params_file(path: str) -> dict:
    """Load a JSON object from ``path`` and return it as a dict."""
    try:
        with open(path) as f:
            data = json.load(f)
    except OSError as exc:
        raise CLIUsageError(f"cannot read params file {path!r}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise CLIUsageError(
            f"cannot read params file {path!r}: invalid JSON ({exc.msg} "
            f"at line {exc.lineno} col {exc.colno})"
        ) from exc
    if not isinstance(data, dict):
        raise CLIUsageError(
            f"params file {path!r} must contain a JSON object, got "
            f"{type(data).__name__}"
        )
    return data


def _parse_param_value(value: str) -> Any:
    """Parse a CLI value as JSON when possible; otherwise retain text."""
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _params_field_names(params_type: Any) -> tuple[set[str], set[str]]:
    """Return ``(valid, required)`` field names for a params class.

    Supports only NamedTuple and ``@dataclass`` -- the same contract
    as ``build_dag``. Raises CLIUsageError if the class is neither.
    """
    if hasattr(params_type, "_fields"):
        valid = set(params_type._fields)
        defaults = set(params_type._field_defaults.keys())
        return valid, valid - defaults
    if dataclasses.is_dataclass(params_type):
        valid: set[str] = set()
        required: set[str] = set()
        for f in dataclasses.fields(params_type):
            valid.add(f.name)
            if (
                f.default is dataclasses.MISSING
                and f.default_factory is dataclasses.MISSING
            ):
                required.add(f.name)
        return valid, required
    raise CLIUsageError(
        f"Pipeline params must be a NamedTuple or @dataclass; got {params_type!r}"
    )


def _build_params(params_type: Any, file_values: dict, flag_values: dict) -> Any:
    """Construct a params instance from file + flag values.

    ``flag_values`` OVERRIDE ``file_values``. Defaults from
    NamedTuple ``_field_defaults`` / dataclass ``default`` /
    ``default_factory`` are respected -- a field with a default is NOT
    required at the CLI.

    Raises CLIUsageError on unknown / missing / unconstructable params.
    """
    valid, required = _params_field_names(params_type)
    merged = {**file_values, **flag_values}
    unknown = sorted(set(merged) - valid)
    if unknown:
        raise CLIUsageError(
            f"Unknown params field(s) for {params_type.__name__}: {unknown}"
        )
    missing = sorted(required - set(merged))
    if missing:
        raise CLIUsageError(
            f"Missing required params field(s) for {params_type.__name__}: {missing}"
        )
    try:
        return params_type(**merged)
    except (TypeError, ValueError) as exc:
        raise CLIUsageError(
            f"Cannot construct {params_type.__name__} from provided params: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------


def _format_params_type(params: Any) -> str:
    """Return a short repr for the params type, for ``info`` output."""
    if params is None:
        return "<none>"
    # NamedTuple, dataclass, or plain class with __name__.
    name = getattr(params, "__name__", None)
    if name:
        return name
    return repr(params)
