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
import base64
import dataclasses
import json
import sys
from collections.abc import Sequence
from typing import Any, Callable, Literal, get_args, get_origin, get_type_hints

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


@dataclasses.dataclass(frozen=True)
class PreRunContext:
    pipeline: PipelineDef
    dag: Dag
    params: Any
    observers_enabled: bool


@dataclasses.dataclass(frozen=True)
class RunOutcome:
    status: Literal["succeeded", "failed"]
    error: BaseException | None


@dataclasses.dataclass(frozen=True)
class PostRunContext:
    pipeline: PipelineDef
    dag: Dag
    params: Any
    observers_enabled: bool
    outcome: RunOutcome


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


class SynaflowCli:
    """Run a fixed :class:`PipelineRegistry` through the standard CLI.

    Projects use this class when their catalog is known in application code.
    The module-level ``main`` function remains the adapter for the packaged
    command, where ``--catalog`` selects that registry dynamically.
    """

    def __init__(
        self,
        *,
        catalog: PipelineRegistry,
        pre_run: Callable[[PreRunContext], Any] | None = None,
        post_run: Callable[[PostRunContext], None] | None = None,
    ) -> None:
        if not isinstance(catalog, PipelineRegistry):
            raise TypeError(
                f"catalog must be a PipelineRegistry, got {type(catalog).__name__}"
            )
        self._catalog = catalog
        self._pre_run = pre_run
        self._post_run = post_run

    def main(self, argv: Sequence[str] | None = None) -> int:
        """Parse ``argv`` and run a command against the fixed catalog."""
        tokens = list(argv) if argv is not None else sys.argv[1:]
        return _main_with_catalog(
            tokens,
            self._catalog,
            catalog_required=False,
            pre_run=self._pre_run,
            post_run=self._post_run,
        )


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
            pre_run=None,
            post_run=None,
        )
    except CLIUsageError as exc:
        print(f"synaflow: {exc}", file=sys.stderr)
        return 1


def _main_with_catalog(
    tokens: list[str],
    catalog: PipelineRegistry,
    *,
    catalog_required: bool,
    pre_run: Callable[[PreRunContext], Any] | None,
    post_run: Callable[[PostRunContext], None] | None,
) -> int:
    try:
        run_pipeline = _resolve_run_pipeline(tokens, catalog)
        params_type = run_pipeline.params if run_pipeline else None
        args = _build_parser(
            params_type=params_type,
            catalog_required=catalog_required,
        ).parse_args(tokens)
        return _dispatch(args, catalog=catalog, pre_run=pre_run, post_run=post_run)
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
    *,
    pre_run: Callable[[PreRunContext], Any] | None = None,
    post_run: Callable[[PostRunContext], None] | None = None,
) -> int:
    sub = args.subcommand
    if sub == "list":
        return _cmd_list(catalog, args)
    if sub == "info":
        return _cmd_info(catalog, args)
    if sub == "dag":
        return _cmd_dag(catalog, args)
    if sub == "run":
        return _cmd_run(catalog, args, pre_run=pre_run, post_run=post_run)
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


def _cmd_run(
    catalog: PipelineRegistry,
    args: argparse.Namespace,
    *,
    pre_run: Callable[[PreRunContext], Any] | None = None,
    post_run: Callable[[PostRunContext], None] | None = None,
) -> int:
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
    observers_enabled = overrides is None
    if pre_run is not None:
        params = pre_run(PreRunContext(p, dag, params, observers_enabled))
        if not isinstance(params, p.params):
            raise TypeError(
                f"pre_run must return {p.params.__name__}, got {type(params).__name__}"
            )
    try:
        if dag.requires_async_runner:
            asyncio.run(async_run(dag, params, overrides=overrides))
        else:
            run(dag, params, overrides=overrides)
    except BaseException as pipeline_error:
        if post_run is not None:
            try:
                post_run(
                    PostRunContext(
                        p,
                        dag,
                        params,
                        observers_enabled,
                        RunOutcome("failed", pipeline_error),
                    )
                )
            except BaseException as hook_error:
                raise hook_error from pipeline_error
        raise
    if post_run is not None:
        post_run(
            PostRunContext(
                p,
                dag,
                params,
                observers_enabled,
                RunOutcome("succeeded", None),
            )
        )
    return 0


# ---------------------------------------------------------------------------
# Catalog / Dag resolution (shared by info, dag, run)
# ---------------------------------------------------------------------------


# Flags accepted before the positional ``run NAME`` that consume the next
# token as their value. The bootstrap parser below must skip flag + value
# pairs so the positional extraction is not confused by a value-looking
# argument.
_PRE_NAME_VALUE_FLAGS = frozenset({"--catalog", "--params-file"})


def _resolve_run_pipeline(
    tokens: list[str],
    catalog: PipelineRegistry,
) -> PipelineDef | None:
    """Load the selected run pipeline before building its direct flags.

    The bootstrap is a manual scan, not an ``argparse`` parser, because
    of a quirk in ``argparse``: when two consecutive positionals are
    declared with ``nargs="?"`` (``subcommand`` then ``name``) and an
    unknown flag appears between them, argparse silently stops consuming
    the second positional and shoves everything after the unknown flag
    into the leftover list. Concretely:

        ["run", "--no-observers", "P", "--x", "1"]
        #  -> subcommand="run" name=None leftover=[...]   (BUG)

    The manual scan tolerates any flags in any position, including
    ones the bootstrap parser cannot know about (the typed param flags
    are per-pipeline and only registered later, on the main parser).
    """
    sub = name = None
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok in _PRE_NAME_VALUE_FLAGS and i + 1 < len(tokens):
            i += 2
            continue
        if tok.startswith("-"):
            i += 1
            continue
        if sub is None:
            sub = tok
        elif name is None:
            name = tok
            break
        i += 1
    if sub != "run" or name is None:
        return None
    return _resolve_pipeline(catalog, name)


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
    for field_name, field_type in sorted(_params_field_types(params_type).items()):
        direct_type = _direct_param_type(field_type)
        if direct_type is None:
            continue
        option = f"--{field_name.replace('_', '-')}"
        if direct_type is bool:
            parser.add_argument(
                option,
                dest=field_name,
                action=argparse.BooleanOptionalAction,
                default=argparse.SUPPRESS,
            )
        elif get_origin(direct_type) is list:
            parser.add_argument(
                option,
                dest=field_name,
                action="append",
                default=argparse.SUPPRESS,
                metavar="VALUE",
                type=_parse_direct_value(get_args(direct_type)[0]),
            )
        else:
            parser.add_argument(
                option,
                dest=field_name,
                default=argparse.SUPPRESS,
                metavar="VALUE",
                type=_parse_direct_value(direct_type),
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


def _params_field_types(params_type: Any) -> dict[str, Any]:
    try:
        return get_type_hints(params_type)
    except (NameError, TypeError):
        return dict(params_type.__annotations__)


def _direct_param_type(field_type: Any) -> Any | None:
    if field_type in {str, int, float, bool, bytes}:
        return field_type
    if get_origin(field_type) is list and get_args(field_type)[0] in {
        str,
        int,
        float,
        bool,
        bytes,
    }:
        return field_type
    return None


def _parse_direct_value(value_type: type):
    def parse(value: str) -> Any:
        if value_type is str:
            return value
        if value_type is bytes:
            try:
                return base64.b64decode(value, validate=True)
            except ValueError as exc:
                raise argparse.ArgumentTypeError("must be valid base64") from exc
        return value_type(value)

    return parse


def _deserialize_param_value(value: Any, field_type: Any) -> Any:
    origin = get_origin(field_type)
    args = get_args(field_type)
    if origin is list:
        return [_deserialize_param_value(item, args[0]) for item in value]
    if origin is set:
        return {_deserialize_param_value(item, args[0]) for item in value}
    if origin is tuple:
        return tuple(
            _deserialize_param_value(item, item_type)
            for item, item_type in zip(value, args, strict=True)
        )
    if origin is dict:
        return {
            _deserialize_param_value(key, args[0]): _deserialize_param_value(
                item, args[1]
            )
            for key, item in value.items()
        }
    if field_type is bytes and not isinstance(value, bytes):
        try:
            return base64.b64decode(value, validate=True)
        except ValueError as exc:
            raise CLIUsageError("bytes values must be valid base64") from exc
    if dataclasses.is_dataclass(field_type) or hasattr(field_type, "_fields"):
        nested = _params_field_types(field_type)
        return field_type(
            **{
                name: _deserialize_param_value(item, nested[name])
                for name, item in value.items()
            }
        )
    if isinstance(field_type, type):
        return field_type(value)
    return value


def _allows_none(field_type: Any) -> bool:
    return field_type is type(None) or type(None) in get_args(field_type)


def _params_field_names(params_type: Any) -> tuple[set[str], set[str]]:
    """Return ``(valid, required)`` field names for a params class.

    Supports only NamedTuple and ``@dataclass`` -- the same contract
    as ``build_dag``. Raises CLIUsageError if the class is neither.
    """
    if hasattr(params_type, "_fields"):
        valid = set(params_type._fields)
        defaults = set(params_type._field_defaults.keys())
        field_types = _params_field_types(params_type)
        return valid, {
            name for name in valid - defaults if not _allows_none(field_types[name])
        }
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
        field_types = _params_field_types(params_type)
        return valid, {name for name in required if not _allows_none(field_types[name])}
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
        field_types = _params_field_types(params_type)
        for name, field_type in field_types.items():
            if name not in merged and _allows_none(field_type):
                merged[name] = None
        return params_type(
            **{
                name: _deserialize_param_value(value, field_types[name])
                for name, value in merged.items()
            }
        )
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
