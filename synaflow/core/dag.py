"""
The Directed Acyclic Graph: the compiled model of a pipeline.

Dag     — the top-level container: name, params (input types), steps (nodes),
          and computed metadata (runner requirements, error materializer).
DagNode — one step in the graph.  Its fields layer in four groups (kept
          contiguous below): identity (fn/deps/output), user policy
          (on_error, mode, materializer, thresholds, ...), compiled
          contracts frozen by the builder (output_contract, publish_plan,
          fn_kind, ...), and private diagnostics the executors must not
          consult.

"Immutability" lives at the decision level: every runtime-relevant
decision is compiled at build time into frozen contract dataclasses
(OutputContract, ConsumerContract, PublishPlan); the dataclasses
themselves are plain mutable records filled in once by ``build_dag``.

Design note:
  Runtime decisions must be driven by producer-level materialization only.
  The builder may keep per-consumer materialization details for diagnostics,
  but executors must not inspect them. If a producer is marked as needing
  materialization, all of its consumers read from the materialized output.

Queries on Dag (all stateless over the graph):
  - consumers_of(step_name) → list of step names that depend on it
  - get_execution_levels()   → topological levels for parallel execution
  - to_dict()                → JSON-serializable representation

Both are @dataclass — plain data with behaviour, no hidden state.
"""

import inspect
import typing
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from synaflow.core.type_compatibility import get_type_name
from synaflow.core.types import OnError, StepMode


def get_safe_type_hints(fn: Any) -> dict[str, Any]:
    """Resolve type hints, tolerating unannotated callables.

    When resolution *fails* but the callable clearly declares annotations,
    raising is the fail-loud contract: silently returning ``{}`` would
    skip every validation that depends on the hints.  Only a callable
    with no annotations at all resolves to an empty map.
    """
    try:
        return typing.get_type_hints(fn, include_extras=True)
    except (NameError, TypeError):
        pass
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return {}
    declares_annotations = (
        any(
            parameter.annotation is not inspect.Parameter.empty
            for parameter in sig.parameters.values()
        )
        or sig.return_annotation is not inspect.Parameter.empty
    )
    if declares_annotations:
        name = getattr(fn, "__name__", repr(fn))
        raise ValueError(
            f"Could not resolve the type hints of '{name}'.  A referenced"
            " name is probably undefined in the module namespace; fix or"
            " remove the annotations — unresolvable hints silently disable"
            " DAG validation."
        )
    return {}


def resolve_resource_output_type(resource_name: str, factory: Any) -> Any:
    """Inspect a resource factory and return its declared return type."""
    if not callable(factory):
        raise ValueError(
            f"Resource '{resource_name}' must be declared as a factory callable."
        )

    hints = get_safe_type_hints(factory)
    try:
        sig = inspect.signature(factory)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Resource '{resource_name}' must be inspectable as a factory callable."
        ) from exc

    return_type = hints.get("return", sig.return_annotation)
    if return_type is inspect.Signature.empty:
        factory_name = getattr(factory, "__name__", type(factory).__name__)
        raise ValueError(
            f"Resource factory '{factory_name}' for key '{resource_name}' must declare a return type annotation."
        )
    if return_type in (None, type(None)):
        factory_name = getattr(factory, "__name__", type(factory).__name__)
        raise ValueError(
            f"Resource factory '{factory_name}' for key '{resource_name}' must not return None."
        )
    return return_type


@dataclass(frozen=True)
class OutputContract:
    runtime_kind: Literal["value", "sync_stream", "async_stream"]
    completion_policy: Literal["immediate", "on_exhaustion"]
    drain_policy: Literal["none", "terminal", "barrier_only"]


@dataclass(frozen=True)
class ConsumerContract:
    consumer_name: str
    consumption: Literal["item", "stream", "materialized", "barrier_only"]


@dataclass(frozen=True)
class PublishPlan:
    strategy: Literal[
        "publish_value",
        "publish_stream",
        "publish_materialized",
        "publish_sync_fanout",
        "publish_async_fanout",
    ]
    handoff: Literal["none", "bounded_iterator", "sync_fanout", "async_queue"]
    max_in_flight: int


@dataclass
class DagNode:
    """One compiled step.

    Fields are grouped by concern, in the order they become known:

    * **Identity** — what the step *is* (function, resolved input/output
      types).  Set once by ``validate_and_compile_step``.
    * **User policy** — what the user *asked for* (error handling, mode,
      materializer, thresholds).  Copied from the declared ``Step``.
    * **Build-time compiled contracts** — frozen decisions the executors
      consult instead of re-deriving (materialization plan, publication
      plan, function shape).  All build-time-knowable facts belong here,
      never in runtime introspection.
    * **Private diagnostics** — builder-internal detail kept for JSON
      export and debugging; executors must not branch on it (see
      ``docs/MATERIALIZATION_RUNTIME_CONTRACT.md``).
    * **Scope metadata** — stamped once after the whole dag is built.
    """

    # -- Identity --------------------------------------------------------
    fn: Callable | None = None
    deps: dict[str, Any] = field(default_factory=dict)
    output: Any = None

    # -- User policy ------------------------------------------------------
    on_error: OnError | None = None
    mode: StepMode = StepMode.ALL
    materializer: Callable | None = None
    error_materializer: Callable | None = None
    observers: list = field(default_factory=list)
    force_materialize: bool = False
    max_in_flight: int = 1
    error_threshold_absolute: int | None = None
    error_threshold_pct: float | None = None

    # -- Compiled contracts (builder-owned; executors only read) ----------
    materialize_output: bool = False
    each_mode_deps: list[str] = field(default_factory=list)
    dataset_param_names: dict[str, str] = field(default_factory=dict)
    output_contract: OutputContract | None = None
    consumer_contracts: list[ConsumerContract] = field(default_factory=list)
    publish_plan: PublishPlan | None = None
    # Compiled runtime shape of the step function ("sync", "sync_generator",
    # "async" or "async_generator").  Executors consult this instead of
    # re-inspecting the callable at run time.
    fn_kind: str | None = None
    # Dependencies whose declared type is AsyncIterator/AsyncGenerator, so
    # the async argument builder converts materialized values without
    # re-deriving the decision from annotations at run time.
    async_stream_deps: list[str] = field(default_factory=list)

    # -- Private diagnostics (not runtime policy) -------------------------
    _materialized_deps: list[str] = field(default_factory=list)
    _materialize_reasons: list[str] = field(default_factory=list)

    # -- Scope metadata (stamped once during ``build_dag``; see
    # ``_stamp_scope_metadata``) ------------------------------------------
    pipeline: str | None = None
    parent_pipeline: str | None = None
    pipeline_scope: str = ""
    step_index_in_scope: int = 0
    step_total_in_scope: int = 0

    def to_serializable(self) -> dict:
        mat = self.materializer
        err_mat = self.error_materializer
        ret = {
            "deps": {k: get_type_name(v) for k, v in self.deps.items()},
            "output": get_type_name(self.output),
            "fn": self.fn.__name__ if self.fn else None,
            "on_error": self.on_error.value if self.on_error else None,
            "mode": self.mode.value,
            "materializer": mat.__name__ if callable(mat) else None,
            "error_materializer": err_mat.__name__ if callable(err_mat) else None,
            "each_mode_deps": self.each_mode_deps,
            "pipeline": self.pipeline,
            "parent_pipeline": self.parent_pipeline,
            "max_in_flight": self.max_in_flight,
        }
        if self.materialize_output:
            ret["needs_materialize_reasons"] = list(self._materialize_reasons)
        if self.observers:
            ret["observers"] = _serialize_observers(self.observers)
        if self.dataset_param_names:
            ret["dataset_param_names"] = dict(self.dataset_param_names)
        if self.error_threshold_absolute is not None:
            ret["error_threshold_absolute"] = self.error_threshold_absolute
        if self.error_threshold_pct is not None:
            ret["error_threshold_pct"] = self.error_threshold_pct
        if self.output_contract is not None:
            ret["output_contract"] = asdict(self.output_contract)
        if self.consumer_contracts:
            ret["consumer_contracts"] = [asdict(c) for c in self.consumer_contracts]
        if self.publish_plan is not None:
            ret["publish_plan"] = asdict(self.publish_plan)
        if self.fn_kind is not None:
            ret["fn_kind"] = self.fn_kind
        if self.async_stream_deps:
            ret["async_stream_deps"] = list(self.async_stream_deps)
        ret["pipeline_scope"] = self.pipeline_scope
        ret["step_index_in_scope"] = self.step_index_in_scope
        ret["step_total_in_scope"] = self.step_total_in_scope
        return ret


def _serialize_observers(observers: list) -> list[dict]:
    result = []
    for obs in observers:
        info = {"handler_name": getattr(obs.handler, "__name__", str(obs.handler))}
        info["source"] = getattr(obs, "source", "step")
        result.append(info)
    return result


def _serialize_pipeline_observers(observers: list) -> list[dict]:
    result = []
    for obs in observers:
        info = {"handler_name": getattr(obs.handler, "__name__", str(obs.handler))}
        info["source"] = getattr(obs, "source", "pipeline")
        result.append(info)
    return result


@dataclass
class Dag:
    name: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    resource_factories: dict[str, Any] = field(default_factory=dict)
    steps: dict[str, DagNode] = field(default_factory=dict)
    requires_sync_runner: bool = False
    requires_async_runner: bool = False
    error_materializer_factory: Any = None
    pipeline_observers: list = field(default_factory=list)
    # Aggregated totals per scope_id, populated by
    # ``_stamp_scope_metadata`` after the dag is fully built.
    scope_step_totals: dict[str, int] = field(default_factory=dict)

    def __getitem__(self, key):
        return self.steps[key]

    def __setitem__(self, key, value):
        self.steps[key] = value

    def __contains__(self, key):
        return key in self.steps

    def __len__(self):
        return len(self.steps)

    def __iter__(self):
        return iter(self.steps)

    def items(self):
        return self.steps.items()

    def values(self):
        return self.steps.values()

    def _resource_type(self, name: str) -> Any:
        """Resolve the output type of a resource factory by name."""
        return resolve_resource_output_type(name, self.resource_factories[name])

    def get(self, key, default=None):
        if key in self.steps:
            return self.steps[key]
        if key in self.resource_factories:
            return DagNode(output=self._resource_type(key))
        if key in self.params:
            return DagNode(output=self.params[key])
        return default

    def to_dict(self) -> dict:
        result = {
            "name": self.name,
            "params": {k: get_type_name(v) for k, v in self.params.items()},
            "steps": {
                name: node.to_serializable() for name, node in self.steps.items()
            },
        }
        if self.resource_factories:
            result["resources"] = {
                k: get_type_name(self._resource_type(k))
                for k in self.resource_factories
            }
        if self.error_materializer_factory is not None:
            result["error_materializer"] = self.error_materializer_factory.__name__
        if self.pipeline_observers:
            result["pipeline_observers"] = _serialize_pipeline_observers(
                self.pipeline_observers
            )
        result["scope_step_totals"] = dict(self.scope_step_totals)
        return result

    def consumers_of(self, step_name: str) -> list[str]:
        return [name for name, node in self.steps.items() if step_name in node.deps]

    def output_key(self, producer: str, consumer: str) -> str:
        if len(self.consumers_of(producer)) > 1:
            return f"{producer}__{consumer}"
        return producer

    def is_hidden_step(self, step_name: str) -> bool:
        return step_name.startswith("_")

    def is_terminal_step(self, step_name: str) -> bool:
        return self.is_hidden_step(step_name) or not self.consumers_of(step_name)

    def should_drain_deferred_step(self, step_name: str) -> bool:
        node = self.steps.get(step_name)
        if node is None or node.output_contract is None:
            return self.is_terminal_step(step_name)
        return node.output_contract.drain_policy != "none"

    def each_inputs(self, step_name: str) -> list[str]:
        """Which deps should be unrolled item-by-item (each mode)."""
        node = self.steps.get(step_name)
        if not node:
            return []
        return list(node.each_mode_deps)

    def needs_materialize(self, step_name: str) -> bool:
        node = self.steps.get(step_name)
        if node is None:
            return False
        return node.materialize_output

    def get_execution_levels(self) -> list[list[str]]:
        in_degree: dict[str, int] = {name: 0 for name in self.steps}
        for name, node in self.steps.items():
            for dep in node.deps:
                if dep in in_degree:
                    in_degree[name] += 1

        levels: list[list[str]] = []
        processed: set[str] = set()

        while len(processed) < len(in_degree):
            level = [
                name
                for name, degree in in_degree.items()
                if degree == 0 and name not in processed
            ]
            if not level:
                # Remaining nodes form a cycle.  Fail loud instead of
                # silently returning a partial topological order.
                remaining = sorted(set(in_degree) - processed)
                raise ValueError(
                    f"Pipeline '{self.name}': dependency cycle detected"
                    f" among steps: {', '.join(remaining)}"
                )
            levels.append(level)
            processed.update(level)

            for name in level:
                for other_name, node in self.steps.items():
                    if name in node.deps:
                        in_degree[other_name] -= 1

        return levels
