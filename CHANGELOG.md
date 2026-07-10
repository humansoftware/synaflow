# CHANGELOG



## v0.28.0 (unreleased)

### Feature

* feat: PipelineRegistry, synaflow CLI, and `synaflow` console script (#108)

New public surface for working with multiple pipelines at once,
without writing a driver script per run.

* **synaflow.PipelineRegistry** — a name-keyed mapping of
  `(name → PipelineDef)` that **caches the compiled `Dag`** so
  repeated lookups don't pay `build_dag`'s cost twice. The
  registry supports the `MutableMapping` protocol
  (`__getitem__/__setitem__/__delitem__/__iter__/__len__/...`).
  `__setitem__` validates that the value is a `PipelineDef` and
  that its `.name` matches the registry key (raising `TypeError`
  before `.name` access, `ValueError` after). `__delitem__` and
  `clear()` invalidate the cached `Dag`. `PipelineRegistry.from_module(name)`
  is the catalog-discovery entry point: it imports `name` and
  returns its `catalog` attribute, raising the standard Python
  exceptions (`ModuleNotFoundError`, `AttributeError`, `TypeError`).
* **synaflow.cli** — argparse-based CLI with 5 subcommands:
  `list`, `info`, `dag`, `validate`, `run`. Catalog discovery
  via `--catalog MODULE` (no plugin system, no entry-point
  sprawl). Sync vs async dispatch is automatic
  (`dag.requires_async_runner`). Param resolution supports
  both `--params-file PATH` (JSON object) and repeatable
  `--param key=value` flags (with `--param` overriding file
  values); values are parsed as JSON when possible, otherwise
  kept as strings. Internal exceptions are NOT caught at the
  boundary — only `CLIUsageError` (the CLI's user-input error
  vocabulary) is, so genuine programmer errors still surface
  as tracebacks.
* **synaflow.__main__** — enables `python -m synaflow` as an
  equivalent of the `synaflow` console script.
* **`synaflow` console script** — pyproject.toml gains
  `[project.scripts] synaflow = "synaflow.cli:main"`, so a
  `pip install synaflow` (or `uv sync`) drops a `synaflow`
  entry point on `PATH`. End users get
  `synaflow --catalog mypackage.pipelines list` /
  `info` / `dag` / `validate` / `run` without typing
  `python -m synaflow` by hand.
* **run / async_run accept a `Dag`** — both engine entry points
  consume a prebuilt `Dag`. Dag construction is a **design-time**
  concern (e.g. `PipelineRegistry.get_dag(name)` or a one-off
  `build_dag(p)` at module load); `run` / `async_run` themselves
  never compile. The two engines are also the only supported
  runtime entry points — they reject Dag whose declared engine
  (`requires_sync_runner` / `requires_async_runner`) does not
  match.

Layering preserved: `PipelineRegistry.from_module` is core / agnostic
and raises standard Python exceptions; `synaflow.cli._load_catalog`
is the thin CLI-side adapter that translates those exceptions
into the CLI's friendly error vocabulary. `synaflow.cli.main`
only catches `CLIUsageError` at the boundary.

### BREAKING CHANGE

* refactor: `run()` / `async_run()` now accept a `Dag` only (#108)

The runtime API no longer accepts a `PipelineDef`. `run()` and
`async_run()` consume a **prebuilt `Dag`** exclusively — they never
call `build_dag` themselves. This is a deliberate separation of
concerns:

* **Design time.** Dag construction is the responsibility of
  `synaflow.PipelineRegistry.get_dag(name)` (cached) or a one-off
  `build_dag(pipeline_def)` call at module load. This is where
  the heavy lifting (sub-pipeline expansion, scope stamping,
  sync/async flagging) happens.
* **Runtime.** `run(dag, params)` and `async_run(dag, params)`
  execute the prebuilt Dag without touching compilation. The
  executor no longer imports `build_dag` or `PipelineDef` at all.

**Migration.** Any code calling `run(p, params)` /
`async_run(p, params)` with a `PipelineDef` must move the
`build_dag(p)` call up to design time. Idiomatic pattern:

```python
catalog = PipelineRegistry()
catalog["hello"] = pipeline(...)
# later, in any runtime:
run(catalog.get_dag("hello"), Params())
```

A one-off script can compile once at startup:

```python
dag = build_dag(p)
run(dag, Params())
```

The two engines are still strict about the declared engine
(`dag.requires_sync_runner` / `dag.requires_async_runner`); a
mismatch raises `RuntimeError` as before.

* refactor: remove internal `PipelineRegistry` base from `synaflow.execution` (#108)

`synaflow.execution` no longer re-exports the internal base
class that was historically named `PipelineRegistry`. That
name is now taken by the new public catalog class
`synaflow.PipelineRegistry`, and the internal override base
has been renamed to **`_OverrideRegistry`** (leading
underscore → private-by-convention). It still lives at
`synaflow/execution/overrides.py` but is **not** re-exported
from `synaflow.execution` and there is no public name for
it.

**What this means in practice.** Code that used the public
override registries — `MaterializerRegistry`,
`ObserverRegistry`, `ResourceRegistry` — needs no changes;
they are still re-exported from `synaflow.execution`. Code
that imported the old `PipelineRegistry` symbol from
`synaflow.execution` (the base class) was reaching into an
internal implementation detail; that symbol is gone. There
is **no drop-in replacement**, because the new
`synaflow.PipelineRegistry` is a different API — a catalog of
`PipelineDef`s with cached `Dag` builds, not a subclassable
override base. To plug into the override system, work through
`MaterializerRegistry`, `ObserverRegistry`, or
`ResourceRegistry`, which remain the supported surface.

### Refactor

* refactor: enforce top-level imports follow-up; CLI helper module-private (#108)

  * PLC0415 enforced everywhere: imports are now at module
    top-level in `synaflow/cli.py`, `tests/cli/test_cli.py`,
    and the tests that share the `cli.X` module-access
    convention for private helpers.


## v0.27.0 (2026-07-09)

### Feature

* feat: scope-aware step lifecycle observers (#105); build_dag cleanup (#107) (#106)

* feat(observers): expose pipeline_scope and step totals on step events (#105)

Stamp `pipeline_scope`, `step_index_in_scope`, and
`step_total_in_scope` onto each `DagNode` at DAG build time so step
lifecycle events (started / completed / failed) can accurately report
per-sub-pipeline progress. Downstream consumers like the Postgres
`db_observer` can now flip a sub-pipeline from RUNNING to COMPLETED
only after the LAST step of that sub-pipeline has fired.

Implementation:

* Counts come from `PipelineDef.count_scope_steps()` at design time,
  walking the include graph via `IncludeStep.pipeline` — NOT from a
  second pass over the post-expansion flat step list. The walker
  accumulates per-include-instantiation so multi-instance includes of
  the same sub-pipeline concatenate into a single inner-scope total.
  Cycle detection via a `seen` frozenset is a safety net; the
  authoritative `validate_no_unused_resources` runs first.
* `Dag` exposes `scope_for(step_name)`, `scope_total(scope)`, and
  `scope_counts()` accessors backed by `_scope_counts` (precomputed
  from the definition tree, stamped at build time).
* A loud `_assert_dag_invariants` in `dag_builder.build_dag` raises
  `RuntimeError` if any compiled DagNode has an empty `pipeline`,
  catching regressions before observers see them. Helper
  `Dag.step_scope_index` raises `RuntimeError` (not silent fallback)
  on the same condition.

Refactor — eliminate the `StepConfig` wrapper indirection:

* `StepConfig` / `AsyncStepConfig` → `StepRuntimeConfig` /
  `AsyncStepRuntimeConfig` carrying only the `DagNode` (the
  duplicated `observers`/`mode`/`on_error`/`max_in_flight`/`dataset_param_names`/`error_threshold_*`
  fields were dead copies — threshold.py reads `node.error_threshold_*`
  directly and observers are resolved at the dispatcher).
* `_dag_node` back-reference → public `dag_node: DagNode` attribute.
* `EventDispatcher.step_started(dag_node, step_name)` /
  `step_completed` / `step_failed` / `materialization_*` take the
  `DagNode` directly — no more `_resolve_dag_node` band-aid that
  unwrapped `StepConfig → DagNode` at the event boundary. Same on the
  async dispatcher.
* `StepLifecycle` and `AsyncStepLifecycle` take `dag_node: DagNode`
  directly instead of forwarding through a step-config wrapper.
* `StepRunner` / `AsyncStepRunner` take `step_runtime_config` (no
  separate `dag_node` parameter — no double-passing). Lazy fallback
  for tests removed; `StepRuntimeConfig` is required (a real DagNode
  is trivial to construct).
* Type annotations everywhere: `node: Any` → `node: DagNode` across
  executor / step_lifecycle / event_dispatch signatures.

Backward compatibility: the new observer-context fields have
zero/empty-string defaults so existing observers continue to work.

* refactor(dag): build_dag accepts PipelineDef (issue #107 step 1)

Replace 9-arg build_dag(pipeline_name, params, steps, ...) with single-arg
build_dag(pipe_def). Building the dag IS the validation; signature now
matches the single object that carries every input. is_default_factory
boolean hack eliminated (derived from pipe_def.materializer is None
internally).

No behavior change; PipelineDef.__post_init__ still calls build_dag on
construction and caches the result on self.dag. The dag field itself
goes away in the next step.

* refactor(dag): drop local var extraction in build_dag (issue #107 step 2a)

PipelineDef fields are now read inline as pipeline_def.X throughout
build_dag. Removed the 7-line block of destructuring assignments that
shadowed attributes with no transformation. The one local that
remains (error_materializer) carries the log_error_materializer_factory
fallback, so its name documents the resolved value.

Also trimmed the verbose module-level comment in build_dag and the
4-line inline comment about resource_factories — the code is now
self-documenting.

* refactor(execution): drop StepRuntimeConfig wrapper, pass dag_node directly (issue #107 step 2b)

The single-field StepRuntimeConfig class added no value over passing
the DagNode itself. StepRunner.__init__ now takes dag_node: DagNode
as a required param (no default, no lazy None-check); the
TypeError on missing arg is raised by Python itself.

Executor builds StepRunner(dag_node=node) without the intermediate
wrapper construction. The 4-line comment about the wrapper
disappears with the class.

* refactor(execution): drop AsyncStepRuntimeConfig wrapper (issue #107 step 2c)

Mirror of step 2b (sync): the single-field AsyncStepRuntimeConfig added
no value. AsyncStepRunner now takes dag_node: DagNode as a required
param directly. Async executor passes dag_node=node without the
intermediate wrapper construction.

* refactor(core): drop StepScopeIndex NamedTuple + Dag.step_scope_index helper (issue #107 step 2d)

The 3-field NamedTuple wrapper around DagNode.pipeline/step_index_in_scope/
step_total_in_scope added no value. Consumers read those public DagNode
fields directly. Dropped:
  - StepScopeIndex class in synaflow/core/dag.py
  - Dag.step_scope_index method
  - 8 tests (4 in test_dag_scope.py, 2 in sync test_observer_runtime.py,
    2 in async test_observer_runtime.py) that exercised only the
    helper. The underlying DagNode fields remain tested in
    test_dag_scope.py scope tests.
  - The namedtuple import from synaflow/core/dag.py
  - One stale reference to the helper in dag_builder._assert_dag_invariants
    docstring.

* refactor(core): trim verbose docstrings in dag_builder/definition/observers (issue #107 step 2e)

Lexical cleanup only — no behavior change.

Trimmed 6 verbose docstrings (&gt;= 4 lines) to 1-3 lines that preserve
the load-bearing design intent:
  - dag_builder._plan_materialization: 12 -&gt; 2 lines (pointer to
    docs/MATERIALIZATION_RUNTIME_CONTRACT.md for the full model)
  - dag_builder._assert_dag_invariants: 13 -&gt; 3 lines (keeps the
    &#39;absence is internal framework bug&#39; warning that justifies the
    loud failure)
  - definition.fill_scope_metadata: 29 -&gt; 4 lines (drops the
    recursion/cycle-prose, kept brief — 3a will shorten further when
    the method becomes non-recursive)
  - definition.get_execution_levels: 5 -&gt; 1 line
  - observers.Observer / ResolvedObserver / dispatch_observers* : 6-10
    -&gt; 2-3 lines each (kept the &#39;filtering/scoping belongs in wrapper
    helpers&#39; design note for Observer)

Left intact:
  - dag_steps.validate_no_unmaterialized_terminal_streams (16 lines)
    — documents a non-obvious deadlock/data-loss rule; the only doc
    of its kind in the codebase.

* refactor(definition): fill_scope_metadata is non-recursive (issue #107 step 3a)

Each PipelineDef stamps only its own direct steps. Sub-pipelines
are separate instances with their own __post_init__ that runs at
construction time, so they stamp themselves.

The recursion + &#39;seen&#39; frozenset cycle protection was unnecessary:
the tree of PipelineDef instances is well-formed by construction
(sub-pipeline must exist before being referenced in an IncludeStep),
and the authoritative cycle detector stays in expand_macros where it
raises &#39;Infinite cycle detected&#39;.

Body: 13 lines -&gt; 5 lines. The &#39;seen&#39; parameter is gone.

Test rename: &#39;stamps_direct_steps_and_recurses&#39; -&gt; &#39;stamps_direct_steps&#39;.
The &#39;pipe_b&#39; module-level fixture is built at import time, so its
own __post_init__ has already stamped its steps before any test
constructs a parent pipeline — the observable behavior is unchanged.

* refactor(definition): dag is lazy cached_property; build moved to build_dag (issue #107 step 3b)

PipelineDef.dag
  - Was: eager dataclass field, built in __post_init__ via build_dag(self)
  - Now: @cached_property, built lazily on first access via build_dag(self)
    and cached. Effect: pipeline() never raises on design-time errors;
    those surface on p.dag (or build_dag(p)) access, or at first run.

PipelineDef.__post_init__
  - Was: self.fill_scope_metadata() + build_dag(self) + requires_* cache
    + handler validation
  - Now: just self.fill_scope_metadata(). The lazy build handles the rest.

PipelineDef.requires_sync_runner / requires_async_runner
  - Was: dataclass fields assigned from self.dag.requires_*
  - Now: @property delegates to self.dag.requires_* — same API, lazy.

PipelineDef.to_dict / get_execution_levels
  - Kept as public methods delegating to self.dag.

Handler validation ordering
  - Was: called from __post_init__ AFTER build_dag had resolved
    materializers. Now called from inside build_dag AFTER
    _resolve_materializers (preserved ordering — a sync factory
    returning an async callable would otherwise pass).

build_dag ordering
  - validate_sync_async_consistency -&gt; _compile_execution_plan -&gt;
    _resolve_materializers -&gt; handler validation. Handler validation
    must run AFTER materializer resolution.

Test contract migration (105+2 tests)
  - 105 tests were &#39;when_constructed_then_raises&#39; / &#39;when_compiled
    _then_raises&#39;. Rewritten via AST script: p = pipeline(...);
    with pytest.raises(...): p.dag. Test function names updated
    &#39;when_constructed_then_raises&#39; -&gt; &#39;when_built_then_raises&#39;
    where the name was explicit.

  - 2 tests mutated Step.max_in_flight AFTER pipeline() but BEFORE
    first .dag access. With the old eager build the mutation was
    ignored; with the lazy build it would be picked up. Updated
    tests to trigger p.dag build BEFORE the mutation, preserving
    the test intent (DagNode is the runtime source of truth).

Final: 704/704 passing. Dag is on-demand, validation explicit via
build_dag. The pipeline_def.dag API still works (cached_property
+ requires_* properties) — full removal happens when the
PipelineRegistry (issue #107) lands.

* refactor(dag_builder): move handler validators out of definition.py

The two handler-kind validators (_validate_no_async_handlers and
_validate_no_sync_handlers) belonged to dag_builder.py conceptually
— they operate on the compiled Dag (pipeline_observers, step nodes
with their resolved materializers/error_materializers/fn), raising
on incompatible handler kinds. They were scheduled in build_dag
right after _resolve_materializers (depends on resolved handlers,
not on the raw PipelineDef).

But the definitions lived in core/definition.py because of an
accident in 3b&#39;s pivot: I moved the call sites from __post_init__
into build_dag, but never moved the defs. The result was a
circular-ish import (dag_builder importing from definition for
validators; definition carrying validation logic that needed
adapters.is_async_callable — which forced the import block).

This commit moves the defs into dag_builder.py next to the other
validators (validate_no_unused_resources, validate_no_unmaterialized
_terminal_streams, validate_sync_async_consistency, _assert_dag
_invariants, _compile_execution_plan). The is_async_callable import
in definition.py is dropped — that module is now back to defining
data containers only (PipelineDef, Step, IncludeStep, observer
classes). Stop 0 of issue #107 cleanup.

No behavior change. 704/704 tests passing.

* refactor(definition): drop .dag attribute; clients call build_dag(pipeline) (issue #107 step 3b)

PipelineDef loses its .dag cached_property + requires_sync_runner /
requires_async_runner properties entirely. Per the design — building
IS the validation, and the dag must not be a hidden lazy property on
the def — callers explicitly invoke build_dag(p).

Production callers migrated:
  - run() / async_run() both build_dag(p) internally; dag is held in
    a local variable that exposes requires_sync_runner /
    requires_async_runner / resource_factories. Replaces the previous
    getattr() defensive dance against the missing properties.
  - overrides._materializer_*, _observer_*, _resource_* helpers all
    build_dag once into a local and reuse.

PipelineDef.to_dict / get_execution_levels:
  - Kept as public methods. They delegate to build_dag(self).to_dict()
    / get_execution_levels(). Lazy import of build_dag inside the
    method body avoids the import cycle (definition.py &lt;-&gt; dag_builder).

105+2 tests migrated to build_dag(p) (instead of .dag access). Done
via two AST scripts:
  - migrate_dag_attr.py: rewrites &#39;&lt;pipe&gt;.dag&#39; (one or more levels
    of attribute chain) into &#39;build_dag(&lt;pipe&gt;)&#39;. Adds the import at
    the top of the file when needed. Handles chained access like
    &#39;pack.pipeline.dag&#39; and known pipe names: p, my_pipeline, my_pipe,
    pipeline_def, pack_pipeline, pack, parent, child, sub, main, top,
    inner, outer, root_pipe, sub_pipe, pipe_a, pipe_b, pipe, pipeline.
  - fix_imports_after_docstring.py: post-pass to move imports the
    script inserted BEFORE a module-level raw-string expression
    (which ruff calls E402 &#39;module-level import not at top of file&#39;)
    back into the proper position.

Two test_pep563_annotations.py  comments that the AST
unparse lost were re-added manually. One f-string nested-quote
artifact (Python 3.12 syntax leaked into a 3.10-compatible test file)
was fixed by switching outer quotes from &#39; to &#34;.

test_runner_contract_uses_dag_node_max_in_flight:
  - The &#39;mutate Step after build&#39; test was rewritten: build_dag into
    a local &#39;dag&#39;, mutate &#39;dag.steps[...].max_in_flight&#39; (the dag node,
    not the Step), then run(p). This preserves the test&#39;s intent:
    runtime reads max_in_flight from the DagNode, Step mutations
    after build are ignored.

Final state: 704/704 tests passing, ruff all-checks green, pre-commit
green. Follows Fase 0 (c677819) which moved the handler validators
out of definition.py so this change doesn&#39;t reintroduce the cycle.

Future: PipelineRegistry (issue #107 step 3c) — will absorb the
build_dag() call inside executor APIs and expose it as
registry.get_entry(name).dag.

* refactor(definition): drop scope fields; deferred stamping (issue #107 step A)

Removes the 1-based declaration-order stamping from PipelineDef so scope
metadata is computed cleanly in build_dag (path-based identity, 0-based
topological index).

Removals:
- Step/IncludeStep.index_in_scope, total_in_scope
- PipelineDef.fill_scope_metadata + __post_init__
- DagNode.step_index_in_scope, step_total_in_scope + to_serializable
- dag_steps.validate_and_compile_step: drops the two kwargs
- dag_builder._compile_steps: drops the two kwargs
- dag_expansion: drops the two kwargs in adapter/sub_step builders
- event_dispatch (sync/async): drops the two kwargs; defaults to 0

Deleted tests that asserted the deprecated semantics:
- tests/core/test_dag_scope.py (full file)
- 3 tests in tests/core/test_dag_builder.py (fill_scope_metadata_*, to_serializable scope)
- 5 tests in each test_observer_runtime.py (sync + async)
- tests/core/test_dag_execution_order.py: drops step_*_in_scope from expected-keys set

Suites: 704 -&gt; 684 (-20). The removed tests are re-added in Stop B/C/E
with the new path-based identity and 0-based topological indexing.

Part of #107 (registry + CLI refactor), addresses #105 design.

* refactor(expansion): thread scope_id through expand_macros (issue #105 v2)

Builds on Stop A: scope metadata now flows as transient tuple data
through expansion, never written onto Step/IncludeStep instances.

expand_macros(...) takes a new scope_path kwarg and returns
list[tuple[str, Step]] instead of list[Step].

Scope identity semantics:
- Root direct step: scope_id = current_pipeline_name
- Adapter step: scope_id = parent&#39;s scope (the include happens *in* the
  caller; the adapter reports the caller&#39;s scope)
- Sub-pipeline inner step: scope_id = &#39;{parent_scope}__{include_name}&#39;
  (cumulative, identifies the include *instance*, not the
  underlying PipelineDef)

Repeated includes of the same PipelineDef yield distinct scope_ids;
nested includes yield cumulative paths with __ separators.

Downstream updates (shape change cascades):
- _expand_include / _expand_sub_pipeline_steps forward scope_path
- _expand_and_validate_steps, _compile_steps accept tuples
- _validate_resource_names, validate_no_duplicate_base_datasets
  iterate over tuples (use step.name without scope_id)

DagNode.pipeline is preserved as-is (Stop C will decide migration).

6 tests added in tests/core/test_expand_macros_scope_paths.py covering
flat, single-include, nested, repeated, and no-name cases.

Suites: 684 -&gt; 690 (+6).

Part of #107 (registry + CLI refactor), addresses #105 scope identity.

* refactor(dag_builder): stamp scope metadata on DagNode (issue #105 v2)

Adds the three scope fields back onto DagNode and a dag-level
scope_step_totals dict, populated by a single _stamp_scope_metadata
pass after the full dag is built and validated. Serialization
includes both the per-node fields and the dag-level dict (part of
the compiled external contract).

New DagNode fields (all default to safe placeholders, stamped on
during build_dag):
- pipeline_scope: str (path-based scope_id, __-separated)
- step_index_in_scope: int (0-based topological within scope)
- step_total_in_scope: int (count of steps in that scope)

New Dag field:
- scope_step_totals: dict[str, int] keyed by scope_id

Flow:
- _compile_steps(...) returns (dag_nodes, produced, scope_id_by_step_name)
- build_dag calls _stamp_scope_metadata(dag, scope_id_by_step_name) at the
  end of the pipeline (after all validation/materialization).
- _stamp_scope_metadata flattens dag.get_execution_levels() to get
  topological order, groups names by scope, then assigns 0-based
  indices per scope (within topo order, no second topo algorithm).

DagNode.pipeline is preserved as-is (immediate pipeline name) — the
new fields carry scope identity, single source of truth for the
scope emitted in observer contexts (Stop D).

DagNode.to_serializable() and Dag.to_dict() emit the new fields.

12 new tests in tests/core/test_dag_scope_stamping.py:
- root, adapter (caller&#39;s scope), nested include (cumulative path),
  repeated includes (distinct scope_ids)
- 0-based indexing
- topological order within scope (diamond)
- scope_step_totals dict at dag level
- to_dict serialization includes both node fields and dict
- empty dag preserves shape

Tests for the legacy corpus (tests/core/test_dag_execution_order.py)
extend _normalize_exported_dag_for_contract_assertions to ignore the
new scope keys (covered by the dedicated scope tests).

Suites: 690 -&gt; 702 (+12).

Part of #107 (registry + CLI refactor), addresses #105 scope identity.

* test(dag): rename empty-dag scope_step_totals test for stable contract

The empty dag emits scope_step_totals as an empty dict by design
(preferable to key absence for the JSON contract). Rename and assert
exact equality.

* feat(observers): expose scope_step_totals in PipelineStartedContext

Adds a scope_step_totals: dict[str, int] field to
PipelineStartedContext. Consumers can now detect scope completion
without waiting for the final step event: track done_count[scope_id]
and compare against scope_step_totals[scope_id].

EventDispatcher (sync + async) reads self._dag.scope_step_totals
when building the context — no new constructor argument, no extra
state on the dispatcher.

PipelineStartedContext gets from dataclasses import field and a
default of empty dict (kept stable for the JSON contract; the live
dag populates the dict at run time).

Replaces the prior negative test
test_given_pipeline_started_context_does_not_carry_step_scope_fields
with two new tests:
- test_given_pipeline_started_context_exposes_scope_step_totals
- test_given_pipeline_started_context_default_scope_step_totals_is_empty_dict

Sync + async parity.

Suites: 702 -&gt; 704 (+2, -1 obsolete; net +1).

Closes #105 in conjunction with Stops A/B/C: observers can report
per-sub-pipeline progress accurately.

* fix(dispatch): forward DagNode scope fields into StepXContext (issue #105)

Stop D exposed scope_step_totals on PipelineStartedContext but
the three step events (StepStartedContext, StepCompletedContext,
StepFailedContext) were still emitting pipeline_scope=dag_node.pipeline
— the immediate PipelineDef name, not the path-based scope_id stamped
in Stop C. This meant the repeated-includes bug remained for any
observer reading step events: both inner steps from the same sub
shared pipeline_scope == &#39;Sub&#39; and indices defaulted to 0.

Sync and async dispatchers now forward:
- pipeline_scope = dag_node.pipeline_scope  (path-based)
- step_index_in_scope = dag_node.step_index_in_scope  (0-based topological)
- step_total_in_scope = dag_node.step_total_in_scope  (per-scope count)

DagNode.pipeline&#39;s semantic (immediate pipeline name) is preserved
for any consumer that still reads it; the new fields are the
single source of truth for scope identity in observer contexts.

Two new regression tests (sync + async):
test_given_repeated_includes_when_step_completed_then_observer_sees_distinct_pipeline_scope
proves that observer sees R__first and R__second for two
includes of the same sub.

Also fixes test_given_pipeline_started_context_default_scope_step_totals_is_empty_dict
(which was running a real pipeline that always has a non-empty dict).
Now it constructs PipelineStartedContext directly to assert the
default is an empty dict — backward-compatible for code that builds
contexts directly without the kwarg.

Suites: 704 -&gt; 706 (+2 regression tests). All other tests pass
unchanged; the observer-contexts scope semantics now matches the
DagNode metadata.

Part of #107 (registry + CLI refactor), addresses #105.

* test(observer): aggregator proves per-instance completion (issue #105)

Two minimal observer/aggregator tests in each engine (sync + async):

test_given_repeated_includes_then_aggregator_completes_each_scope_independently
- Two includes of the same sub (&#39;Sub&#39;) with 3 internal steps each.
- Aggregator tracks done[pipeline_scope] vs
  PipelineStartedContext.scope_step_totals.
- Verifies the path-based scope_id keeps the two instances apart:
  totals has R__first and R__second separately, both reach their
  total before the run completes.
- Includes the invariant: done[scope] &lt;= totals[scope] for every
  scope (never over-counts).

test_given_nested_includes_then_inner_scope_completes_before_outer_scope
- One outer include with 1 inner include; both sub-pipelines run.
- Inner scope is R__outer__inner; outer adapter in R__outer; outer
  in R. Verifies all three scopes report independent completion.

Sync + async parity (same logic, async-compatible aggregator).

Suites: 706 -&gt; 710 (+4, 2 per engine).

Closes issue #105: scope-aware lifecycle observers can now report
per-sub-pipeline progress accurately. The path-based identity and
per-scope totals, combined with the public observer contract,
let consumers mark a scope complete only after its instance&#39;s
final step has fired.

---------

Co-authored-by: Marcelo Elias Del Valle &lt;marcelo@mvalle.br&gt; ([`267a130`](https://github.com/humansoftware/synaflow/commit/267a1301fbc52ab5ab0c4b8268e751f8b5f1885d))


## v0.26.1 (2026-07-07)

### Fix

* fix: prevent PipelineExecutor hang on step failure (issue #103) (#104)

* fix: prevent PipelineExecutor hang on step failure (issue #103)

Three independent hang mechanisms were causing sync_engine to block
indefinitely when a step failed with bounded handoff active:

1. SyncFanout._put_terminal() blocked forever pushing EOF_MARKER to a
   branch whose consumer never iterated because build_arguments() had
   raised before the SyncQueueIterator was closed.  The leaked iterator
   kept its branch in _active_branches, so the pump&#39;s terminal push
   spun on a full queue with no consumer.

2. _run_graph() blocked in cond.wait() when one step failed while a
   sibling was still blocked on user I/O.  fatal_error was set but the
   wait loop never broke out.

3. cleanup() blocked on fanout.join() when the pump thread was stuck in
   next() on a user-code iterator.  Python cannot kill arbitrary user
   threads, so cleanup() must bound the wait.

Changes
-------

* argument_builder.build_arguments() (sync + async): track iterators /
  AsyncQueueBranch slots obtained from runtime state and close them in
  the except block before re-raising.  This is the root-cause fix for
  the production scenario in the issue.

* executor._run_graph() (sync): abandon ThreadPoolExecutor&#39;s
  context-manager form so we can call shutdown(wait=False,
  cancel_futures=True) on fatal_error.  Loop while running_tasks AND
  fatal_error is None so a step failure exits the wait immediately.
  In-flight workers blocked on user code are intentionally leaked
  (Python limitation) - their threads are reclaimed by the event loop
  / interpreter shutdown.

* executor._run_graph() (async): mirror - event.set() fires on either
  natural completion or fatal_error; pre-wait guard prevents blocking
  when a failure occurs before the wait begins.

* executor.cleanup() (sync): bound fanout.join(timeout=1.0); the
  SyncFanout.join() method now accepts a timeout and returns a bool.
  Log a warning when a pump cannot exit (Python cannot kill stuck
  user code).

* executor.cleanup() (async): mirror - asyncio.wait_for(gather(...),
  timeout=1.0) instead of unbounded wait.  Orphan pumps are reclaimed
  by the event loop.

Tests
-----

tests/execution/{sync,async}_engine/test_runner_max_in_flight_hang.py
contain six mirrored tests each:

  A - cleanup() hang via stuck SyncFanout _pump thread
  B - _run_graph() hang via in-flight future that never completes
  C - production scenario: build_arguments() leaks a SyncQueueIterator
      (root-cause reproducer for the issue)
  D - same scenario without bounded handoff (baseline: no hang)
  E - OnError.CONTINUE, consumer raises mid-iteration (no hang)
  F - OnError.STOP, consumer raises mid-iteration with fanout
      (PipelineStopException propagates)

All 673 tests pass (12 new); patch coverage 81.0%; pre-commit clean.
uv.lock bumped to 0.26.0 to match pyproject.toml.

* ci: switch pytest-timeout from thread to signal method (issue #103)

The &#39;thread&#39; method spawns a daemon monitor that can keep pytest
teardown alive on GitHub Actions after all tests finish printing
the summary table - this caused a 18-minute hang that we had to
cancel manually.

&#39;signal&#39; on Linux is cleaner: SIGALRM-based interruption that does
not leak daemon threads during pytest teardown.

* fix: make ThreadPoolExecutor workers daemon to unblock CI teardown (issue #103)

Even after the production hang was fixed, CI runs continued to hang
for hours after &#34;673 passed in N.NNs&#34; was printed.  Two layers
were responsible:

1. (Fixed in prior commit) The unbounded cond.wait() and
   fanout.join() inside the executor - root cause of the
   production hang.

2. (Fixed in this commit) The clean-up abandons ThreadPoolExecutor
   workers via shutdown(wait=False, cancel_futures=True), but stdlib
   ThreadPoolExecutor&#39;s workers are non-daemon by default.  When
   shutdown(wait=False) returns, the stuck workers stay alive, and
   Python&#39;s threading._shutdown helper installs an atexit that joins
   them all on interpreter shutdown.  pytest session teardown walks
   the same path, so workers blocked on user I/O made pytest hang
   indefinitely - even though every test had already passed.

The fix is a small subclass, _DaemonThreadPoolExecutor, that
overrides _adjust_thread_count to spawn threading.Thread with
daemon=True.  The trade-off (a daemon worker may be abruptly
killed if the interpreter exits while it is in user code) is
acceptable because the abandoned worker was explicitly abandoned
by shutdown(wait=False) anyway and the alternative is a stuck CI
job.

A regression test - test_no_non_daemon_worker_leak_after_executor_shutdown
- enumerates threading.enumerate() after the test and fails if
any thread whose name starts with &#39;synaflow-worker_&#39; is still
alive as a non-daemon thread.  test_parity was updated to mark
this test as sync-only since the async engine does not use a
ThreadPoolExecutor.

* fix(sync): drop _DaemonThreadPoolExecutor, log blocked pool workers instead (issue #103)

The daemon ThreadPoolExecutor subclass was a workaround that kept the
process alive past stuck workers but did not help the user identify
*which* worker was stuck.  In a clean fix:

* Use the stock concurrent.futures.ThreadPoolExecutor (workers are
  not daemon).
* After shutdown(wait=False, cancel_futures=True), call the new
  wait_for_workers_after_shutdown helper that polls alive workers
  every 0.5 s and logs a warning every 60 s with the worker names and
  the process pid.  If user code is stuck, the framework now blocks
  (still no daemon kill) but emits a diagnostic line per minute so the
  user can find the stuck step.
* The contract shifts explicitly: workers stuck *inside framework code*
  (SyncQueueIterator queue.get() with ExceptionMarker) wake up and
  exit; workers stuck *inside user code* are the user&#39;s responsibility.

The helper is a top-level function in executor.py and accepts test
seams (_enumerate_threads, _is_alive, _sleep,
_monotonic, _log, _process_pid) so tests inject mocks.

PipelineExecutor (and run()) expose worker_shutdown_poll_seconds
and worker_shutdown_log_every_seconds so tests can shorten the
60 s log window in production runs; defaults remain 0.5 s / 60 s.

Removed Test G (non-daemon leak regression - the leak is now expected
and the new function&#39;s contract is logged instead) and Test B (which
tested user code blocks via Event.wait() in a step function, the
user&#39;s responsibility).

Added unit tests for wait_for_workers_after_shutdown covering empty
threads, prefix filtering, log windows, multi-worker reporting, PID
fallback, and the poll_seconds parameter.

See Issue #103. ([`bc07617`](https://github.com/humansoftware/synaflow/commit/bc07617f3c6a76950da2fbe8892860388d208fde))


## v0.26.0 (2026-07-07)

### Feature

* feat: add design-time validation for declared/unused and used/undeclared resources (#102)

- validate_and_resolve_dependencies: when a step depends on a parameter
  that is not a resource, step output, or params field, if the type is a
  user-defined class (not builtins), add a hint suggesting the user may
  have forgotten to declare it in resources={}.
- validate_no_unused_resources (new): after step compilation, check that
  every resource declared in effective_resources is used by at least one
  step&#39;s deps. Raises ValueError if not.
- Added 4 new tests: undeclared resource used, declared resource unused,
  resource used (happy path), sub-pipeline resource used internally. ([`721ee92`](https://github.com/humansoftware/synaflow/commit/721ee92acc4a88065bda70eba8b33bbd82231159))


## v0.25.9 (2026-07-07)

### Fix

* fix: complete sub-pipeline resource inheritance to runtime (issue #100) (#101)

* feat(core): propagate merged resource factories to DAG for runtime

Adds Dag.resource_factories (runtime-only, non-serialized, mirrors
pipeline_observers). build_dag populates it from effective_resources
(the already-computed merge of parent + sub-pipeline resource factories
from _collect_pipeline_resources). This stores the resource factories
on the DAG where the runtime expects them (§3.8 DAG primacy) and
completes the existing inheritance contract from build time.

No change to to_dict() — JSON contract is unchanged. The runtime source
(pipeline.resources -&gt; pipeline.dag.resource_factories) is wired in
the next task.

* fix(sync): read inherited sub-pipeline resources from DAG at runtime

Changes run() to pass pipeline.dag.resource_factories to
PipelineExecutor instead of pipeline.resources. The merged factory dict
(now stored on the DAG by build_dag) is the runtime source, completing
the inheritance contract end-to-end.

Regression test mirrors issue #100: parent omits &#39;db&#39;, sub declares
&#39;db: get_db&#39;, sync run() injects the DB instance into the sub-step.

No change to PipelineExecutor / ArgumentBuilder signatures or behavior;
the executors already consult self._resource_factories (now sourced from
the DAG). Override precedence in argument_builder is unchanged.

* fix(async): read inherited sub-pipeline resources from DAG at runtime

Mirror of the sync fix. async_run() now passes
pipeline.dag.resource_factories to AsyncPipelineExecutor instead of
pipeline.resources. Restores sync/async parity (§3.8) for inherited
sub-pipeline resources. Regression test mirrors issue #100 in async.

The include adapter is an async def because async_run() enforces
all step fns to be async for an async pipeline
(PipelineDef._validate_no_sync_handlers).

* test: lock in conflict and multi-sub semantics for inherited resources

Adds three regression guards:
- Two subs declaring the same resource with the SAME factory instance
  must build and run, injecting the shared instance (sync + async
  parity — identical test name in both engines per test_parity.py).
- Two subs declaring the same resource with DIFFERENT instances must
  raise ValueError at build time (the existing _merge_resources
  identity check). Asserts it stays a design-time error, never runtime.

* style: ruff format test files

* fix: resolve resource factories at design time, store instances on Dag

Replaces dag.resource_factories with dag.resource_instances.

The merged resource factories (from parent + include() sub-pipelines)
are now resolved into concrete instances during build_dag (design time)
instead of being passed as factories to the runtime executor.

This fixes issue #100 properly — the executor never sees a factory
callable, resolving the design/build separation violation in the
previous approach.

Key changes:
- Dag.resource_instances: stores resolved instances (non-serialized)
- Dag.resource_factories: removed
- build_dag: calls each factory once after step compilation
- executors: receive resource_instances instead of resource_factories
- ArgumentBuilder: base resources are instances (never called);
  override resources may still be factories (backward compat)

Fixes: #100

* Revert &#34;fix: resolve resource factories at design time, store instances on Dag&#34;

This reverts commit 8a4498590761b79204b365bf4e767a9927d89c5c.

* refactor(dag): remove redundant dag.resources field, derive types from factories

The Dag dataclass had both resources (type metadata) and
resource_factories (callable factories), but the types were already
derivable from the factory return annotations.

Removes dag.resources entirely:
- dag.get(key) now checks resource_factories and computes the output
  type via resolve_resource_output_type() (lazy import in method body).
- dag.to_dict() computes the resources JSON section from factories
  at serialization time.
- ArgumentBuilder, ExecutionState, and ResourceRegistry use
  resource_factories for membership checks.

The Dag now has a single resource field: resource_factories.

* refactor(dag): move resolve_resource_output_type into dag.py as Dag._resource_type()

Removes inline imports from dag.get() and dag.to_dict() by moving
resolve_resource_output_type and get_safe_type_hints from
dag_dependencies.py into dag.py (top-level functions).

Adds Dag._resource_type(name) — a clean delegation method that
dag.get() and dag.to_dict() call instead of inlining the type
resolution logic. ([`d69de6a`](https://github.com/humansoftware/synaflow/commit/d69de6a9a0afbd5c679423d2df15b59323c107a6))


## v0.25.8 (2026-07-06)

### Fix

* fix: compile sync publish contract at design time (#99)

* fix: compile sync publish contract at design time

* test: expand execution plan coverage

* refactor: rename runtime contract validation helpers

* test: align sync stream fixtures with compiled contracts

* style: format files for ruff ([`33c13db`](https://github.com/humansoftware/synaflow/commit/33c13db58ae868d7196f47577b8fb7ac5ee2b033))


## v0.25.7 (2026-07-06)

### Fix

* fix: drain deferred barrier steps (#97) ([`ff86f4c`](https://github.com/humansoftware/synaflow/commit/ff86f4c61391eb0a75cd76011a1a46963f576b05))


## v0.25.6 (2026-07-06)

### Fix

* fix: lazy-start sync fanout pump (#95) ([`d3a7875`](https://github.com/humansoftware/synaflow/commit/d3a7875beb04cc3a2d22f77c952b14f3a426a511))


## v0.25.5 (2026-07-06)

### Fix

* fix: trigger release for merged #93 ([`51b595d`](https://github.com/humansoftware/synaflow/commit/51b595d085405333a9691dc1c20f0e7fe446626d))

### Unknown

* [codex] remove step output observers and fix MagicMock resource check (#93)

* remove step output observers and fix MagicMock resource check

* extract context manager detection helper ([`0d7c193`](https://github.com/humansoftware/synaflow/commit/0d7c1934240fdec8bd3576f4dd57aa46d16c1316))


## v0.25.4 (2026-07-05)

### Fix

* fix: resolve duplicate parameter name crash during sub-pipeline expansion (#91) ([`2815a68`](https://github.com/humansoftware/synaflow/commit/2815a684f5946166331e364fe9c9168670417f90))

### Refactor

* refactor: extract StepRunner / AsyncStepRunner and introduce StepRunStats to decouple executors (#89)

* feat: introduce StepRunStats and refactor StepLifecycle

* refactor: implement StepRunner and delegate sync step execution

* refactor: apply reviewer feedback for sync engine StepRunner and executor cleanup

* refactor: define runtime execution counts in StepConfig and improve type annotations

* refactor: implement AsyncStepRunner and delegate async step execution

* cleanup: remove runtime metrics mutation from DagNode definition classes

* refactor: add type annotations to sync executor and remove unused stats parameter from async executor

* refactor: apply final code review improvements and clean up DAG topology leak

* test: rename test_async_step_runner_simple and align test parity configuration ([`43e5558`](https://github.com/humansoftware/synaflow/commit/43e555867debee15cd217575ee57e23507cd8527))

* refactor: extract stream lifecycle wrappers and simplify executors (#88)

* feat: add LifecycleStream and AsyncLifecycleStream wrappers

* fix: resolve type annotations issues in lifecycle_stream and its tests

* fix: prevent multiple callback triggers in LifecycleStream and AsyncLifecycleStream after terminal state

* fix: execute startup callbacks inside try-except blocks and simplify type checks

* refactor: simplify sync engine executor using LifecycleStream

* refactor: simplify async engine executor using AsyncLifecycleStream

* chore: fully type wrap_started_stream and optimize isawaitable in AsyncLifecycleStream

* refactor: split lifecycle streams and tests into sync/async modules

* test: convert expected sync/async only sets to lists of tuples with explanations in test_parity.py

* test: clarify explicit reasons in test parity exclusion list

* test: align lifecycle stream test names to achieve parity and remove them from exclusion lists

* test: remove async prefix from remaining tests in expected_async_only and tests

* test: merge sync/async lifecycle stream test cases to eliminate parity list exclusions

* test: align runner compatibility and resource closing tests for 100% parity ([`d5c71a3`](https://github.com/humansoftware/synaflow/commit/d5c71a39dfdf2512cb5e0ec56b4765969aeabbdb))

* refactor: reunify executor and stream publisher, extract state management to ExecutionState (#87) ([`3dda1d8`](https://github.com/humansoftware/synaflow/commit/3dda1d8cd55e98a5a0d0967c8a37bb33072aebbc))

* refactor(async): decompose AsyncPipelineExecutor into SOLID components (#85)

* refactor: promote threshold.py to shared execution module

* refactor(async): extract AsyncEventDispatcher

* Fix issues from review: typed events parameter and removed refactor scripts

* refactor(async): extract AsyncDependencyResolver

* refactor(async): extract AsyncStreamPublisher

* chore: resolve final review findings ([`8326669`](https://github.com/humansoftware/synaflow/commit/8326669e6093a0e89b85cd7159ff1acac01f142d))

* refactor(sync): decompose PipelineExecutor into SOLID components (#84)

* refactor(sync): extract threshold logic to pure functions

* refactor(sync): extract EventDispatcher class

* refactor(sync): extract StepScope class for arg/resource lifecycle

* refactor(sync): extract StreamPublisher for fanout lifecycle

* fix(sync_engine): decouple executor and stream_publisher, fix circular import

* Refactor StreamPublisher to own event emission natively

* refactor: redistribute utils.py to rightful owners

* chore: remove test scripts and plan, ignore superpowers dir

* Refactor StepScope to DependencyResolver and add docstrings ([`20f4112`](https://github.com/humansoftware/synaflow/commit/20f411210aabef9e7b090a166f1c5f73035a6058))

### Unknown

* Executor architecture refinement (#86)

* docs: add design spec for executor architecture refinement

* docs: remove StreamPublisher and Error Flow from spec

* docs: add implementation plan for executor architecture refinement

* refactor: rename DependencyResolver to ArgumentBuilder

* Fix imports of AsyncArgumentBuilder in StreamPublisher

* feat: introduce StepLifecycle to encapsulate state tracking

* Fix executor issues from code review

* Fix executor issues from code review

* feat: enforce strict async boundaries

* Fix observer runtime, dag validation, and async adapter

* fix: add dag validation and remove scratch scripts

* fix: remove dynamic async_adapter and add active dag validation

* fix: remove magic strings and fix async_adapter wrapping

* fix: remove magic strings, strictly validate async boundaries, and safely adapt default error materializers

* chore: remove docs/superpowers and update gitignore

* docs: add design spec for context-aware factory async materializers

* docs: add implementation plan for factory async materializers

* refactor: add is_async_pipeline to contexts and remove legacy DagNode flags

* feat: make materializer factories context-aware and reorder dag compilation

* Fix type hints and remove unused variables

* Fix integration test failures from removing materializer backdoors

- Updated DAG expected snapshots in corpus.py since memory_materializer_factory now defaults to async_collection for async pipelines.
- Marked two tests as xfail which rely on the deprecated behavior where exceptions in stream iterations preserve items (async materializers consume the whole stream and drop partial results).

* Fix Task 3 issues: Revert unauthorized changes to tests, dag_builder, dag_steps, and definition

* refactor: simplify async StreamPublisher by relying on strict async materializers

* fix: resolve StreamPublisher tests and redundant materializer validation

* fix: implement stream tracking wrapper to preserve partial items in async materializers

* fix: clean up syntax error in test file

* test: rename test_dag_steps.py to test_validate_sync_async_consistency.py

* test: move imports to top of file

* fix: relax dag validation for non-callables so runtime checks handle them as before

* fix: enforce callable checks during dag validation and remove runtime fallback tests ([`5b6a623`](https://github.com/humansoftware/synaflow/commit/5b6a6230585f5250e265c8ab1bb0c2270aa62de8))


## v0.25.3 (2026-07-03)

### Fix

* fix(core): StepStarted fires on first input consumption (issue 78) (#82)

* fix(core): StepStarted fires on first input consumption (issue 78)

* style(tests): fix linting issues

* test(core): fix EventRecorder.record signature mismatch in sync tests

* style(core): format executor files

* fix(core): remove async engine hack for sync generators

* refactor(core): cleanup inline imports and simplify generator wrappers

* refactor(core): extract inline wrapper functions to module level

* refactor(tests): move PipelinePack imports to top of corpus files

* refactor(tests): move NamedTuple import to top of test_is_type_compatible.py and remove noqa

* refactor(tests): move inline imports to top in test_pep563_annotations.py ([`d1c5ab2`](https://github.com/humansoftware/synaflow/commit/d1c5ab2f44e1f3bee69ff54040c5a68a5abef5e8))


## v0.25.2 (2026-07-03)

### Fix

* fix(core): make observer success_count reflect logical item count for… (#81)

* fix(core): make observer success_count reflect logical item count for list outputs

* test(core): add regression test for logical item count observer logic (issue 80)

* test(core): add observer regression tests for logical item count (issue 80)

* fix(test): correct logical count in terminal step tests ([`4b8eb1d`](https://github.com/humansoftware/synaflow/commit/4b8eb1dc06bc69e545521690019cb8c6e401465d))

### Test

* test(core): add out-of-core custom materialization tests (#79) ([`2b186b3`](https://github.com/humansoftware/synaflow/commit/2b186b3d0715ffb7ed291a8a3d0a8f829f233599))


## v0.25.1 (2026-07-03)

### Fix

* fix(sync_handoff): do not drop item when enqueueing EOF on full branch queue (#77) ([`1c338b6`](https://github.com/humansoftware/synaflow/commit/1c338b6c66cb336d84d18548cb4280db868f9fd4))


## v0.25.0 (2026-07-03)

### Breaking

* refactor!: redesign lockstep execution engine and DAG materialization validation (#75)

* fix: update lockstep validation and fix tests

* fix: lint and coverage issues

* fix(tests): cover unmaterialized terminal streams branch with step_output_observers

* test: move cross-level bypass test to dag validation suite ([`41d6070`](https://github.com/humansoftware/synaflow/commit/41d6070358976eb04036d844d7c62fc103e973b6))

### Style

* style: format after imports fix ([`6f868d3`](https://github.com/humansoftware/synaflow/commit/6f868d3d6beaefb66b14e38316b417215e736be8))


## v0.24.0 (2026-07-03)

### Feature

* feat: Add Lockstep Symmetry Validation and prevent asymmetric deadlocks at design time ([`64529cc`](https://github.com/humansoftware/synaflow/commit/64529ccd04ab15d49dd6f53b655301da368c3c49))


## v0.23.1 (2026-07-01)

### Fix

* fix: trigger release for observer completion events ([`8122fc6`](https://github.com/humansoftware/synaflow/commit/8122fc6a461eb56e90ecb7bec65c1a4969541368))

### Unknown

* fix observer completion events (#73) ([`996333b`](https://github.com/humansoftware/synaflow/commit/996333b9b80822819099535b284f96f13317391c))

* [codex] add runtime context to error materializers (#71)

* add runtime context to error materializers

* simplify error materializer context api ([`08e8d34`](https://github.com/humansoftware/synaflow/commit/08e8d34dfaf46e8744cfe20ce5cfc92ce9c35ef2))


## v0.23.0 (2026-06-30)

### Feature

* feat: expose execution run_id in BaseObserverContext (#69)

* feat: expose execution run_id in BaseObserverContext

* test: add test suite for run_id consistency

* test: refine run_id test to reuse same pipeline instance ([`2cd3a6c`](https://github.com/humansoftware/synaflow/commit/2cd3a6c071a84e1ccc9c53f6076ac46388b66ab3))


## v0.22.0 (2026-06-30)

### Feature

* feat: support dataclass params in pipeline definitions (#68)

* feat: support dataclass params in pipeline definitions

* fix: resolve dataclass execution bugs and add tests for complex object param injection

* chore: remove temporary scratch files ([`902cfc4`](https://github.com/humansoftware/synaflow/commit/902cfc4fb68004e7c48072ec73a9619a9fe35bfa))


## v0.21.0 (2026-06-25)

### Chore

* chore: enable full ruff linting in pre-commit (#62) ([`e08f440`](https://github.com/humansoftware/synaflow/commit/e08f44047f5d0415077024db891d34314619be25))

### Feature

* feat: error threshold for EACH-mode steps + BREAKING: on_error=STOP no longer forces materialization (#63)

* refactor: drop on_error=STOP forced materialization (breaking)

- on_error=STOP no longer marks the producer for materialization in
  _plan_materialization. It is now a pure runtime policy: raise
  PipelineStopException on the first error.
- Remove Dag.requires_eager_materialization() -- the method was buggy
  (covered only 2 of 5 materialization reasons), was never called by
  any executor or production code, and is superseded by
  Dag.needs_materialize().
- Re-raise PipelineStopException in the per-item except inside the
  consumer&#39;s _unroll_step.generate() so that a STOP from an upstream
  producer propagates to the consumer (matches the runtime promise
  that &#39;consumers simply stop receiving items when the stream ends&#39;).
- Update 4 tests that relied on forced materialization to add explicit
  force_materialize=True (test_given_on_error_stop_with_downstream_*
  in sync + async, test_given_scalar_output_with_on_error_stop_* in
  sync + async).
- Remove 4 tests that validated the removed behavior.
- Drop now-unused OnError imports.
- Document the breaking change in materialization.md and CHANGELOG.

* feat: error threshold for EACH-mode steps (#)

- error_threshold_absolute: fail step after N failed invocations
- error_threshold_pct: (0, 1] fail step when error rate &gt;= P
- ThresholdExceededException propagates from run()/async_run()
- generate() dispatches StepEvent.FAILED/COMPLETED with actual counts
- execute() dispatches PipelineEvent.FAILED with step_name
- InvalidThresholdRaiseInEACHStep wraps manual raises from EACH steps
- Build-time: rejects STOP+threshold, ALL+threshold, bad values
- Sub-pipeline expansion propagates threshold fields
- Bug fix: COMPLETED events populate real per-item error counts
  for all on_error=CONTINUE steps (was always error_count=0)
- Docs: new Error Thresholds section in materialization.md

Design: threshold check runs pos-loop inside generate(), dispatches
events directly (no _emit_deferred_completion bypass). Consumer&#39;s
per-item except is never touched (exception propagates through
next() calls outside the try/except). Async _pump_iterator puts
ThresholdExceededException in consumer queue regardless of on_error.

* fix: remove unused Iterator import in async threshold tests

* chore: auto-format (ruff format)

* chore: revert manual CHANGELOG update (auto-generated on merge) ([`39ca329`](https://github.com/humansoftware/synaflow/commit/39ca32933fd11c41bf5f9cc2a51cc6b539dd7dff))


## v0.20.4 (2026-06-21)

### Fix

* fix(execution): replace lockstep level execution with ready-queue scheduler to prevent nested fanout deadlocks (#61)

This commit implements Option A as per Issue #60. It refactors the sync and async executors to use a ready-queue scheduler that starts steps dynamically as their inputs become available. This preserves lazy evaluation for nested EACH fanouts while preventing the pipeline from hanging on cross-level producer/consumer barriers. ([`686076d`](https://github.com/humansoftware/synaflow/commit/686076dd20c9effdda6677a81fae5a5ae1fc13a7))


## v0.20.3 (2026-06-21)

### Fix

* fix: remove uv lock from release ([`05871ec`](https://github.com/humansoftware/synaflow/commit/05871ec4854598eb51db1590116d213b59ba4148))

* fix: sync uv lock during release ([`17e4313`](https://github.com/humansoftware/synaflow/commit/17e4313b70ee615c6749cec379104676b224e27a))


## v0.20.2 (2026-06-21)

### Fix

* fix: trigger release for materialization contract refactor ([`8973f3c`](https://github.com/humansoftware/synaflow/commit/8973f3cb3624cfc3c2f0d73c3fda72cf469239a3))

### Refactor

* refactor: compile producer-level materialization contract (#59)

* fix: honor dag materialization plan in async executor

* fix: use dag plan for async materialization

* refactor: clarify merging fanout materialization planning

* refactor: centralize dag materialization planning

* refactor: share dag builder indexes and planning

* refactor: compile producer-level materialization contract

* refactor: drop eager branch handoff plumbing

* refactor: hide materialized deps from exported dag

* test: remove private materialization debug from corpus exports ([`fe6a0eb`](https://github.com/humansoftware/synaflow/commit/fe6a0ebdff5d709cfff22c3ba6ea8d53286c2474))


## v0.20.1 (2026-06-21)

### Documentation

* docs: add dedicated resources guide (#55) ([`e7d487f`](https://github.com/humansoftware/synaflow/commit/e7d487f89b3d0a4ce89ecd6f10e52cb333fd1f4a))

### Fix

* fix: resolve pipeline deadlock in merging fan-outs at compile-time ([`aa545ef`](https://github.com/humansoftware/synaflow/commit/aa545eff23fb62baa355cbd2abf1ed7d4e54abd3))

### Refactor

* refactor: refine merging fan-outs deadlock resolution to use edge-specific materialized_deps ([`5c31c0a`](https://github.com/humansoftware/synaflow/commit/5c31c0ab64cbf4b4bd9ade621b993a81978e2377))

### Test

* test: cover async resource provider errors (#54) ([`ada7c2a`](https://github.com/humansoftware/synaflow/commit/ada7c2a75ebe89e5ad742415045802550d83d1e6))


## v0.20.0 (2026-06-20)

### Feature

* feat: support callable resource overrides (#53) ([`ff6ca4b`](https://github.com/humansoftware/synaflow/commit/ff6ca4bc63323874c8198111118b09b611bdc841))


## v0.19.0 (2026-06-20)

### Feature

* feat: add production resource factories (#52) ([`fccbeae`](https://github.com/humansoftware/synaflow/commit/fccbeae06a8d27888f70cbc15ae1a6ee4929a79a))


## v0.18.0 (2026-06-20)

### Documentation

* docs: polish execution overrides documentation (#51) ([`ff81e55`](https://github.com/humansoftware/synaflow/commit/ff81e55b386918ebb4f92231fead7c2cbe139697))

### Feature

* feat: release execution overrides and runtime resources ([`9528034`](https://github.com/humansoftware/synaflow/commit/95280345752fba067c7531330a06864e21290ff4))

### Refactor

* refactor: simplify executor materialization and fallback logic (#46)

* refactor(core): catch specific exceptions (NameError, TypeError) in type hint evaluation

* test(core): add PEP 563 runtime execution and materializer tests

* test(core): add custom types and Future compatibility checks to type_compatibility tests

* refactor(tests): move pep 563 runtime tests from core to execution module

* chore(tests): remove unused Iterator import from core test file

* test: maintain parity by moving PEP 563 runtime tests to sync and async engines

* feat: only require custom materializer for custom types when needs_materialize is True

* test: modify non-builtin type test to consume as Iterator and not use a custom materializer

* test: add custom class type cases to is_type_compatible test

* refactor: use Dag.needs_materialize in _resolve_materializers instead of duplicating logic

* test: add custom NamedTuple and Iterator[Future] compatibility test cases

* ci: report Total Coverage as a GitHub Check Run in PRs

* refactor: simplify executor materialization and enforce callable error materializers

* refactor: simplify explicit pipeline materializer validation

* refactor: centralize dag materialization planning

* test: trim redundant executor materialization cases

* docs: align build-time materialization contract ([`fdc8e0e`](https://github.com/humansoftware/synaflow/commit/fdc8e0eca4c3d7e7bfff9736ca39742da0173e94))

* refactor(core): catch specific exceptions in type hint resolution (#44)

* refactor(core): catch specific exceptions (NameError, TypeError) in type hint evaluation

* test(core): add PEP 563 runtime execution and materializer tests

* test(core): add custom types and Future compatibility checks to type_compatibility tests

* refactor(tests): move pep 563 runtime tests from core to execution module

* chore(tests): remove unused Iterator import from core test file

* test: maintain parity by moving PEP 563 runtime tests to sync and async engines

* feat: only require custom materializer for custom types when needs_materialize is True

* test: modify non-builtin type test to consume as Iterator and not use a custom materializer

* test: add custom class type cases to is_type_compatible test

* refactor: use Dag.needs_materialize in _resolve_materializers instead of duplicating logic

* test: add custom NamedTuple and Iterator[Future] compatibility test cases

* ci: report Total Coverage as a GitHub Check Run in PRs ([`1762a31`](https://github.com/humansoftware/synaflow/commit/1762a315bfebeee751fcb803f55b43c458d3c766))

### Unknown

* add testability documentation (#50) ([`b67ac9f`](https://github.com/humansoftware/synaflow/commit/b67ac9f38e3176030302e235f6202ac8f9eb3cb6))

* [codex] Add runtime resource overrides (#49)

* add runtime resource overrides

* document runtime resource overrides

* inherit sub-pipeline resources in contract ([`228cb3f`](https://github.com/humansoftware/synaflow/commit/228cb3f74e45e979149c0f7e6dffaa750a69ea7f))

* [codex] Add observer execution overrides (#48)

* add observer execution overrides

* add scope helper for override keys

* accept builtin concrete materializers

* document scope override examples

* align observer empty overrides with design ([`97826b6`](https://github.com/humansoftware/synaflow/commit/97826b64c0e2df35535efca39e4009c8dedebaed))

* add materializer execution overrides (#47) ([`4a5acc7`](https://github.com/humansoftware/synaflow/commit/4a5acc7b556cb8f3b0486b3def4040de1a7dad7a))


## v0.17.3 (2026-06-19)

### Fix

* fix(core): allow bare collections to satisfy type compatibility checks (#43)

* fix(core): allow bare collections to satisfy type compatibility checks

* style(tests): format test_is_type_compatible.py using ruff ([`802c160`](https://github.com/humansoftware/synaflow/commit/802c160bcf70a4bce27ce3cbe01eaff152a3a5a0))


## v0.17.2 (2026-06-19)

### Fix

* fix(core): evaluate pep 563 annotations using typing.get_type_hints (#42)

* fix(core): evaluate pep 563 annotations using typing.get_type_hints

* test(core): add coverage tests for pep 563 annotation fallback paths ([`4e58181`](https://github.com/humansoftware/synaflow/commit/4e581814dd0fb6b802ce086daf45272d38349913))


## v0.17.1 (2026-06-19)

### Chore

* chore(tests): remove unused imports in custom_types corpus packs ([`e05a3ca`](https://github.com/humansoftware/synaflow/commit/e05a3ca7a2efdd0d08864b979d39a8cf1eec434a))

* chore: add site/ directory to .gitignore ([`524b29d`](https://github.com/humansoftware/synaflow/commit/524b29db950c7bb0bdefb013d4b5c556c88c1a57))

* chore: add OpenCode project config with agents, commands, and permissions ([`108a2d7`](https://github.com/humansoftware/synaflow/commit/108a2d7560b18ed4fee7a29335c5a2a8b774099c))

### Fix

* fix(core): allow custom materializers to bypass non-builtin inner type validation ([`2873429`](https://github.com/humansoftware/synaflow/commit/2873429689663fb1dce7f8826e1f6b1245b23297))

### Test

* test(execution): add runtime materialization tests for custom types ([`ae21523`](https://github.com/humansoftware/synaflow/commit/ae215236a9f22d94bae6183d32061316471674e3))

* test(corpus): add custom_types validation and execution tests ([`2b4207f`](https://github.com/humansoftware/synaflow/commit/2b4207ffdc7c6db2571244e657a16c75b19fa4d1))

### Unknown

* Merge pull request #41 from humansoftware/fix/issue-38-materializer-validation

fix(core): allow custom materializers to bypass non-builtin inner type validation ([`5b8b959`](https://github.com/humansoftware/synaflow/commit/5b8b959343ee65fcd2c854f2e905065bd15e5ceb))


## v0.17.0 (2026-06-18)

### Ci

* ci: separate lint, test, and coverage into parallel jobs

Split the single test job in ci.yml (which ran lint + install + tests
sequentially) into three independent parallel jobs:

- lint — only needs ruff via uvx, skips package installation (faster)
- test — builds/installs the wheel and runs pytest
- total-coverage — runs pytest --cov, posts Total Coverage and Patch
  Coverage checks to GitHub (gated on pull_request events)

The Total Coverage check now enforces an 80% threshold (previously
always success) and includes a per-file coverage report table in its
summary, so the full report is visible directly on the PR checks
without digging into job logs. Patch Coverage keeps its 80% threshold.

Consolidated the standalone test-coverage.yml workflow into the
total-coverage job and deleted the old file. Pre-commit behavior is
unchanged locally — everything still runs together via the pytest hook. ([`2441ba3`](https://github.com/humansoftware/synaflow/commit/2441ba3b019ae99b94ffa3bfc1c83a140dc780a6))

### Feature

* feat: validate terminal steps with unmaterialized stream output at build time

A terminal step (no consumers) whose output is Iterator/Generator or
AsyncIterator/AsyncGenerator and whose output is not materialized
(needs_materialize is False) produces a stream that nobody drains. At
runtime this causes either a deadlock (bounded handoff pump blocks
forever) or silent data loss (the pump discards items to deliver the
EOF marker).

Add validate_no_unmaterialized_terminal_streams in dag_steps.py, called
from build_dag after materialized_deps are computed. Uses the existing
needs_materialize flag -- no new materialization logic. Steps exported
via sub-pipeline exports are skipped because they will have consumers
in the parent. Also propagate force_materialize through sub-pipeline
expansion (was missing).

Fix existing tests with latent terminal Iterator-returning steps:
- corpus complex_parallel_mixed: step3 -&gt; list[int]
- observer runtime: lazy_consumer/passthrough -&gt; None (drain input)
- dag_materializer: add force_materialize=True (test targets UNRUNNABLE)
- dag_expansion: add force_materialize=True (test targets DAG structure)
- materializers_ergonomics: add force_materialize=True on sub-pipeline gen
- async_runner_basic: add force_materialize=True (test targets RuntimeError)

Document in DESIGN_PHILOSOPHY that observers receiving an Iterator must
consume it fully (application responsibility, causes tee buffer growth
otherwise). ([`26b999f`](https://github.com/humansoftware/synaflow/commit/26b999f21c218635ac2480ced6dee93ee84949e4))

### Refactor

* refactor: remove dead branches in async _pump_iterator and drop put_terminal

- Collapse the two isinstance(q, AsyncQueueBranch): await q.put(item)
  else: await q.put(item) blocks in async _pump_iterator (both arms
  were byte-for-byte identical) into a single await q.put(item), and
  unify the materialize_before_enqueue / plain paths under one async
  for loop.
- Remove AsyncQueueBranch.put_terminal: it was functionally identical
  to put (same active-gated put_nowait loop), so collapse callers in
  _pump_iterator to use put for terminal markers too.
- Add pytest-timeout dev dependency and a per-test timeout=10s in
  pyproject.toml so a hung streaming test fails loudly instead of
  stalling the suite.

The sync SyncFanout._put_terminal busy-wait + get_nowait() drop is
intentionally left untouched here: removing the drop without a full
shutdown redesign of SyncFanout (terminal consumer that never drains
its SyncQueueIterator deadlocks the pump) turns it into a hard hang.
That belongs in its own PR. ([`dc8bebf`](https://github.com/humansoftware/synaflow/commit/dc8bebf6b488e22fe9d9b4b8c09a7ca509e7d835))

### Unknown

* Merge pull request #37 from humansoftware/feat/validate-terminal-stream-output

feat: validate terminal steps with unmaterialized stream output at build time ([`43e5787`](https://github.com/humansoftware/synaflow/commit/43e5787e4b31e8abc2d868f9991a32cc7f8804e3))

* Merge pull request #35 from humansoftware/ci/separate-lint-test-coverage

ci: separate lint, test, and coverage into parallel jobs ([`2377af9`](https://github.com/humansoftware/synaflow/commit/2377af95e6d0a62fe4e36bb3cb2df0bc36c5c6fc))

* Merge pull request #34 from humansoftware/refactor/max-in-flight-cleanup

refactor: remove dead branches in async _pump_iterator and drop put_terminal ([`9d9a325`](https://github.com/humansoftware/synaflow/commit/9d9a325ab9619cb656a28a12f060f8c1537d47ef))


## v0.16.0 (2026-06-18)

### Documentation

* docs: add final finished state t11 to interactive animation ([`a12d697`](https://github.com/humansoftware/synaflow/commit/a12d6974a4cfe47de8b856095b88c093d4759472))

* docs: add interactive visualization for max_in_flight ([`c9ca483`](https://github.com/humansoftware/synaflow/commit/c9ca48372f647871795ca545cd911818b893203b))

* docs: add max_in_flight parallels to java-streams and linq comparison pages ([`830636b`](https://github.com/humansoftware/synaflow/commit/830636bace08990714906e65abc49cd5156d8a04))

* docs: restructure max-in-flight page with Sync/Async tabs ([`917e367`](https://github.com/humansoftware/synaflow/commit/917e36790a35e462f62db341a748b02f09f1f2d9))

* docs: expand max_in_flight documentation ([`c4059a6`](https://github.com/humansoftware/synaflow/commit/c4059a68e342a61a1644c9af86413d897e679b7b))

### Feature

* feat: complete max_in_flight runtime and docs ([`c3f54d5`](https://github.com/humansoftware/synaflow/commit/c3f54d5d713392598b6f25211d6960ed9f58a1c0))

* feat: add max_in_flight tests and fix bounded iterator

- Add test_dag_builder_max_in_flight.py (8 build-time validation tests)
- Add test_runner_max_in_flight.py for sync and async (4 tests each)
- Fix BoundedIterator: propagate exceptions immediately, don&#39;t buffer
- Fix test parity by using identical test function names in sync and async ([`27c0fdd`](https://github.com/humansoftware/synaflow/commit/27c0fdd96b6489417504a51a1bd93c3463874a31))

* feat: add max_in_flight with bounded handoff for sync and async

- Add max_in_flight: int = 1 to Step and DagNode
- Validate max_in_flight &gt;= 1 and integer at build time
- Serialize max_in_flight in DAG JSON (always present)
- Sync: BoundedIterator deque-backed wrapper for max_in_flight &gt; 1
  (max_in_flight=1 preserves exact current behavior)
- Async: use max(max_in_flight, 100) for queue sizing
- Update all 18 corpus snapshots with max_in_flight: 1 ([`c838678`](https://github.com/humansoftware/synaflow/commit/c83867804d379ca10c056a855dc627e19d1734b0))

### Fix

* fix: async queue sizing, observer threads, bounded handoff tests

Codex changes:
- AsyncQueueBranch: implement __aiter__/__anext__ for direct async iteration
- Async executor: _attach_argument_cleanup, _close_stream_arguments
- Async executor: queue sizing uses max(2, max_in_flight+1) for EOF
- Async executor: _resolve_queue handles AsyncQueueBranch type
- Sync executor: observer threads via SyncFanout branches
- Sync executor: _observer_threads with cleanup on pipeline finish
- Sync executor: _notify_observers in publish stream paths
- 12 new tests: ahead_distance_bounded, producer_blocks,
  terminal_lazy_drains, flattening_stream_internal_items (sync+async)
- 3 new observer tests: does_not_force_eager, does_not_consume_slots,
  bound_is_unchanged (sync+async) ([`73e92a9`](https://github.com/humansoftware/synaflow/commit/73e92a958b397cd009e41bf16fd79b6f3577a0b4))

* fix: use producer&#39;s max_in_flight for async unroll queue sizing

- Async _unroll_step: look up producer node&#39;s max_in_flight instead
  of using consumer node&#39;s (fixes reviewer issue #1)
- Keep 100 minimum queue size for publish/unroll to prevent pump
  deadlock (v1 limitation — reviewer issue #2 acknowledged)
- Sync tee fan-out limitation documented as v1 constraint ([`0d8b75b`](https://github.com/humansoftware/synaflow/commit/0d8b75bbb5e2a96efdb486fae7ba14673d202476))

* fix: honor max_in_flight contract for sync fan-out and async queue sizing

- Sync fan-out: apply BoundedIterator to source BEFORE itertools.tee
  so producer advancement is bounded at the source level
- Async queue sizing: use node.max_in_flight directly when &gt; 1,
  keep 100 for default=1 (backward compatible)
- Add tests: sync fan-out bounded, async bounded ahead verification,
  parity between sync and async test names ([`5d8059d`](https://github.com/humansoftware/synaflow/commit/5d8059dbf81a0a918b70999af627a93b04107e07))

### Refactor

* refactor: move execution graph helpers onto Dag ([`7601b76`](https://github.com/humansoftware/synaflow/commit/7601b76ed13c583fa8b9e257a17a96a514af7293))

* refactor: clean residual imports and shared executor helpers ([`dcb9408`](https://github.com/humansoftware/synaflow/commit/dcb9408cae5041bdad5703a590e7a73d9e389164))

* refactor: structure pipeline executors ([`15fb065`](https://github.com/humansoftware/synaflow/commit/15fb0654642ee0022dec35566a0a62a180b535cd))

* refactor: structure DAG builder and include expansion ([`a9deeab`](https://github.com/humansoftware/synaflow/commit/a9deeab4661fee41ff23e1e4e663356f92d142bc))

* refactor: stabilize observer defaults and build helpers ([`07711b4`](https://github.com/humansoftware/synaflow/commit/07711b4c5d57132088ce2ec96b63c1d9224cb019))

* refactor: replace **kw with explicit params in observer dispatch, direct attribute access

- Sync and async executors: replace **kw in _dispatch_pipeline_event,
  _dispatch_step_event, _dispatch_materialization_event with explicit
  parameters (step_name, exception, success_count, error_count,
  completed_all_inputs)
- Replace getattr(node, &#39;observers&#39;, None) with node.observers (DagNode
  always has observers as a list)
- Replace getattr(step, &#39;error_materializer&#39;) → step.error_materializer
- Replace getattr(step, &#39;parent_pipeline&#39;) → step.parent_pipeline
- Replace getattr(step, &#39;observers&#39;) → step.observers in dag_builder
- Replace getattr(node, &#39;observers&#39;, []) → node.observers in definition
- Keep getattr for IncludeStep (lacks Step attributes) in dag_expansion
  and dag_steps validate_sync_async_consistency ([`5b6467d`](https://github.com/humansoftware/synaflow/commit/5b6467d1c663ab05653bc8ef26732816d1bf9651))

### Style

* style: ruff format codex changes ([`205afba`](https://github.com/humansoftware/synaflow/commit/205afba8cbf404ae41ce6b66827175386c170420))

* style: format PR1 changes ([`a95f620`](https://github.com/humansoftware/synaflow/commit/a95f62012f8baa5f994cf3a6e68e09f9a01db2cf))

### Test

* test: add runner contract, adapter serialization tests and fix threadpool corpus output type ([`c16170d`](https://github.com/humansoftware/synaflow/commit/c16170d1aaf9a2c9a126b9e26740d8b5df5d7bb3))

* test: add max_in_flight runtime coverage ([`4d44897`](https://github.com/humansoftware/synaflow/commit/4d44897f5f335263a883c31dbfd975cb7341d4b3))

* test: expand max_in_flight coverage ([`c4b58ba`](https://github.com/humansoftware/synaflow/commit/c4b58ba1ec2ea63f1d99343ea4bfb647e5a197de))

* test: add BoundedIterator unit tests, fix exception deferral

- Add 10 unit tests for BoundedIterator edge cases
- Fix exception handling: buffer items before raising, only raise
  pending exception when buffer is empty
- maxsize validation, empty source, partial iteration covered ([`269a7d8`](https://github.com/humansoftware/synaflow/commit/269a7d8a0a9835ec5000983db6d1635599737d84))

* test: cover Dag execution helper methods ([`344a64d`](https://github.com/humansoftware/synaflow/commit/344a64d8fa9c9555080ae953c11c4f96c15a55a3))

### Unknown

* Merge pull request #33 from humansoftware/feat/max-in-flight-clean

feat: max_in_flight — bounded stream handoff ([`4879c70`](https://github.com/humansoftware/synaflow/commit/4879c702ffc82956d724ef5e6ffaab4b1884a22a))

* fix max_in_flight expansion and docs ([`0bfc670`](https://github.com/humansoftware/synaflow/commit/0bfc670b10935dcc5c47ee944e1c67f7eea969a6))

* Merge pull request #31 from humansoftware/refactor/pr4-residual-cleanup

[codex] Refactor PR4: residual cleanup and shared executor helpers ([`e2d8787`](https://github.com/humansoftware/synaflow/commit/e2d878763b4d7c0e3ec482ba3a4336e0aeea7cc9))

* Merge pull request #30 from humansoftware/refactor/pr3-executor-structure

[codex] Refactor PR3: structure pipeline executors ([`b7a355a`](https://github.com/humansoftware/synaflow/commit/b7a355a56b154c89695192f6ea2379044cd03858))

* Merge pull request #29 from humansoftware/refactor/pr2-builder-expansion-structure

[codex] Refactor PR2: structure DAG builder and include expansion ([`bac95d6`](https://github.com/humansoftware/synaflow/commit/bac95d6bba8e8a8f4d62ed11df234358d85106a5))

* Merge pull request #28 from humansoftware/refactor/pr1-internal-contracts

[codex] Refactor PR1: stabilize observer defaults and build helpers ([`c240e4a`](https://github.com/humansoftware/synaflow/commit/c240e4a908500fe0f20636224ad0614c8e06605b))

* Merge pull request #27 from humansoftware/refactor/observer-dispatch-cleanup

refactor: explicit observer dispatch params, direct attribute access ([`f20fd3c`](https://github.com/humansoftware/synaflow/commit/f20fd3cb05fd63c238a800f839cab2413556b495))


## v0.15.0 (2026-06-15)

### Documentation

* docs: rewrite README to reflect current documentation

- Concise quickstart matching landing page
- Highlights: type-hint wiring, lazy streaming, static validation, custom runners
- Expanded comparison table with links
- Links to all documentation sections
- Badges (PyPI, license, Python) ([`79e63dc`](https://github.com/humansoftware/synaflow/commit/79e63dc7a62159bb5fbe49ea09197f3020fb434f))

* docs: add Google Analytics tracking ID ([`a83a636`](https://github.com/humansoftware/synaflow/commit/a83a636748de18c1998c324a40c5265d0da78cdc))

* docs: improve SEO metadata in mkdocs.yml ([`9b06873`](https://github.com/humansoftware/synaflow/commit/9b06873535b5518873f81881e2a1d9e05e63cdcc))

* docs: add LINQ comparison, concise landing page examples, reorganize comparisons section

- Move java-streams to new Comparisons section
- Add LINQ comparison page (Select/Where/GroupBy/ToList mapping)
- Add concise type-hint wiring + lazy streaming examples to homepage
- Update introduction page to mention Comparisons section ([`1e9fcef`](https://github.com/humansoftware/synaflow/commit/1e9fcef7278427adbd59798e1a2d9d506a9b3365))

* docs: add interactive lockstep animation + expanded framework comparison

- Replace static table/sequence diagram on lockstep-flow with interactive
  frame-by-frame animation (play/pause, step navigation)
- Shows pipeline executing concurrently: numbers yielding item 2 while
  doubler processes item 1 and printer prints item 0
- Expand How It Compares table on homepage with Dagster, Prefect, Airflow
- Add framework comparison table to DESIGN_PHILOSOPHY.md section 2.4 ([`09849db`](https://github.com/humansoftware/synaflow/commit/09849db2e0ce3399c2b6b139e9ee8b69fce1bbac))

* docs: rewrite lockstep-flow + enable branch preview deploys

- Rewrite lockstep-flow with pipeline example first, DAG diagram,
  execution walkthrough table, fan-out section, execution levels
- Single docs.yml workflow deploys via peaceiris/actions-gh-pages
  - main branch → root of gh-pages
  - feat/* branches → preview/&lt;branch&gt;/
- No environment restrictions — deploys directly to gh-pages branch

IMPORTANT: GitHub Pages must be set to &#39;Deploy from branch&#39;
  (gh-pages, / root) for this to work. ([`f5eec27`](https://github.com/humansoftware/synaflow/commit/f5eec27f9680d09597c859612b47edfd5129a9ab))

### Feature

* feat: add cookiecutter templates for quick project scaffolding

- boilerplates/minimal: single-file app with pipeline.py
- boilerplates/structured: multi-file project (steps, pipeline, main)
- boilerplates/scaffold: single module to drop into existing project
- Update installation docs with cookiecutter quickstart section
- Exclude boilerplates/ from ruff in pre-commit config ([`d69bae0`](https://github.com/humansoftware/synaflow/commit/d69bae0374a8f454270d90250288a3a1405a9fb6))

### Unknown

* Merge pull request #26 from humansoftware/feat/cookiecutter-templates

feat: cookiecutter templates for project scaffolding ([`50825eb`](https://github.com/humansoftware/synaflow/commit/50825eb4ff89f5fcf4acbc4d5c1cf5d38c2e85de))

* Merge pull request #25 from humansoftware/feat/readme-update

docs: rewrite README to reflect current documentation ([`2d5be75`](https://github.com/humansoftware/synaflow/commit/2d5be7565fe39fd23baae81b1900bbbdbd0e6315))

* Merge pull request #24 from humansoftware/feat/landing-page-examples

docs: landing page overhaul, comparisons, build-vs-run architecture ([`11e243b`](https://github.com/humansoftware/synaflow/commit/11e243b93a866d8954ea201e4afe08820d84eafe))

* Merge pull request #23 from humansoftware/feat/docs-animation-comparison

docs: interactive lockstep animations, expanded comparisons, Java Streams &amp; event-based processing ([`c920014`](https://github.com/humansoftware/synaflow/commit/c920014adb7e136d50b7cebfc992bc3e6d12ca1b))

* Merge pull request #22 from humansoftware/feat/docs-improvements

docs: improve lockstep-flow page + enable branch preview deploys ([`0e4bda3`](https://github.com/humansoftware/synaflow/commit/0e4bda33cd0d9d377354df361078fe610f96988a))


## v0.14.0 (2026-06-14)

### Feature

* feat: documentation portal with MkDocs Material

- Add mkdocs-material&gt;=9.5 to dev dependencies
- Configure mkdocs.yml with Material theme, code tabs, Mermaid support
- Create .github/workflows/docs.yml to deploy to GitHub Pages on merge
- Create scripts/visualize_dag.py (JSON to Mermaid flowchart)
- Write comprehensive documentation under docs/user_docs/:
  - Introduction &amp; Getting Started (installation, lockstep flow)
  - Step-by-step tutorial (4 levels: hello world, multi-step, observers, materializers)
  - Core Concepts (semantic naming, DAG construction, sync/async parity)
  - Advanced Guides (custom materializers, observers, export guidance)
- All code snippets use sync/async tabs via pymdownx.tabbed
- Update README.md with link to documentation portal
- Delete docs/specs/documentation_portal.md (spec implemented)
- Update docs/ROADMAP.md — moved to Completed ([`c05e533`](https://github.com/humansoftware/synaflow/commit/c05e5331d6e368cfbd9563995374edefd8a68000))

### Unknown

* Merge pull request #21 from humansoftware/feat/documentation-portal

feat: documentation portal with MkDocs Material ([`2ef6da0`](https://github.com/humansoftware/synaflow/commit/2ef6da0d8bc42bf6660154f61a48fa057d47c306))


## v0.13.0 (2026-06-14)

### Feature

* feat: implement Smart Binding &amp; Semantic Step Naming

- Add inflect&gt;=7.0 dependency for plural/singular resolution
- New module synaflow/core/naming.py with get_base_dataset_name()
- Smart binding at build time: deps keys are base names (producer names),
  DagNode.dataset_param_names maps base name → original param name
- Executors use dataset_param_names for function calling; no runtime resolution
- Build-time validations:
  - Duplicate Base Datasets (e.g., &#39;user&#39; and &#39;users&#39; steps)
  - Duplicate Parameters within a single function
- dag.py: consumers_of and get_execution_levels simplified (deps are base names)
- materialized_deps use dep keys directly (already base names)
- Corpus pipelines updated (producer &#39;numbers&#39;, transformer param &#39;number&#39;)
- 16 naming tests + 3 validation tests
- Delete docs/specs/step_naming_rules.md (spec implemented)
- Update docs/ROADMAP.md and docs/DESIGN_PHILOSOPHY.md ([`c5c4ba5`](https://github.com/humansoftware/synaflow/commit/c5c4ba555b9b8b0bdf9cab91085318b9a61df848))

### Unknown

* Merge pull request #20 from humansoftware/feat/semantic-naming

feat: Smart Binding &amp; Semantic Step Naming ([`ff2d1b3`](https://github.com/humansoftware/synaflow/commit/ff2d1b3995dee13df8924da799f5bf006c5e436a))


## v0.12.0 (2026-06-14)

### Feature

* feat: add patch coverage check (80%) to pre-commit hook

- Refactor coverage_report.py to support --precommit mode
- Pre-commit shell script calls coverage_report.py --precommit
- Single source of truth for coverage computation ([`05efffb`](https://github.com/humansoftware/synaflow/commit/05efffb00edaa6fabec04271b5137b59039ceab0))

* feat: add coverage to pre-commit pytest hook ([`0bba7dc`](https://github.com/humansoftware/synaflow/commit/0bba7dcf4a3dd755535e855ccfb3e03da3d6dea6))

### Unknown

* Merge pull request #19 from humansoftware/feat/precommit-coverage

feat: add patch coverage check (80%) to pre-commit hook ([`2f8302a`](https://github.com/humansoftware/synaflow/commit/2f8302a19e18e3f2abc1c389052e6620f286f8c3))


## v0.11.0 (2026-06-14)

### Chore

* chore: address PR #18 review comments

- Add [tool.coverage.run] omit for tests/ and scripts/
- Delete docs/specs/test_coverage_ci.md (spec implemented)
- Move Test Coverage CI to Completed in ROADMAP.md ([`19b671b`](https://github.com/humansoftware/synaflow/commit/19b671b07add691f1ba7afa982cbdfb531b2dbcc))

### Documentation

* docs: update roadmap to reflect in-progress specs ([`46a09bf`](https://github.com/humansoftware/synaflow/commit/46a09bf6abfab1dc0aea67e311d9142a12b2ddba))

* docs: add pypi version and install instructions to documentation spec ([`6dcebc9`](https://github.com/humansoftware/synaflow/commit/6dcebc980b55d79ee8a128b70f95e4ff4625e5a0))

* docs: upgrade spec 3 to full documentation portal and visualization ([`b2c73fd`](https://github.com/humansoftware/synaflow/commit/b2c73fdb26c15c46f1ffd8419773ad80f36ec3d5))

* docs: add pre-commit hook requirement for testing to coverage spec ([`7aa5ead`](https://github.com/humansoftware/synaflow/commit/7aa5ead743180eecbb115bff6b77ca6b1e753dd3))

* docs: rewrite coverage spec for dual metrics and non-blocking statuses ([`7145f7b`](https://github.com/humansoftware/synaflow/commit/7145f7b52b1502061ad03b8102f5d83f4bf9a0c2))

* docs: detail coverage spec with pyproject.toml and PR comment action ([`9d4418b`](https://github.com/humansoftware/synaflow/commit/9d4418b8c865ae140fc91382951b6765edd77a8b))

* docs: add specs for naming rules, test coverage, and export guidance ([`fa92f3a`](https://github.com/humansoftware/synaflow/commit/fa92f3a43c3f7b54a0e8f0cdb762001a6651938f))

### Feature

* feat: implement dual test coverage CI (total + patch at 80%)

- Add .github/workflows/test-coverage.yml workflow triggered on PRs
- Add .github/scripts/coverage_report.py to compute and post check runs
- Add pytest hook to pre-commit config
- Add pytest-cov to dev dependencies
- Update .gitignore for coverage artifacts ([`0580864`](https://github.com/humansoftware/synaflow/commit/05808640e97a27892c4d6b36680d57563bef891c))

### Unknown

* Merge pull request #18 from humansoftware/feat/test-coverage-ci

feat: implement dual test coverage CI (total + patch at 80%) ([`0a4072c`](https://github.com/humansoftware/synaflow/commit/0a4072c03ba3b6f4b8fa01673504edfc882bfbdc))

* Merge pull request #17 from humansoftware/feature/update-roadmap-and-specs

docs: add specs for naming rules, coverage, and export guidance ([`cbb505b`](https://github.com/humansoftware/synaflow/commit/cbb505b1617426486193ccd0a7913ef862058ff5))


## v0.10.0 (2026-06-14)

### Chore

* chore: update uv.lock with ruff dependency ([`088fd05`](https://github.com/humansoftware/synaflow/commit/088fd05f7cfabea7e263660f3c9b4feebf2a8500))

### Documentation

* docs: clarify observer scope model; restore pipeline_observers to DAG JSON

Documentation:
- Step-level observers never receive PipelineEvent.*
- Pipeline-level observers receive PipelineEvent.* + inherited by all steps
- DAG JSON: pipeline_observers at root, observers per step

DAG JSON now includes compiled pipeline observer metadata alongside
per-step effective observer lists. Both carry handler_name + source. ([`20eb757`](https://github.com/humansoftware/synaflow/commit/20eb757962eb13c28e2a24f38bce57de4d36198c))

### Feature

* feat: unified lifecycle observer system

Introduces a generic observer infrastructure for pipeline, step, and
materialization lifecycle events with sync/async parity.

- Public API: Observer, PipelineEvent, StepEvent, MaterializationEvent
- Step-level observers compile into the same model as pipeline-level
- Effective observers resolved at build time, stored in DagNode
- DAG JSON includes observer metadata (event + source), never callables
- Fire-and-forget dispatch: observer failures are logged and swallowed
- Async handler detection via awaitable protocol (not iscoroutinefunction)
- 54 new tests: 15 build-time, 18 sync runtime, 21 async runtime
- Corpus pack updated for integration coverage
- Docs updated: DESIGN_PHILOSOPHY (3.15) and ROADMAP ([`385fedc`](https://github.com/humansoftware/synaflow/commit/385fedc2015e15b78a60c78e6bcb4aa6831accc2))

### Fix

* fix: propagate observers through include() / sub-pipeline expansion ([`35d2505`](https://github.com/humansoftware/synaflow/commit/35d250595db21b397e3605761f185cfe3f68eb41))

* fix: three remaining codex issues

1. Async _run_step isinstance check covers AsyncIterator/AsyncGenerator/Generator
2. ResolvedObserver internal type replaces _source mutation
3. dispatch_observers_async uses inspect.isawaitable

Both sync and async repros now emit single terminal FAILED with real exception. ([`3e493c1`](https://github.com/humansoftware/synaflow/commit/3e493c1c6ae44ebe302a429d8c1aecef6a512d95))

* fix: thread real exception through StepFailedContext; remove pipeline_observers from DAG JSON

1. _collect_iterator returns (items, had_error, exception)
   _apply_materializer and _materialize_with_events propagate it
   _emit_step_result passes it to StepFailedContext.exception
   Applied to both sync and async executors.

2. DAG JSON: removed redundant top-level pipeline_observers field.
   Effective per-step observer lists already preserve source metadata.
   Pipeline observers are inherited into each step node with source=&#34;pipeline&#34;.

Verified codex repro now emits:
(&#39;gen&#39;, &#39;step_failed&#39;, 1, 1, False, &#39;ValueError&#39;) ([`bc575a9`](https://github.com/humansoftware/synaflow/commit/bc575a9b77defd985405adca5fc1397541f63b02))

* fix: defer ALL-mode Iterator step COMPLETED until consumption

For ALL-mode steps that return an Iterator, COMPLETED was emitted
prematurely in _run_step before the iterator was consumed. If the
iterator later failed during downstream materialization, the step
remained marked as COMPLETED instead of FAILED.

Now: ALL-mode Iterator steps have their COMPLETED deferred to
_publish_output, same as EACH-mode steps. The _emit_step_result
helper checks had_error and emits StepFailedContext when the
iterator fails during consumption.

Confirmed with codex repro: gen yields 1 item then raises; downstream
list consumer triggers materialization. Now correctly emits:
(&#39;gen&#39;, &#39;step_failed&#39;, 1, 1, False, None) ([`be65d6a`](https://github.com/humansoftware/synaflow/commit/be65d6a9b9654570026d06ae3ad38a0c2118fcab))

* fix: DAG JSON source preservation and lazy iterator step lifecycle

1. DAG JSON: pipeline observers inherited into steps now preserve
   source=&#34;pipeline&#34; on the step node instead of source=&#34;step&#34;.
   Uses _source attribute tagged during build_dag normalization.

2. Lazy iterator step lifecycle: when an EACH-mode step&#34;s iterator
   fails during consumption (OnError.CONTINUE), StepFailedContext is
   now emitted instead of StepCompletedContext.
   _collect_iterator and _apply_materializer return (result, had_error).
   _emit_each_step_result dispatches COMPLETED or FAILED accordingly.
   Applied to both sync and async executors.

3. Added tests for source preservation on step-level and mixed observers. ([`000e00a`](https://github.com/humansoftware/synaflow/commit/000e00a356b4942b87b3b5ef159d926a6f970455))

### Refactor

* refactor: align Observer API with updated spec

- Observer now carries only handler (no event field)
- dispatch_observers(registrations, context) — no event param, calls all handlers
- Event filtering done via wrapper helpers inspecting ctx.event
- Pipeline observers inherited by all steps (receive pipeline + step + mat events)
- DAG JSON metadata uses handler_name instead of event
- Tests updated: on_event() wrapper pattern matching spec recommendations
- 340 tests passing ([`2bf6ee6`](https://github.com/humansoftware/synaflow/commit/2bf6ee6f39289003ce6039434958aba4dc9d0f59))

### Unknown

* Merge pull request #15 from humansoftware/feature/observer-system

feat: unified lifecycle observer system ([`5fdeeb1`](https://github.com/humansoftware/synaflow/commit/5fdeeb140accc888d709e264f7b60eaf6160a2a4))


## v0.9.1 (2026-06-14)

### Documentation

* docs: remove stale SyncStreamManager reference from coding standards ([`ee522cf`](https://github.com/humansoftware/synaflow/commit/ee522cf63c38bc5f2a86fd2a783521f18737b3e6))

* docs: fix inconsistencies in design philosophy

- Clarify materializer resolution (runtime still handles factory-with-context)
- Rename needs_materialize→materialized_deps section (matches code)
- Replace adapt_argument_to_consumer_type section (removed from code)
- Add sections: inline executors, step_output_observers, PipelineStopException context ([`efa3698`](https://github.com/humansoftware/synaflow/commit/efa36984baf1c54fd72e134636afa92c529b2b53))

### Fix

* fix: make materializer signature validation strict and prevent swallowing signature exceptions ([`c400779`](https://github.com/humansoftware/synaflow/commit/c400779622fd429b8fafa69615e8595b054a618d))

* fix: support async sub-materializers and handlers in composite materializers ([`1b618bb`](https://github.com/humansoftware/synaflow/commit/1b618bbda4857cc72d9aeb360bd44d28a9b47932))

* fix: resolve sub-pipeline laziness regression and align DAG serialization contract ([`cff719b`](https://github.com/humansoftware/synaflow/commit/cff719b3d2a453e1fc2ebb92c448999e68fdc7b6))

### Refactor

* refactor: simplify laziness design by removing has_step_materializer and relying on force_materialize ([`50b09f4`](https://github.com/humansoftware/synaflow/commit/50b09f4dfd56f54109bd60d72dc6cdc59a08cd2b))

### Test

* test: refactor error_handling corpus verification to dedicated parity tests ([`66f0c94`](https://github.com/humansoftware/synaflow/commit/66f0c94c0e443fed6aa27212f322c8e98b6cca11))

* test: add explicit regression tests for lazy steps with step-level materializers ([`756192f`](https://github.com/humansoftware/synaflow/commit/756192f09b0ed8b69b48e06cee33a01c19b97b54))

* test: add async error presets, include() precedence, and serializer/helper unit tests ([`b5e1088`](https://github.com/humansoftware/synaflow/commit/b5e108878869f86d82e65a35e39f905fd4bc893a))

### Unknown

* Merge pull request #14 from humansoftware/feature/materializers-ergonomics

Materializer Ergonomics and Standard Library ([`abdf950`](https://github.com/humansoftware/synaflow/commit/abdf9504c9b333c82815263e556d5ddb8e78e4da))

* Implement materializer ergonomics improvements and standard library ([`396a702`](https://github.com/humansoftware/synaflow/commit/396a702f9b3b9d7121b16c9e3977f91081228c2d))

* Merge pull request #12 from humansoftware/review/dag-snapshots-observer-contracts

[codex] strengthen DAG snapshots and observer contracts ([`f4884de`](https://github.com/humansoftware/synaflow/commit/f4884de0bb626e42b7adf50c23693420da0d12d0))

* update docs for step mode and runtime contracts ([`f3f9f17`](https://github.com/humansoftware/synaflow/commit/f3f9f17b6a7ec0fbb238d4fc5c6ff285757ba1d5))

* strengthen dag snapshots and observer contracts ([`710d729`](https://github.com/humansoftware/synaflow/commit/710d729734e92cf986b01dfd0ebaeb24fbc05bd0))

* Merge pull request #11 from humansoftware/review/more-robustness-tests

[codex] strengthen mode resolution and runtime robustness ([`52f3571`](https://github.com/humansoftware/synaflow/commit/52f3571bc04747374edf53fcf522db5d24738b1d))

* align runtime error handling and output inference contracts ([`1011364`](https://github.com/humansoftware/synaflow/commit/1011364c67caf4b2e2d6780356a976d7519ef99a))

* add step mode coverage and robustness tests ([`03a70a1`](https://github.com/humansoftware/synaflow/commit/03a70a1505d0260a4db44c8e114d24d315fa7f46))

* Merge pull request #10 from humansoftware/review/test-contract-gaps

[codex] add failing contract tests for materialization semantics ([`209ef93`](https://github.com/humansoftware/synaflow/commit/209ef93ee65e6edaec97710f12309edb00ab80be))

* fix materialization and error handling semantics ([`3b66eaf`](https://github.com/humansoftware/synaflow/commit/3b66eaf5a826f15598359e20576af3f19cf0003f))

* add failing contract tests for materialization semantics ([`f8fcc33`](https://github.com/humansoftware/synaflow/commit/f8fcc33fec5499dce9129ed8894ac2aff9bd1721))

* Merge pull request #9 from humansoftware/docs/update-design-philosophy

docs: fix design philosophy inconsistencies ([`26a782f`](https://github.com/humansoftware/synaflow/commit/26a782f2b90d21b2dcf5f2ae1fcab39b5c88481b))

* Merge pull request #8 from humansoftware/chore/cleanup-dump-script

chore: remove debug dump script ([`c4919e2`](https://github.com/humansoftware/synaflow/commit/c4919e2cf57d21f52112f9822cd12c4ef857fc8f))


## v0.9.0 (2026-06-13)

### Chore

* chore: remove dump_json_dags.py debug script ([`4fc56c9`](https://github.com/humansoftware/synaflow/commit/4fc56c92f883b51652077b93c7a0e3ab607f06af))

### Ci

* ci: add ruff F401 check for unused imports in pre-commit and CI

- Add ruff to pre-commit with --fix --select F401 (auto-remove)
- Add ruff check to CI workflow (verify only)
- Remove unused imports found by ruff across codebase ([`c2ea5d2`](https://github.com/humansoftware/synaflow/commit/c2ea5d2679378639ca794590ed477d7fd5442c14))

### Feature

* feat: add name to Dag, make _dag public, use dag.name in runtime

- Add name field to Dag dataclass, set during build_dag
- Make _dag public (dag) on PipelineDef
- Include name in DAG JSON output
- Runtime components use dag.name instead of pipeline.name
- Update all corpus json_dag with name field ([`7d782c0`](https://github.com/humansoftware/synaflow/commit/7d782c096721606357312fff9f0a5b9b6da6f2b6))

### Fix

* fix: ruff fixes (format + unused imports) ([`26b5e30`](https://github.com/humansoftware/synaflow/commit/26b5e30fa281fb592c82debd8d6c1c1278fbaff5))

* fix: use ruff-check hook id instead of legacy alias ([`5230d36`](https://github.com/humansoftware/synaflow/commit/5230d3681740bd88f0310adcc6a5dc65bb641a40))

* fix: align ruff version to 0.15.17 in pre-commit and dev deps ([`6d86afc`](https://github.com/humansoftware/synaflow/commit/6d86afc9405b6bfa605fac716eccbd246b802006))

* fix: ruff format test_parity.py ([`39888f4`](https://github.com/humansoftware/synaflow/commit/39888f4fa0c73da30c438f164bedfc46622b948a))

* fix: remove redundant ruff-format (black already covers formatting) ([`e67a6a3`](https://github.com/humansoftware/synaflow/commit/e67a6a365a37e8a6ebfe0ebd1dabe0494b4db1a2))

### Refactor

* refactor: replace black+isort with ruff format+ruff check

- Replace black and isort hooks with ruff-format and ruff
- Update CI to use ruff format --check and ruff check
- Add ruff to dev dependencies ([`b840f56`](https://github.com/humansoftware/synaflow/commit/b840f56ddba75b3d18d3ffda3d867d49c6675ba6))

* refactor: rewrite executors, add dag methods, update docs

- Merge all sync/async engine modules into single executor.py each
- Remove Sync/AsyncStreamManager, NodeRunner, DependencyResolver classes
- Remove TeeWrapper/AsyncTeeWrapper, InterleavedIterator, materializer stubs
- Add step_output_observers for test injection
- PipelineStopException with step_name + cause + raise from
- Composite key fan-out, zip multi-stream unroll with None padding
- each_inputs method on Dag
- Move pipeline_pack.py to tests/common/
- Delete dump_packs.py, stale files
- Update DESIGN_PHILOSOPHY.md, ROADMAP.md, AGENTS.md ([`5a202d1`](https://github.com/humansoftware/synaflow/commit/5a202d1bc33f2f030019659e82899afd960d3381))

* refactor: replace SyncStreamManager with pure functions in stream_routing.py

- Move topology.py to stream_routing.py with clean functions: handle_step_output, apply_materializer, resolve_dependency
- Remove SyncStreamManager class - no state needed
- Remove coerce_value_for_consumer - materializer should produce right type directly
- Move consumers_of to Dag with unit test
- Simplify SyncNodeRunner and PipelineExecutor - no stream_manager dependency
- Remove set/tuple coercion tests (tested wrong contract) ([`69c45c6`](https://github.com/humansoftware/synaflow/commit/69c45c622c2c17747d6ad0cb21b7cd70e2e1764a))

### Unknown

* Merge pull request #7 from humansoftware/refactor/executor_di

refactor: executor rewrites, dag model improvements, materializer architecture ([`cbd5c8b`](https://github.com/humansoftware/synaflow/commit/cbd5c8b7693509cac6c684794e8a595c92b6dc82))


## v0.8.0 (2026-06-13)

### Documentation

* docs: update design philosophy with stream processing analogies, materializer architecture, and type protocol design decisions ([`a7a3cd6`](https://github.com/humansoftware/synaflow/commit/a7a3cd6e93c1f850b1e020e0bdee12296d2c1b4f))

### Feature

* feat: add error_materializer_factory to DAG JSON, use logging, tighten TypeError handling

- Add error_materializer_factory to Dag dataclass and to_dict()
- Wire error_materializer_factory from PipelineDef through build_dag to Dag
- Use logging.warning instead of print in default error materializer
- Tighten except TypeError in default_materializer_factory (per-candidate) ([`37bc707`](https://github.com/humansoftware/synaflow/commit/37bc707475c0b3404b2b05d4e471c09b384c4e15))

* feat: add ErrorMaterializeContext and default error materializer factory

- Add ErrorMaterializeContext dataclass (pipeline_name, dataset_name, exception_type)
- Add default_error_materializer_factory (prints class, message, stack trace)
- Add default_error_materializer_factory to PipelineDef
- Add tests for factory and pipeline default ([`cc182d5`](https://github.com/humansoftware/synaflow/commit/cc182d5b133e09d75d44a7180515a808ad330f31))

* feat: add force_materialize flag to Step and DagNode

- Add force_materialize: bool = False to Step dataclass
- Add force_materialize to DagNode, wired from Step during compilation
- _compute_materialized_deps honors force_materialize for all deps
- Add test for force_materialize behavior ([`f4e54d1`](https://github.com/humansoftware/synaflow/commit/f4e54d148cfe0e714f6b95f75822cccd62a97503))

* feat: default materializer factory returns identity for scalar consumer types

- Add _identity pass-through function
- default_materializer_factory returns _identity when consumer_type is scalar
- Fallback remains list for None/unknown consumer types
- Add tests for scalar identity and None fallback ([`0b63c0e`](https://github.com/humansoftware/synaflow/commit/0b63c0ecc1f6e6888f51c535784ceee120352d91))

* feat: add cycle detection, strict typing to macro expansion, and deep nested sub-pipeline examples ([`96a770c`](https://github.com/humansoftware/synaflow/commit/96a770c95ad611ff0fef841ca6b9eb4814e835e0))

* feat: introduce PipelinePack for unified corpus testing ([`4f9983e`](https://github.com/humansoftware/synaflow/commit/4f9983e43cbae9b51d1b7205fa94fa37462b7770))

### Fix

* fix: make IncludeStep.pipeline required (no None default) ([`f9e36cc`](https://github.com/humansoftware/synaflow/commit/f9e36cc4c280d657f4ae922a04672711f801cb85))

### Refactor

* refactor: merge pipeline.py and step.py into definition.py

- Move PipelineDef, Step, IncludeStep, BaseStep into core/definition.py
- Add lazy import helpers for default factory to avoid circular imports
- Remove core/pipeline.py and core/step.py
- Update all imports across codebase ([`ca9d86e`](https://github.com/humansoftware/synaflow/commit/ca9d86e9f78b5667040f1dc5affbb2b9be4f18c5))

* refactor: convert validators to plain functions, separate params from steps in Dag, materializer never None

- Convert DagBuilder, StepValidator, DependencyValidator, TopologyValidator to module functions
- Add params dict to Dag, separate from steps dict (params no longer in DAG nodes)
- Remove needs_materialize from JSON serialization (runtime detail)
- materializer never None in JSON - all steps have default_materializer_factory
- pipeline field always set to pipeline name (never None)
- Validate __ is forbidden in user step names (reserved for sub-pipeline names)
- Update to_dict() format: {params, steps} instead of flat dict
- Update all 14 corpus json_dag + expected_execution_levels
- Add coding and testing standards document (CODING_AND_TESTING_STANDARDS.md) ([`4a6e364`](https://github.com/humansoftware/synaflow/commit/4a6e364fd1913c6e3d53613d499d4532634480ec))

* refactor: extract Dag/DagNode dataclasses, rename validation to dag_builder, add materializer/materialized_deps to DAG

- Add Dag/DagNode dataclasses with dict-like access (core/dag.py)
- Rename PipelineValidator to DagBuilder, move validation modules to dag_*.py
- Pre-compute materializer per node in DagBuilder
- Add materialized_deps to consumer nodes (replacing needs_materialize on producer)
- Add default_materializer_factory (never None on pipeline)
- Simplify SyncStreamManager/AsyncStreamManager.apply_materializer
- Enforce no-silent-wrapping: scalar producer cannot feed iterable consumer
- Add MaterializeContext.consumer_type
- Remove context dependency from SyncStreamManager
- Rename test files to match new module names
- Add unit tests for DagBuilder (compatibility table, materializer resolution, materialized_deps)
- Add xfail tests for future features
- Update all corpus json_dag with materializer and materialized_deps ([`874401a`](https://github.com/humansoftware/synaflow/commit/874401af2043741775ddb80887da5ea4922fd8a8))

### Test

* test: add untracked test init files and json dump script ([`daa272f`](https://github.com/humansoftware/synaflow/commit/daa272f2762206230ddd3476bf3f9690cd6bce6b))

* test: dynamically generate corpus pack dict keys based on module names ([`9416db9`](https://github.com/humansoftware/synaflow/commit/9416db972e11c99dd1157b688647fa233d84a81a))

* test: statically define test pack names as tuples for PyCharm IDE test runner parsing ([`f3d46d3`](https://github.com/humansoftware/synaflow/commit/f3d46d3059259d8629bedb9817ce0e4e5d1cface))

* test: change parametrize to only use pack_name to fix PyCharm IDE test runner parsing ([`99f7377`](https://github.com/humansoftware/synaflow/commit/99f7377f12a6cb93cb8f6d79a310b07fc2a90d5a))

* test: provide explicit ids to pytest.mark.parametrize to fix IDE test runner resolving ([`2ac3665`](https://github.com/humansoftware/synaflow/commit/2ac3665111b950b6200173fc436de175e95b0af9))

* test: inject json_dag in pipeline packs and assert against pipeline.to_dict() ([`218dc72`](https://github.com/humansoftware/synaflow/commit/218dc721bc7d887e264c66348f739d612a0ec517))

* test: rename expected_results to step_results in PipelinePack and all dependent corpus files ([`a529f14`](https://github.com/humansoftware/synaflow/commit/a529f14208e6d0a525174f408a36699a7ba56d4e))

* test: populate expected_results with actual expected sequence values instead of None across all pipeline packs ([`82839fd`](https://github.com/humansoftware/synaflow/commit/82839fdc7dbd49740946322bca2dce6e57967c84))

* test: implement test recorders to capture and assert lazy generator output correctly ([`d54ee1a`](https://github.com/humansoftware/synaflow/commit/d54ee1a6771b3698306c39289120fbc1a76ecb62))

* test: add expected_execution_levels and exception_match handling to PipelinePack iterations ([`3e0b8f9`](https://github.com/humansoftware/synaflow/commit/3e0b8f9bc9babd3757a19eefe0c83e4f1ead25a4))

### Unknown

* Merge pull request #6 from humansoftware/feature/pipeline_pack

feat: introduce PipelinePack for unified corpus testing ([`50a422c`](https://github.com/humansoftware/synaflow/commit/50a422c483bcb4e1ebd23d8befd64a433debe27a))

* merge: resolve conflicts with remote, port PyCharm IDE fix to test_runner_step_results ([`5627ae2`](https://github.com/humansoftware/synaflow/commit/5627ae208faed9ee11b1f163c866e8d0f748dc5d))


## v0.7.0 (2026-06-12)

### Documentation

* docs: document architectural parity between validation and execution engines ([`6b871ab`](https://github.com/humansoftware/synaflow/commit/6b871abcacb29891ff8518411159647c0ae1f7cf))

### Feature

* feat: implement sub-pipeline macro expansion with Each/All support ([`bf90dde`](https://github.com/humansoftware/synaflow/commit/bf90ddee26400bd208a10b552b3503fb2c8697c4))

### Refactor

* refactor: remove TYPE_CHECKING from step.py and use string annotation ([`0c261b5`](https://github.com/humansoftware/synaflow/commit/0c261b54623028d9180cb55990d5b3dabc368bf4))

* refactor: address PR comments (BaseStep, IncludeStep typing, parent_pipeline tracking, on_error scope) ([`0294069`](https://github.com/humansoftware/synaflow/commit/0294069e7263e0fae37ab9c064317b4a05360d8b))

* refactor: simplify MacroExpander into top-level functions ([`a4c3924`](https://github.com/humansoftware/synaflow/commit/a4c39245146ad8d411c027a593eecf93d76b4984))

* refactor: break sync executor into symmetric runners and resolvers ([`4efbb69`](https://github.com/humansoftware/synaflow/commit/4efbb693b00ed6b868b0aeb764941aa00cb9bb16))

* refactor: break async executor into dependency, topology, and step runners ([`3c926fc`](https://github.com/humansoftware/synaflow/commit/3c926fcd2b160be3d4eb6cd29f05879e3fe692f2))

* refactor: break validator.py into symmetric core validation modules ([`f8d71ac`](https://github.com/humansoftware/synaflow/commit/f8d71acacb56dde7d10428e638f65bb63922fa9b))

### Test

* test: translate sub_pipelines corpus and tests to English ([`47742fe`](https://github.com/humansoftware/synaflow/commit/47742fe1ab6844c1de1929abc347cb006e43fd8b))

* test: assert that directories exist in parity test ([`b623e93`](https://github.com/humansoftware/synaflow/commit/b623e93b90efa0448f7d3a0a538e76f5e4e2be5c))

* test: add async runner test, fix parity, add corpus, and support sub_pipeline serialization ([`83a8928`](https://github.com/humansoftware/synaflow/commit/83a892851fee619a60e55d39f03056aef5bf4c1b))

### Unknown

* Merge pull request #5 from humansoftware/feature/sub_pipelines

feat: Sub-Pipelines (Macro Expansion) ([`c07a5e6`](https://github.com/humansoftware/synaflow/commit/c07a5e6888fc39b25726165e74ff7eb22052c538))

* Merge pull request #4 from humansoftware/feature/executor_refactoring

refactor: symmetric breakdown of validation and execution engines ([`f69b169`](https://github.com/humansoftware/synaflow/commit/f69b169b3abc4614e78e65f6e395655ab5504f34))


## v0.6.0 (2026-06-12)

### Documentation

* docs: add SOLID principles explanation to design philosophy ([`6051595`](https://github.com/humansoftware/synaflow/commit/60515958cd331546b4d0eeced37e791f77200cd7))

* docs: update README with name explanation and philosophy link ([`faaebfe`](https://github.com/humansoftware/synaflow/commit/faaebfe35f62588e68ad3196eaa81972ad5bf876))

* docs: update design philosophy with DAG orchestrator agnosticism ([`c8cc320`](https://github.com/humansoftware/synaflow/commit/c8cc320f9f7368d77bf5527d91fd0b9482c33245))

* docs: update design philosophy with Core Mission and Event-Based rationale ([`98c1cc1`](https://github.com/humansoftware/synaflow/commit/98c1cc1ccbdd99481d6976b51a2ef5050cd5c9bd))

* docs: add design philosophy document ([`a91986f`](https://github.com/humansoftware/synaflow/commit/a91986fc966580393ca8f36b96d93d1970d286cf))

### Feature

* feat: validate materializer compatibility and fix missing context import ([`7fe46ab`](https://github.com/humansoftware/synaflow/commit/7fe46ab43304d85bf2a33dd856ae21578c3f90f3))

* feat: implement Materializer protocol and rewrite materialization tests ([`e40005c`](https://github.com/humansoftware/synaflow/commit/e40005c6c7b7fb98cf9d49a6e1ed3827fd4a7754))

* feat: add MaterializeContext and MaterializerFactory to types ([`881bf10`](https://github.com/humansoftware/synaflow/commit/881bf1009b2448f8697152222a093cf9db9d284b))

### Fix

* fix: isolate framework exceptions from on_error swallowing using StepExecutionError ([`81d9341`](https://github.com/humansoftware/synaflow/commit/81d9341cbca9621d674b8001d422ab2389f8c585))

### Refactor

* refactor: restructure modules into core, sync_engine, and async_engine ([`159e479`](https://github.com/humansoftware/synaflow/commit/159e47958f3472e3a920f7d54dae2ba870786f91))

### Test

* test: add coverage for step-level overrides and factory context injection ([`81d1996`](https://github.com/humansoftware/synaflow/commit/81d19969057a9fb9373d7abfb67c5cc1e84d5499))

### Unknown

* Merge pull request #3 from humansoftware/feature/decorators_and_materializers

feat: Support Custom Materializers and Update Design Philosophy ([`7d53762`](https://github.com/humansoftware/synaflow/commit/7d537629022266a811afb5de9db9cb5134c66c39))

* types: move materializer types to sync and async engines and simplify unions ([`8f1d31c`](https://github.com/humansoftware/synaflow/commit/8f1d31c6a6881cde0abdc0780b1740574d89eb09))

* types: extract materializer types into dedicated synaflow/core/materializer.py module ([`dbf924a`](https://github.com/humansoftware/synaflow/commit/dbf924a9a61e0efc8d8904892f97bc0376e553dc))

* types: update Materializer type hint to support AsyncIterator and Awaitable ([`5d57ef4`](https://github.com/humansoftware/synaflow/commit/5d57ef45fcd686c7d625ece4400c0d2c5bd6a82c))


## v0.5.1 (2026-06-12)

### Chore

* chore: remove __pycache__ and add to gitignore ([`62810dc`](https://github.com/humansoftware/synaflow/commit/62810dce66e841ec59c2323571acb970d071c9a4))

* chore: sync uv.lock version bump ([`1900d63`](https://github.com/humansoftware/synaflow/commit/1900d63efba19d4c4311963d2f14873399fd8877))

* chore: remove throwaway automation scripts ([`27c1579`](https://github.com/humansoftware/synaflow/commit/27c1579594990caace19c010eed2d3bf0c1d4774))

* chore: fix isort and black conflict and format files ([`ba72755`](https://github.com/humansoftware/synaflow/commit/ba7275597b91bd02cc87e9b770ad342ae35dcba2))

### Documentation

* docs: append sub-pipelines to roadmap ([`fbd8f09`](https://github.com/humansoftware/synaflow/commit/fbd8f095c3d97fb03a728bcb5012e13771f583d4))

* docs: add project roadmap ([`6025b45`](https://github.com/humansoftware/synaflow/commit/6025b45cc92e56fad4478bd2c70a7a16a7132ece))

### Fix

* fix: change default OnError to CONTINUE ([`9c59894`](https://github.com/humansoftware/synaflow/commit/9c598942e0db0b834ca364607085b19d36920e61))

* fix: force materialization for steps using OnError.STOP ([`5df1c4b`](https://github.com/humansoftware/synaflow/commit/5df1c4beedaf973d7017139982dac6c340303033))

### Test

* test: add param injection tests for intermediate steps ([`2637aac`](https://github.com/humansoftware/synaflow/commit/2637aac2f756c046624ed22155a53fdc8ac4796e))

* test: improve OnError.STOP validation test ([`9ce3532`](https://github.com/humansoftware/synaflow/commit/9ce3532d0b1d8cef131e8e01009803c71bfcddf8))

* test: add downstream verification for OnError.CONTINUE ([`c57dbb3`](https://github.com/humansoftware/synaflow/commit/c57dbb3759506aa07dad9c8c4c99381e7dda8c61))

* test: set asyncio_mode auto and remove manual async markers ([`e44b6ca`](https://github.com/humansoftware/synaflow/commit/e44b6caa5ecf577ed7550cdf6fe8ecf32a112de0))

* test: add parity test and gitignore ([`4291756`](https://github.com/humansoftware/synaflow/commit/4291756b6d3ef3b1b2aec64cc9126a06f97c11f6))

### Unknown

* Merge pull request #2 from humansoftware/feature/roadmap

fix: Materialization bug on OnError.STOP ([`0b38bb7`](https://github.com/humansoftware/synaflow/commit/0b38bb775530145c544f7641730bfdb43ee31133))

* Merge pull request #1 from humansoftware/feature/roadmap

docs: add project roadmap ([`3d3f3a4`](https://github.com/humansoftware/synaflow/commit/3d3f3a4a38081cc8ecf017b18cbf71e710dd6259))


## v0.5.0 (2026-06-12)

### Feature

* feat: canonical stream json and runner guard tests ([`5f4828b`](https://github.com/humansoftware/synaflow/commit/5f4828b7cec7ad70142bd3ba56f29598d1b2b84b))

### Test

* test: add validation test for mixed sync/async pipelines ([`615c98b`](https://github.com/humansoftware/synaflow/commit/615c98bcbcc86202f78f5e8838f15d9d8782482f))


## v0.4.1 (2026-06-12)

### Fix

* fix: async materialization, queue fallbacks, and error propagation ([`5b2f249`](https://github.com/humansoftware/synaflow/commit/5b2f249f09e088438db73c5059d27bb1f23bc44e))

### Test

* test: rename sync/async to test_sync/test_async for static imports ([`e3f396b`](https://github.com/humansoftware/synaflow/commit/e3f396b9fe78d85e14fca8058e16995a57ba8456))

* test: move corpus to tests/sync/corpus and tests/async/corpus ([`44843fb`](https://github.com/humansoftware/synaflow/commit/44843fba5c105dd149c8c1d8c5906ca7e79cb848))

* test: split corpus into sync_topologies and async_topologies ([`a575f06`](https://github.com/humansoftware/synaflow/commit/a575f06dd56bd95801cec933f0e1d3415f36cea7))


## v0.4.0 (2026-06-12)

### Feature

* feat: enforce strict sync/async pipeline color boundaries during validation ([`bd5ef62`](https://github.com/humansoftware/synaflow/commit/bd5ef62654d51f280ba17eec4757efb02fb9af3e))


## v0.3.0 (2026-06-12)

### Documentation

* docs: clarify trade-offs between corpus and unit tests in HACKING.md ([`ea002d6`](https://github.com/humansoftware/synaflow/commit/ea002d608e0eefb8d36c3b98cd6001595031f82c))

* docs: add HACKING.md with contribution guidelines and architectural principles ([`6b1fb0d`](https://github.com/humansoftware/synaflow/commit/6b1fb0d8af5d2e95b56344d1eb86bea1c67e4f4c))

### Feature

* feat: implement AsyncPipelineExecutor with async streaming via asyncio.Queue ([`8103255`](https://github.com/humansoftware/synaflow/commit/810325519a7c800dd4473618b2ccbe44e314f3b5))

### Refactor

* refactor: replace magic strings with Enums and classes in executor ([`70bfb5f`](https://github.com/humansoftware/synaflow/commit/70bfb5fe41c6a3fcc5ce51010bb23dede6579750))

### Test

* test: refactor all runner tests to use parameterized fixture and order-independent contract assertions ([`347da87`](https://github.com/humansoftware/synaflow/commit/347da87460f7f07dbf6eb83d85efc72d9e2fcfc2))


## v0.2.0 (2026-06-11)

### Chore

* chore: apply pre-commit formatting ([`58da73e`](https://github.com/humansoftware/synaflow/commit/58da73eb7bb8365eba53e3495733d89fc9ac00ce))

### Documentation

* docs: Add Execution Semantics and Custom Runners section ([`acce236`](https://github.com/humansoftware/synaflow/commit/acce2363aa00db4edee77a10141e3b507f3feeac))

* docs: Add DAG JSON to README and rename fixtures to corpus ([`c078184`](https://github.com/humansoftware/synaflow/commit/c0781841ce57c8513ebeaf31d6b121ee8bfa5533))

### Feature

* feat: add fibonacci streaming generator to corpus ([`bb3624a`](https://github.com/humansoftware/synaflow/commit/bb3624a88c4a1b9dbcd354b3c36a35cd4c0f7fd4))

* feat: decouple topological sort into PipelineDef and expand corpus ([`a98980d`](https://github.com/humansoftware/synaflow/commit/a98980dea1093e85df02bc99a6710702a6c1f151))


## v0.1.0 (2026-06-11)

### Documentation

* docs: Add MIT License ([`5bbed31`](https://github.com/humansoftware/synaflow/commit/5bbed317dbfdee421e2c29630b5de029dbd652af))

* docs: Add framework comparisons to README ([`d8f2c53`](https://github.com/humansoftware/synaflow/commit/d8f2c53e3440ee470760e3276c0f8c894a63cba3))

### Feature

* feat: Add CI/CD workflows for testing and semantic release ([`30f4218`](https://github.com/humansoftware/synaflow/commit/30f4218b2b265c39ce56f17b33572c24b928ab4f))

### Fix

* fix: remove build_command from semantic release ([`3232a3e`](https://github.com/humansoftware/synaflow/commit/3232a3e5d33621803955ed341ee5bd3a29c10d8c))

* fix: bump python-semantic-release to v9.8.1 to fix debian repository error ([`e691d1c`](https://github.com/humansoftware/synaflow/commit/e691d1cc19905a6d81f0634911540ddc337fd97c))

### Unknown

* Add pre-commit, improve README, update org in pyproject ([`6c8fbe8`](https://github.com/humansoftware/synaflow/commit/6c8fbe884d0f7f812980f554cbe22a58056faada))

* Initial commit for SynaFlow open source release ([`1cd3c94`](https://github.com/humansoftware/synaflow/commit/1cd3c94f77314898ebb5834e5e2f5d1e8780c224))
