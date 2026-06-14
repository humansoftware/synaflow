# CHANGELOG



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
