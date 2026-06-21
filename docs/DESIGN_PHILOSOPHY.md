# Synaflow: Design Philosophy and Architectural Decisions

This document records the fundamental principles and architectural design decisions of Synaflow. It serves as the definitive guide for the framework's evolution, ensuring that new features respect the original vision.

## 1. Fundamental Principles

### 1.1. Core Mission: Clean, Decoupled Business Rules
The primary problem Synaflow solves is isolating business logic from architectural complexity. The final code containing business rules must be impeccably clean, readable, and completely decoupled from how data is orchestrated, materialized, or distributed. The framework handles the heavy lifting of orchestration so developers can focus solely on the domain logic.

### 1.2. Make Simple Things Easy, Complex Things Possible
The learning curve of the framework should be friendly. Default configurations should be intuitive and seamlessly handle 90% of use cases (e.g., using `list` or `set` as native materializers). However, the framework must expose protocols and interfaces (such as context-rich *Factories*) to allow advanced engineering (e.g., partitioned disk persistence, database-backed collections, or LRU-cached proxies).

This philosophy translates into a strict adherence to SOLID principles, specifically:
- **Open/Closed Principle (OCP)**: The framework is open for extension (you can inject your own `MaterializerFactory` or custom execution runners) but closed for modification (you never need to hack the core engines to support new behaviors).
- **Interface Segregation Principle (ISP)**: Users are never forced to implement massive classes or unused interface methods (like `setup()`, `teardown()`, etc.). Simple tasks only require a simple python `def`.

### 1.3. Convention Over Configuration
User code should focus on business rules, not wiring things together.
- The DAG discovers dependencies by reading signature types (Type Hints).
- Global options (e.g., Materializers, Timeouts) are configured once at the `pipeline` root and propagated by convention, rather than requiring the user to configure every node. Exceptions to rules (overrides) are explicit per node.

### 1.4. Lazy by Default (Event-Based & Stream Processing)
The framework assumes *Stream* processing (Lazy Evaluation) as the default. This is specifically designed so that pipelines can be idempotent and utilized for event-based processing.
- When processing events, we often need to re-process past events individually, while at other times it makes more sense to aggregate them into a time window and materialize them all at once. The framework makes both scenarios possible through delayed evaluation.
- The default error handling is `OnError.CONTINUE`, allowing a failing event to be discarded without halting the continuous stream, preserving the RAM and CPU optimization of lazy pipelines.

### 1.5. Orchestrator Agnostic (The DAG JSON)
By compiling the pipeline into a serializable JSON DAG (via `pipeline.to_dict()`), the framework intentionally decouples the pipeline definition from its execution engine (the Runner). This makes it possible for anyone to create a custom runner, or to convert a Synaflow DAG into native pipelines for enterprise orchestrators like **Airflow**, **Dagster**, or **Prefect**.

The serialized DAG is not a lossy debug artifact. It is the externalized execution contract. Decisions resolved at build time — such as step mode and which dependencies run in each-mode — belong in the DAG JSON so alternative runners do not need to re-infer semantics.

### 1.6. Type Safety at Build Time
The DAG builder validates all type compatibility at compile time. Silent type coercion (e.g., wrapping a `str` into a `list[str]`) is forbidden — the user must explicitly declare correct types. If a consumer expects `Iterator[str]` but the producer outputs `str`, a validation error is raised.

## 2. Conceptual Analogies

Synaflow draws inspiration from established data processing paradigms. Understanding these analogies clarifies the framework's design choices.

### 2.1. Java Streams API

| Java Streams | Synaflow |
|---|---|
| `stream.map(f)` | Each mode: consumer `(T) → U` over producer `Iterator[T]` |
| `stream.collect(toList())` | Materializer: consumer `list[T]` triggers `Iterator[T] → list[T]` |
| `stream.collect(toSet())` | Materializer: consumer `set[T]` triggers `Iterator[T] → set[T]` |
| `stream.collect(toMap(k,v))` | Materializer: consumer `dict[K,V]` triggers `Iterator[tuple[K,V]] → dict[K,V]` |
| `stream.forEach(f)` | Each mode sink (no downstream consumers) |
| `stream.iterator()` | Lazy consumer `Iterator[T]` |
| `Collector` | `MaterializerFactory` + returned `Materializer` callable |

**Key difference from Java**: Java Streams are purely in-memory. Synaflow's materializer can target disk, databases, or remote storage — the factory decides whether to return an in-memory `list`, a disk-backed `MutableSequence` proxy, or a database-persisted collection with LRU caching. The consumer only knows the protocol (`list`, `set`, `dict`), not the concrete storage backend.

### 2.2. SQL

SQL can be viewed as a functional language for stream processing over collections. Synaflow mirrors this:
- **`SELECT` / `map`** — Each mode transformation: `(T) → U`
- **`WHERE` / `filter`** — Each mode with conditional yield
- **`GROUP BY`** — `Iterator[tuple[K,V]] → dict[K, V]` via materializer
- **`ORDER BY`** — Producer with `list[T]` materializer (materialized for random access)
- **`JOIN`** — Step with multiple dependencies, optionally materializing the smaller side
- **`UNNEST` / `LATERAL`** — Each mode producing an iterable, with implicit flatten

Like SQL, Synaflow always operates on flat, typed streams — there is no `Iterator[Iterator[T]]` in user-facing code.

### 2.3. MapReduce

| Phase | Synaflow Pattern |
|---|---|
| **Map** | Each mode: `[str] → Iterator[tuple[K,V]]` — emits key-value pairs per item |
| **Shuffle** | Materializer: `Iterator[tuple[K,V]] → dict[K, list[V]]` — groups by key |
| **Reduce** | Each mode over dict items: `[tuple[K, list[V]]] → U` — processes each group |

The materializer can persist the shuffle phase to disk when datasets are too large for memory.

### 2.4. Framework Comparison

| | SynaFlow | Hamilton | Dagster | Prefect | Airflow |
|---|---|---|---|---|---|
| **Type-hint wiring** | ✅ auto | ✅ | ❌ | ❌ | ❌ |
| **Lazy streaming** | ✅ lockstep + bounded handoff | ❌ DataFrame-centric | ❌ task-based | ❌ task-based | ❌ task-based |
| **Smart binding** | ✅ singular/plural/suffix | ❌ | ❌ | ❌ | ❌ |
| **Scope** | In-process micro | Feature engineering | Asset orchestration | Workflow orchestration | DAG scheduling |
| **DAG export** | ✅ JSON | ✅ | ✅ | ✅ | ✅ |
| **Sync/async parity** | ✅ identical | ❌ | ✅ | ✅ | ❌ |
| **Memory model** | One item per step | Full DataFrame | Task I/O boundary | Task I/O boundary | Task I/O boundary |
| **Learning curve** | Low (plain functions) | Medium | High | Medium | High |

SynaFlow fills the gap of **in-process streaming micro-orchestration**. It is not
a replacement for Airflow, Dagster, or Prefect — it runs inside a single Python
process. Use those tools to schedule and trigger jobs; use SynaFlow inside the
job to stream millions of rows between functions with zero boilerplate.

### 2.5. Detailed Comparison: SynaFlow vs Hamilton

Hamilton and SynaFlow are the two Python frameworks that use **function signatures
and type hints** to automatically build DAGs. The similarity ends there — they
serve fundamentally different data models.

#### Declarative model

| | SynaFlow | Hamilton |
|---|---|---|
| **Wiring** | Parameter name matches producer name | Function name becomes output column |
| **Binding** | Smart: `item` → `items`, `user_list` → `users` | Exact: `user` ≠ `users` |
| **DRY** | Natural synonyms, no renaming needed | Must align function names meticulously |
| **Example** | `def transform(item: User)` binds to step `items` | `def transform(items: pd.Series)` — name must match |

#### Data model

| | SynaFlow | Hamilton |
|---|---|---|
| **Default flow** | Lazy streaming (`Iterator[T]`) | DataFrame columns (materialized) |
| **Memory** | One item per step — generators | Entire column in memory |
| **Multiple consumers** | Auto `tee` in lockstep, bounded handoff when configured | Single consumer per column |
| **Materialization** | Consumer-driven: ask for `list[T]` → materialize | Always materialized |
| **Generators** | Native: `yield` in any step | Not supported |
| **Streaming to disk** | Transparent via materializer factories | Manual code in each function |

#### Use cases

| | SynaFlow | Hamilton |
|---|---|---|
| **Best for** | Streaming micro-orchestration, event pipelines | Feature engineering, DataFrame transforms |
| **When to use** | Millions of rows, lazy forks, lockstep consumers | Columnar data, sklearn transforms, notebook-to-production |

#### Other frameworks

**Flyte** and **Metaflow** wire pipelines explicitly (`A >> B >> C`, `self.next()`).
Type hints are used for validation, not DAG construction. **Dask delayed** builds task
graphs lazily but requires explicit task declarations — no auto-wiring from signatures.

None of these support smart binding (singular/plural synonyms), lazy lockstep
streaming with automatic `tee`, bounded `max_in_flight` handoff, or
consumer-driven per-branch materialization.

## 3. Architectural Decisions and Patterns (Decision Log)

### 3.1. Transparent Parameter Injection
**Decision:** Parameters (`params`) defined as a `NamedTuple` are made globally and transparently available to any step in the chain, not just the first node of the pipeline.
**Reason:** Reduces "boilerplate" when passing parameters through the flow. The executor merges the `NamedTuple` keys with upstream node outputs, allowing intermediate steps to directly request these parameters in their signature.

### 3.2. The `OnError.STOP` Rule and Forced Materialization
**Decision:** When a node is configured with `OnError.STOP`, the producer is marked as needing materialization before any downstream consumer begins execution.
**Reason:** Pipeline transactional integrity. If processing stops midway due to an error and propagation is lazy, the downstream node would receive garbage or a fraction of the collection. Additionally, if the materializer persists to disk/database, the processed data must be saved before the error halts the pipeline so it can be inspected.

### 3.3. Protocol Separation: Materializers vs. Materializer Factories
**Decision:** The responsibility for persistence and collection buffering was separated into two semantic layers:
- **Materializer (Execution Protocol):** A simple `Callable[[Iterator], Iterable]`. Native language functions like `list`, `set`, and `dict` natively fit here.
- **Materializer Factory (Configuration Protocol):** A `Callable[[MaterializeContext], Materializer]`. It bridges the DAG intelligence and the executor, receiving a rich Context (pipeline name, dataset name, producer type, consumer types) and returning the configured `Materializer`.
**Reason:** Follows the *Simple Things Easy* principle (users can override with `materializer=list` on a step) while maintaining *Complex Things Possible* (users define a Factory with self-discovered file naming via the `Context` in the root `pipeline` constructor).

### 3.4. Materializer Resolution at Build Time
**Decision:** The materializer for every DAG node is pre-computed during DAG construction (build time). Resolution order: step-level `materializer` → pipeline-level `memory_materializer_factory` → global default factory. A materializer is **never None** in the serialized DAG. The DAG builder raises a `ValidationError` if no compatible materializer can be resolved (e.g., for custom types without an explicit factory).
**Reason:** Runtime should not be responsible for fallback resolution or type checking — that is a build-time concern. The builder stores the resolved factory; the runtime only handles the factory-with-context call pattern when needed.

### 3.5. Producer-Level Materialization Contract
**Decision:** Materialization is compiled at the **producer** level. If any rule forces a producer to materialize, all of its consumers read from that materialized output.
**Reason:** This keeps the runtime simple and prevents executors from re-deriving eager/lazy policy per edge. Consumer-facing materialization details may still exist internally for diagnostics, but they are not the runtime contract and are not part of the public export shape.

### 3.5.1. Step Mode Is a DAG Decision
**Decision:** A step's execution mode is resolved at build time and stored in the DAG as `mode` plus `each_mode_deps`. The user-facing API exposes `StepMode.AUTO`, `StepMode.EACH`, and `StepMode.ALL`.
**Reason:** "Is this step each-mode or all-mode?" is a semantic decision, not a runtime heuristic. The builder validates explicit mode requests and records the resolved answer once. Sync and async executors must then consult the DAG rather than re-infer from annotations or runtime values.

### 3.6. Type Protocols (Not Concrete Types)
**Decision:** Consumer type annotations describe **protocols** (interfaces), not concrete classes. `list[T]` means "I need random access, indexing, and iteration" — not necessarily a Python `list` instance. The materializer factory returns an object satisfying the protocol. The framework detects protocols via `collections.abc`: `Sequence`/`MutableSequence` for list-like, `MutableSet` for set-like, `MutableMapping` for dict-like.
**Reason:** Enables transparent persistence backends. A `DiskBackedList` implementing `MutableSequence` satisfies a `list[T]` consumer without loading the entire dataset into memory. The consumer is unaware of where data lives.

### 3.7. Runtime Type Coercion Removed
**Decision:** The runtime dependency resolver no longer converts types (e.g., `list → set`, scalar → `[scalar]`). Values pass through as-is from context to consumer.
**Reason:** Type safety and predictability. If the materializer produced the wrong type for a consumer, silently fixing it at runtime hides the bug. The build-time materializer should produce the correct type directly. (The legacy `coerce_value_for_consumer` and `adapt_argument_to_consumer_type` functions were removed.)

### 3.8. Architectural Parity (Compile-Time vs Run-Time)
**Decision:** The internal architecture of Synaflow enforces a strict, symmetric separation of concerns across both Compilation (DAG Building) and Execution (Run-Time) engines.
**Reason:** To ensure massive maintainability, single-responsibility principle (SRP), and to prevent the creation of "God Classes" in the executors. Whether the framework is building the DAG, running synchronous callbacks, or orchestrating asynchronous `asyncio.Queue` streams, the exact same domain structure is respected:

| Domain / Concern | Compile-Time (DAG Build) | Run-Time |
| :--- | :--- | :--- |
| **Pipeline/Orchestration** | `build_dag()` | `PipelineExecutor` / `AsyncPipelineExecutor` |
| **Dependencies/Type Resolution** | `validate_and_resolve_dependencies()` | inlined in executor |
| **Topology/Stream Routing** | `check_circular_dependencies()` | inlined in executor |
| **Node Execution** | `validate_and_compile_step()` | inlined in executor |

The practical consequence is strict DAG primacy at runtime:
- the builder resolves `mode` and `each_mode_deps`
- the executors consult the DAG and do not re-infer step mode
- materialization is compiled once in the DAG and executors follow that producer-level contract
- sync and async error handling must preserve the same user-visible contract

### 3.9. Materializer Compatibility Table (Default Factory)

The default materializer factory maps producer-consumer type pairs to appropriate materializer callables:

| Producer type | Consumer type | Default materializer | Invoked when |
|---|---|---|---|
| `T` | `T` | `identity` | `on_error=STOP` or `force_materialize` |
| `Iterator[T]` | `T` | `identity` | `on_error=STOP` or `force_materialize` |
| `Iterator[T]` | `Iterator[T]` | `identity` | `on_error=STOP` or `force_materialize` |
| `Iterator[T]` | `list[T]` | `list` | always |
| `Iterator[T]` | `set[T]` | `set` | always |
| `Iterator[T]` | `tuple[T, ...]` | `tuple` | always |
| `dict[K,V]` | `dict[K,V]` | `identity` | `on_error=STOP` or `force_materialize` |
| `dict[K,V]` | `Iterator[tuple[K,V]]` | `identity` | `on_error=STOP` or `force_materialize` |
| `dict[K,V]` | `list[tuple[K,V]]` | `list` | always |
| `dict[K,V]` | `set[tuple[K,V]]` | `set` | always |
| `Iterator[tuple[K,V]]` | `dict[K,V]` | `dict` | always |
| `Iterator[tuple[K,V]]` | `list[tuple[K,V]]` | `list` | always |
| `Iterator[tuple[K,V]]` | `Iterator[tuple[K,V]]` | `identity` | `on_error=STOP` or `force_materialize` |
| `T` | `Iterable[T]` | — | **validation error** |
| `CustomType` | any | — | **validation error** (requires custom factory) |

**Invocation rules:** The materializer is invoked when (a) the consumer demands a materialized protocol (`list`/`set`/`tuple`/`dict`), OR (b) the producer has `on_error=STOP`, OR (c) the step has `force_materialize=True`. For fan-out scenarios, materialization stays branch-local: lazy consumers keep progressive delivery while materialized consumers receive a collected branch copy.

For uneven multi-stream each-mode, exhaustion is modeled with `None` padding rather than silent truncation. This is part of the execution contract and must behave identically in sync and async runners.

### 3.10. `max_in_flight`
**Decision:** Every compiled `DagNode` carries `max_in_flight`, defaulting to `1`, and runners enforce it from DAG metadata rather than step definitions.
**Reason:** Bounded handoff is part of runtime semantics, not user-code convenience. The contract is "maximum number of items already emitted by a step and not yet delivered to the next consumption stage." Keeping it in the DAG preserves build/run separation, JSON export fidelity, and sync/async parity.
**Operational motivation:** This is primarily for I/O-bound pipelines where one step starts work and the next resolves it, such as request submission followed by response awaiting. It gives the runtime a small, explicit ahead window without changing the programming model into manual queues or semaphores.

### 3.11. `force_materialize` Flag
**Decision:** Steps can explicitly declare `force_materialize=True` to trigger materialization of their output regardless of consumer types or error handling configuration.
**Reason:** Some use cases require materialization as a side effect (e.g., persisting intermediate results for debugging, caching expensive computations, or ensuring data is written to an audit log at a specific pipeline stage). This is orthogonal to `on_error=STOP`.

### 3.12. No Silent Type Wrapping
**Decision:** The framework never silently coerces scalar values into iterables. If a producer outputs `str` and a consumer expects `Iterator[str]`, a `ValidationError` is raised at build time. Users must explicitly declare `Iterator[str]` as the output type and `yield` the single item.
**Reason:** Implicit wrapping hides design errors and breaks the type contract. Explicit yield makes the data flow visible and predictable.

### 3.13. Inline Executors (Single-File Runtime)
**Decision:** The sync and async execution engines each live in a single file (`executor.py`). The previous sub-components (`SyncStreamManager`, `SyncNodeRunner`, `SyncDependencyResolver`, and their async counterparts) were stateless classes that existed only as namespaces. They were replaced by plain functions and inlined into the executor.
**Reason:** Simpler dependency graph, no fake "classes" without state, easier to understand the full execution flow in one file.

### 3.14. Observable Execution (`step_output_observers`)
**Decision:** The executor accepts an optional list of observer callbacks via `step_output_observers`. Each observer is called with `(step_name, output)` every time a step produces an output. For stream outputs, the sync executor tees the stream so the observer receives an independent copy; the async executor creates a dedicated pump task.
**Reason:** Enables test infrastructure (capturing step outputs for spec compliance tests) without modifying production logic. Follows the Observer pattern — the executor doesn't know what observers do, only that they exist.

**Observer contract:** observers see the producer's output exactly once in producer semantics:
- in mixed lazy/eager fan-out, observing the producer must not force all consumers eager
- when a stream fails under `OnError.CONTINUE`, observers see the valid prefix that was already produced
- observer behavior is a public contract and is covered by corpus/spec tests, not only unit tests
- when the output is an `Iterator`/`AsyncIterator`, the observer receives the iterator directly (via `tee`) and **must consume it fully**; an unconsumed iterator causes memory growth (the `tee` buffer retains all items) and the observed data is silently lost. This is application responsibility, not framework responsibility.

### 3.15. PipelineStopException with Context
**Decision:** `PipelineStopException` carries `step_name` and `cause` (the original exception). It uses `raise ... from` to preserve the full stack trace.
**Reason:** When a pipeline stops, callers need to know which step caused the stop and why. An empty exception is useless for debugging.

---

### 3.15. Lifecycle Observer System

**Decision:** A unified observer infrastructure for pipeline, step, and materialization lifecycle events. Observers are registered declaratively on `pipeline(...)` or `step(...)` and are fire-and-forget — their failures are logged and swallowed, never affecting pipeline execution.

**Reason:** Provides a single mental model for logging, metrics, tracing, and alerts. Step-level `observers=[...]` is ergonomic convenience syntax that compiles into the same observer model as pipeline-level registration. Observers observe lifecycle; materializers handle data. The two are strictly separated: observer presence must not force materialization, disable laziness, or alter dataflow semantics.

**Observer scope model:**
- **Step-level observers** (`step(..., observers=[...])`) receive only `StepEvent.*` and `MaterializationEvent.*` for that specific step. They never receive `PipelineEvent.*`.
- **Pipeline-level observers** (`pipeline(..., observers=[...])`) receive `PipelineEvent.*` (pipeline lifecycle) and are also inherited into every compiled step's effective observer list, where they receive `StepEvent.*` and `MaterializationEvent.*` for each step.
- Event filtering is done via wrapper helpers above the core — handlers inspect `ctx.event` to decide whether to act.

**DAG JSON contract:**
- `pipeline_observers` at the DAG root: compiled pipeline-scope observers used for pipeline lifecycle event dispatch.
- `observers` on each step node: effective observers for that step (pipeline-level inherited + step-level own), used for step and materialization event dispatch.
- Both carry `handler_name` and `source` (`"pipeline"` or `"step"`) metadata. Raw callables are never serialized.

**Key rules:**
- Observer registration is declarative only — it does not change execution semantics
- Materialization events only fire when materialization actually occurs per normal runtime rules
- Observer dispatch is fire-and-forget; failures are swallowed after logging
- Observers carry metadata only (step name, mode, counts, exception); never inputs/outputs
- Effective observers are resolved at build time and stored in the DAG
- Sync and async executors provide semantic parity; async handlers are detected via `inspect.isawaitable`

**Scope boundary:** Observers are for lightweight operational side effects (metrics, logs, alerts). They are not for data tapping, item-level inspection, or replacing materializers.

---

### 3.16. Base Dataset Name & Smart Binding

**Decision:** Every step produces exactly one Base Dataset. The Base Dataset name is derived from the step name by normalizing it to its absolute plural form. Function parameters can reference datasets using any natural synonym (singular, plural, or common suffixes like `_list`), and the framework resolves them automatically via **Smart Binding**.

**Reason:** Reduces boilerplate and naming friction. Users writing EACH-mode consumers naturally think in singular terms (`item`) while ALL-mode consumers think in plural terms (`items`). The framework bridges the gap so the developer writes what is semantically correct without worrying about exact name matching. This also prevents accidental collisions (a `user` step and a `users` step in the same pipeline are now detected as conflicting Base Datasets).

**How it works:**
1. `get_base_dataset_name(name)` strips collection suffixes (`_list`, `_set`, `_dict`, `_tuple`) and pluralizes the last word using `inflect`, producing the canonical Base Dataset name (always absolute plural).
2. During dependency resolution, if a parameter name is not found directly in `produced`, the system computes its Base Dataset name and searches for a match among existing producers.
3. Build-time validations prevent:
   - **Duplicate Base Datasets**: two steps whose names map to the same Base Dataset (e.g., `user` and `users`) raise a `ValueError`.
   - **Duplicate Parameters**: a single function with two parameters that map to the same Base Dataset (e.g., `def fn(user, users)`) raises a `ValueError`.
4. The resolved mappings are stored in `DagNode.binding_map` and serialized in the DAG JSON under `binding_map` so external runners can replicate the resolution without re-inferring semantics.

**Naming philosophy:** Step names should focus on **nouns** that describe what data the step produces (e.g., `users`, `transactions`, `logs`). EACH-mode consumers naturally reference the singular form (`user: User`) while ALL-mode consumers reference the plural or a suffixed form (`users_list: list[User]`). The framework handles the mapping transparently — there is no state collision because the Base Dataset name is always unique.

---

*(This document should be iteratively evolved whenever a new architectural contract is established in the Synaflow codebase).*
