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

## 3. Architectural Decisions and Patterns (Decision Log)

### 3.1. Transparent Parameter Injection
**Decision:** Parameters (`params`) defined as a `NamedTuple` are made globally and transparently available to any step in the chain, not just the first node of the pipeline.
**Reason:** Reduces "boilerplate" when passing parameters through the flow. The executor merges the `NamedTuple` keys with upstream node outputs, allowing intermediate steps to directly request these parameters in their signature.

### 3.2. The `OnError.STOP` Rule and Forced Materialization
**Decision:** When a node is configured with `OnError.STOP`, all downstream consumers have their `needs_materialize` forced to `True`. The producer's output is fully materialized before any consumer begins execution.
**Reason:** Pipeline transactional integrity. If processing stops midway due to an error and propagation is lazy, the downstream node would receive garbage or a fraction of the collection. Additionally, if the materializer persists to disk/database, the processed data must be saved before the error halts the pipeline so it can be inspected.

### 3.3. Protocol Separation: Materializers vs. Materializer Factories
**Decision:** The responsibility for persistence and collection buffering was separated into two semantic layers:
- **Materializer (Execution Protocol):** A simple `Callable[[Iterator], Iterable]`. Native language functions like `list`, `set`, and `dict` natively fit here.
- **Materializer Factory (Configuration Protocol):** A `Callable[[MaterializeContext], Materializer]`. It bridges the DAG intelligence and the executor, receiving a rich Context (pipeline name, dataset name, producer type, consumer types) and returning the configured `Materializer`.
**Reason:** Follows the *Simple Things Easy* principle (users can override with `materializer=list` on a step) while maintaining *Complex Things Possible* (users define a Factory with self-discovered file naming via the `Context` in the root `pipeline` constructor).

### 3.4. Materializer Resolution at Build Time
**Decision:** The materializer for every DAG node is pre-computed during DAG construction (build time). Resolution order: step-level `materializer` → pipeline-level `default_materializer_factory` → global default factory. A materializer is **never None** in the serialized DAG. The DAG builder raises a `ValidationError` if no compatible materializer can be resolved (e.g., for custom types without an explicit factory).
**Reason:** Runtime should not be responsible for fallback resolution or type checking — that is a build-time concern. The builder stores the resolved factory; the runtime only handles the factory-with-context call pattern when needed.

### 3.5. `materialized_deps` Belongs to the Consumer
**Decision:** `materialized_deps` is a property of the **consumer**, listing which of its inputs must be fully materialized before the node can execute. The DAG builder computes this from consumer type annotations (`list`, `set`, `tuple`, `dict`), `on_error=STOP` propagation, and an explicit `force_materialize` flag on the step.
**Reason:** The consumer knows what it needs. The producer doesn't know (or care) who will consume its output. The legacy `needs_materialize` flag on the producer is a cached convenience for runtime and is not part of the JSON serialization.

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

**Invocation rules:** The materializer is invoked when (a) the consumer demands a materialized protocol (`list`/`set`/`tuple`/`dict`), OR (b) the producer has `on_error=STOP`, OR (c) the step has `force_materialize=True`. For fan-out scenarios, `tee` splits the stream before materialization, so lazy consumers receive the original stream while materialized consumers receive the materialized copy.

### 3.10. `force_materialize` Flag
**Decision:** Steps can explicitly declare `force_materialize=True` to trigger materialization of their output regardless of consumer types or error handling configuration.
**Reason:** Some use cases require materialization as a side effect (e.g., persisting intermediate results for debugging, caching expensive computations, or ensuring data is written to an audit log at a specific pipeline stage). This is orthogonal to `on_error=STOP`.

### 3.11. No Silent Type Wrapping
**Decision:** The framework never silently coerces scalar values into iterables. If a producer outputs `str` and a consumer expects `Iterator[str]`, a `ValidationError` is raised at build time. Users must explicitly declare `Iterator[str]` as the output type and `yield` the single item.
**Reason:** Implicit wrapping hides design errors and breaks the type contract. Explicit yield makes the data flow visible and predictable.

### 3.12. Inline Executors (Single-File Runtime)
**Decision:** The sync and async execution engines each live in a single file (`executor.py`). The previous sub-components (`SyncStreamManager`, `SyncNodeRunner`, `SyncDependencyResolver`, and their async counterparts) were stateless classes that existed only as namespaces. They were replaced by plain functions and inlined into the executor.
**Reason:** Simpler dependency graph, no fake "classes" without state, easier to understand the full execution flow in one file.

### 3.13. Observable Execution (`step_output_observers`)
**Decision:** The executor accepts an optional list of observer callbacks via `step_output_observers`. Each observer is called with `(step_name, output)` every time a step produces an output. For stream outputs, the sync executor tees the stream so the observer receives an independent copy; the async executor creates a dedicated pump task.
**Reason:** Enables test infrastructure (capturing step outputs for spec compliance tests) without modifying production logic. Follows the Observer pattern — the executor doesn't know what observers do, only that they exist.

### 3.14. PipelineStopException with Context
**Decision:** `PipelineStopException` carries `step_name` and `cause` (the original exception). It uses `raise ... from` to preserve the full stack trace.
**Reason:** When a pipeline stops, callers need to know which step caused the stop and why. An empty exception is useless for debugging.

---

*(This document should be iteratively evolved whenever a new architectural contract is established in the Synaflow codebase).*
