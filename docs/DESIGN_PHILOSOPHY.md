# Synaflow: Design Philosophy and Architectural Decisions

This document records the fundamental principles and architectural design decisions of Synaflow. It serves as the definitive guide for the framework's evolution, ensuring that new features respect the original vision.

## 1. Fundamental Principles

### 1.1. Make Simple Things Easy, Complex Things Possible
The learning curve of the framework should be friendly. Default configurations should be intuitive and seamlessly handle 90% of use cases (e.g., using `list` or `set` as native materializers). However, the framework must expose protocols and interfaces (such as context-rich *Factories*) to allow advanced engineering (e.g., partitioned disk persistence by data type).

### 1.2. Convention Over Configuration
User code should focus on business rules, not wiring things together.
- The DAG discovers dependencies by reading signature types (Type Hints).
- Global options (e.g., Materializers, Timeouts) are configured once at the `pipeline` root and propagated by convention, rather than requiring the user to configure every node. Exceptions to rules (overrides) are explicit per node.

### 1.3. Lazy by Default (Stream Processing)
The framework assumes *Stream* processing (Lazy Evaluation) as the default whenever possible, to protect memory (RAM) and optimize CPU time.
- The default error handling is `OnError.CONTINUE`, allowing a failing item to be discarded without halting the continuous stream.

## 2. Architectural Decisions and Patterns (Decision Log)

### 2.1. Transparent Parameter Injection
**Decision:** Parameters (`params`) defined as a `NamedTuple` are made globally and transparently available to any step in the chain, not just the first node of the pipeline.
**Reason:** Reduces "boilerplate" when passing parameters through the flow. The executor (`_resolve_node_arguments`) merges the `NamedTuple` keys with upstream node outputs, allowing intermediate steps to directly request these parameters in their signature.

### 2.2. The `OnError.STOP` Rule and Forced Materialization
**Decision:** When a node is configured with `OnError.STOP`, the framework is forced to break the *Lazy* paradigm for that step, fully materializing the produced data before releasing execution to consumer nodes.
**Reason:** Pipeline transactional integrity. If processing stops midway due to an error and propagation is lazy, the downstream node will receive garbage or a fraction of the collection, corrupting the logical flow of the system and complicating concurrency control and cleanup.

### 2.3. Protocol Separation: Materializers vs. Materializer Factories
**Decision:** The responsibility for persistence and collection buffering was separated into two semantic layers:
- **Materializer (Execution Protocol):** A simple `Callable[[Iterator], Iterable]`. Native language functions like `list`, `set`, and `dict` natively fit here.
- **Materializer Factory (Configuration Protocol):** A `Callable[[MaterializeContext], Materializer]`. It bridges the DAG intelligence and the executor, receiving a rich Context (dataset name, step, pipeline, type hints) and returning the configured `Materializer`.
**Reason:** Follows the *Simple Things Easy* principle (users can override with `materializer=list` on a step) while maintaining *Complex Things Possible* (users define a Factory with self-discovered file naming via the `Context` in the root `pipeline` constructor).

---
*(This document should be iteratively evolved whenever a new architectural contract is established in the Synaflow codebase).*
