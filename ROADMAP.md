# Synaflow Roadmap 🚀

This roadmap outlines the planned features and architectural evolutions for the Synaflow framework, ensuring it remains a powerful, type-driven, and robust engine for DAGs and pipelines.

## 1. Data Type Enhancements
- **Dictionary Support (`dict`)**:
  - Support for `dict` type dependencies to be processed as a whole (`all` mode).
  - Support for streaming/iterating over key-value pairs of a `dict` (`each` mode), relying purely on native Python type hints.
- **Complex Types in Corpus**:
  - Expand `tests/test_async/corpus` and `tests/test_sync/corpus` with examples leveraging complex data structures such as `List[NamedTuple]`, `dataclasses`, and `Pydantic` models.
  - Prove that the engine operates seamlessly with higher-level user models without requiring framework-specific couplings.

## 2. Pluggable Materialization
- **Step-level Materializers**:
  - Introduce the ability to annotate steps (either via the `step()` definition or through decorators) with custom materializers.
  - Enable datasets to be lazily or eagerly persisted to disk, databases, or cloud storage regardless of whether the step is `each` or `all`.

## 3. Advanced Error Handling & Interception
- **Error Processors / Interceptors**:
  - Allow attaching error handlers at the *step level* or the *pipeline level*.
  - Error handlers will act as specialized sinks/steps (supporting `each` or `all`, and materializers), enabling pipelines to stream failed records into error tables, dead-letter queues, or logs.
- **Framework Agnosticism**:
  - Ensure the framework remains completely agnostic to specific data models. Users will define their own canonical `ErrorModel` (e.g., via Dataclasses), and the engine will generically route errors to their custom interceptors.

## 4. Timeouts & Metrics
- **Step Timeouts**:
  - Add native support for configuring maximum execution time (timeouts) per step, gracefully aborting and capturing the timeout exception.
- **Metrics Decorators**:
  - Implement extensible telemetry decorators to process and track successes, failures, and timeouts.
  - Allow users to hook custom observability solutions (Prometheus, Datadog, Logs) into the pipeline lifecycle.

## 5. Documentation & Developer Experience
- **Extensive Documentation**:
  - Write comprehensive user guides, architectural decision records (ADRs), and API references.
  - Automatically publish the documentation to GitHub Pages via CI/CD.

## 6. Sub-Pipelines (Pipelines as Steps)
- **Nested Execution**:
  - Allow entire pipelines to be registered and executed as individual steps within a parent pipeline.
- **Parameter Adapters**:
  - Implement adapters to map inputs and parameters between the parent DAG and the nested child pipeline seamlessly.

---
*Note: All new features will be developed following our rigorous Branch + Pull Request workflow, guaranteeing 100% test parity between Sync and Async implementations, and respecting the `pre-commit` code quality standards.*
