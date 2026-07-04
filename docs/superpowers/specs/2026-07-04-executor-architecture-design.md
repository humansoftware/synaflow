# Executor Architecture Refinement Design

## 1. Context & Motivation
Following the extraction of the massive 1200+ LOC PipelineExecutor into focused sub-modules (sync and async), an analysis of the extracted components revealed structural overlaps and technical debt. Specifically:
- **DAG Mutation:** The runtime was mutating the immutable DAG definitions (`node._runtime_error_count`) to share state between the `Executor` and `StreamPublisher`.
- **Event Duplication:** `Step_completed` and `step_failed` events were being manually emitted in multiple places by both the `Executor` and the `StreamPublisher`.
- **"Maybe Async" Ambiguity:** The async runtime contained defensive `inspect.iscoroutinefunction` checks scattered throughout to handle the possibility of synchronous observers or materializers.
- **Naming Inconsistencies:** Sync modules used private variables (`_dag`), while async modules used public ones (`dag`).

This spec outlines the structural refinements needed before unit tests can be written effectively.

## 2. Refined Components

### 2.1 ArgumentBuilder (formerly DependencyResolver)
- **Role:** Pure argument resolution. "Given a callable's requirements, build the dictionary of kwargs it needs."
- **Scope:** It resolves inputs, resource lifecycles (`ExitStack`), and materializers. It **does not** invoke the callable.
- **Consistency:** Will adopt standard private variable naming (`self._dag`, `self._outputs`) across both sync and async.

### 2.2 StepLifecycle (New)
- **Role:** Centralized state tracking and lifecycle event emission for a single step execution.
- **Scope:**
  - Created by the `Executor` when a step starts.
  - Holds `success_count`, `error_count`, and `completed_all_inputs`.
  - Exposes `record_success()` and `record_error()` methods. The `StreamPublisher` calls these instead of mutating `node._runtime_error_count`.
  - Exposes `start()` and `finish(exception=None)` which internally invoke the `EventDispatcher` to emit the correct `step_started`, `step_completed`, or `step_failed` events.

### 2.3 EventDispatcher
- **Role:** Pure event dispatching.
- **Scope:** Receives pre-built contextual data and notifies all registered observers. It no longer contains logic to execute error materializers.

### 2.4 Error Materialization Flow
- The `Executor` is responsible for handling step errors.
- Flow: `Executor` captures error -> `Executor` uses `ArgumentBuilder` to resolve `error_materializer` args -> `Executor` invokes `error_materializer` -> `Executor` passes the resulting `ErrorContext` to `EventDispatcher`.

### 2.5 StreamPublisher
- **Role:** Responsible for routing outputs, managing observer fan-out, and materializing streams.
- **Scope:** It receives the `StepLifecycle` object to track progress. It calls `lifecycle.finish()` when the stream is fully consumed.

## 3. Strict Sync/Async Boundaries

- **Rule:** The runtime engines will no longer defensively check for synchronous vs asynchronous callables (`inspect.iscoroutinefunction`).
- **Async Engine:** Assumes **all** callables (steps, materializers, error materializers, observers) are `async`.
- **Sync Engine:** Assumes **all** callables are synchronous.
- **Adapter:** To support existing tests or use cases where a synchronous callable (like a simple lambda) is passed to an async pipeline, a new explicit `async_adapter` utility will be introduced in `synaflow.execution`.
- **Validation:** The DAG Builder (at compile time) must be updated to enforce these type guarantees, failing fast if a sync callable is passed to an async pipeline without an adapter.

## 4. Testability
This architecture ensures that each component can be unit-tested in isolation:
- `ArgumentBuilder` can be tested with mock outputs and dependencies.
- `StepLifecycle` can be tested by mocking the `EventDispatcher`.
- `StreamPublisher` can be tested by passing a mock `StepLifecycle`.
- The strict sync/async boundaries remove the need for testing mixed-type resolution inside the engine itself.
