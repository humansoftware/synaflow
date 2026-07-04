# Executor Architecture Refinement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refine the executor architecture by standardizing dependency resolution as `ArgumentBuilder`, extracting step state into `StepLifecycle`, and enforcing strict sync/async boundaries.

**Architecture:** We will systematically remove runtime checks for `inspect.iscoroutinefunction` across the async engine by enforcing types at DAG compilation time. We will introduce an explicit `async_adapter` for users who need to provide sync fallbacks. Finally, `DependencyResolver` will be renamed to `ArgumentBuilder` to reflect its true responsibility, standardizing attribute naming.

**Tech Stack:** Python 3.10+, `asyncio`

## Global Constraints

- Do not modify any files related to `StreamPublisher` or the Error Materialization Flow.
- Do not write any new unit tests during this phase; validation relies strictly on the existing 632 tests passing.
- TDD steps usually provided by this template are omitted per spec requirement; instead, focus on implementing the changes and running `pytest tests/` to ensure no regressions.

---

### Task 1: Rename DependencyResolver to ArgumentBuilder

**Files:**
- Modify: `synaflow/execution/sync_engine/dependency_resolver.py`
- Modify: `synaflow/execution/sync_engine/executor.py`
- Modify: `synaflow/execution/sync_engine/stream_publisher.py`
- Modify: `synaflow/execution/async_engine/dependency_resolver.py`
- Modify: `synaflow/execution/async_engine/executor.py`
- Modify: `synaflow/execution/async_engine/stream_publisher.py`

**Interfaces:**
- Consumes: Existing dependency resolver classes.
- Produces: `ArgumentBuilder` and `AsyncArgumentBuilder`.

- [ ] **Step 1: Rename the Sync class and module**
Rename `DependencyResolver` to `ArgumentBuilder` in `synaflow/execution/sync_engine/dependency_resolver.py`.
Rename the file itself from `dependency_resolver.py` to `argument_builder.py` using `git mv`.
Update the imports in `executor.py` and `stream_publisher.py` to point to `argument_builder`.

- [ ] **Step 2: Rename the Async class and module**
Rename `AsyncDependencyResolver` to `AsyncArgumentBuilder` in `synaflow/execution/async_engine/dependency_resolver.py`.
Rename the file itself from `dependency_resolver.py` to `argument_builder.py` using `git mv`.
Update the imports in `executor.py` and `stream_publisher.py` to point to `argument_builder`.

- [ ] **Step 3: Standardize Attributes**
In `synaflow/execution/async_engine/argument_builder.py`, ensure internal state attributes are private (`self._dag`, `self._outputs`) to match the sync version, and update any references inside the class to use the private properties.

- [ ] **Step 4: Verify**
Run: `uv run pytest tests/`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git commit -am "refactor: rename DependencyResolver to ArgumentBuilder"
```

---

### Task 2: Introduce StepLifecycle

**Files:**
- Create: `synaflow/execution/sync_engine/step_lifecycle.py`
- Create: `synaflow/execution/async_engine/step_lifecycle.py`
- Modify: `synaflow/execution/sync_engine/executor.py`
- Modify: `synaflow/execution/async_engine/executor.py`

**Interfaces:**
- Consumes: DAG nodes and EventDispatcher.
- Produces: `StepLifecycle` and `AsyncStepLifecycle` providing `.start()`, `.finish()`, `.record_success()`, `.record_error()`.

- [ ] **Step 1: Create Sync StepLifecycle**
Create `synaflow/execution/sync_engine/step_lifecycle.py` with the `StepLifecycle` class that accepts `node`, `step_name`, and `events` (EventDispatcher). Implement `start()`, `record_success()`, `record_error()`, and `finish()`.

- [ ] **Step 2: Create Async StepLifecycle**
Create `synaflow/execution/async_engine/step_lifecycle.py` with the `AsyncStepLifecycle` class mirroring the sync logic but awaiting the event dispatcher calls (`await self.events.step_started()`, etc).

- [ ] **Step 3: Integrate in Executors**
In both `executor.py` files, instantiate the lifecycle object before starting a step, invoke `lifecycle.start()`, and modify the immediate completion branches to invoke `lifecycle.finish()` rather than directly calling the `EventDispatcher`. Do NOT touch `StreamPublisher` logic, but ensure the `Executor` can still pass the needed references.

- [ ] **Step 4: Verify**
Run: `uv run pytest tests/`
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add synaflow/execution/sync_engine/step_lifecycle.py synaflow/execution/async_engine/step_lifecycle.py
git commit -am "feat: introduce StepLifecycle to encapsulate state tracking"
```

---

### Task 3: Enforce Strict Async Boundaries

**Files:**
- Create: `synaflow/execution/adapters.py`
- Modify: `synaflow/execution/async_engine/executor.py`
- Modify: `synaflow/execution/async_engine/event_dispatch.py`
- Modify: `synaflow/core/dag_builder.py` (or where validation occurs)

**Interfaces:**
- Consumes: Mixed sync/async tests passing lambdas.
- Produces: `async_adapter` and stricter async runtime engines.

- [ ] **Step 1: Create async_adapter utility**
Create `synaflow/execution/adapters.py` implementing a simple `async_adapter(fn)` that wraps a synchronous function into an asynchronous one.

- [ ] **Step 2: Remove inspect.iscoroutinefunction**
Remove `inspect.iscoroutinefunction` branching from `synaflow/execution/async_engine/event_dispatch.py` and `synaflow/execution/async_engine/executor.py`. Assume `await` on all materializers and observers.

- [ ] **Step 3: Validate in DAG Builder**
Add validation logic when registering observers, materializers, and error materializers to ensure they are coroutines if the pipeline expects it (or rely on tests failing to catch where adapters are needed).

- [ ] **Step 4: Fix Tests via Adapter**
Run the test suite. Identify failing tests in `tests/execution/async_engine/` that throw `TypeError: object NoneType can't be used in 'await' expression` or similar because a lambda was passed. Wrap those lambdas using `async_adapter`.

- [ ] **Step 5: Verify**
Run: `uv run pytest tests/`
Expected: PASS

- [ ] **Step 6: Commit**
```bash
git add synaflow/execution/adapters.py
git commit -am "feat: enforce strict async boundaries and remove runtime inspections"
```
