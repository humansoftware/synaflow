# Sync Executor Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the sync `PipelineExecutor` (currently >1000 lines) by extracting threshold logic, event dispatching, step scope management, and stream publishing into dedicated components with clear lifecycle state, reducing the main executor to an orchestrator.

**Architecture:** Decompose `synaflow/execution/sync_engine/executor.py` into 5 files: `executor.py`, `threshold.py` (pure functions), `event_dispatch.py` (`EventDispatcher`), `step_scope.py` (`StepScope`), and `stream_publisher.py` (`StreamPublisher`). The public API and existing tests remain unchanged.

**Tech Stack:** Python, pytest.

## Global Constraints

- **No existing tests are modified**: Validation relies entirely on running the existing test suite (`pytest tests/`).
- **No public API changes**: `PipelineExecutor` and `run()` signatures must remain identical.
- **Sync Engine First**: This plan only targets the sync engine. Async will be a separate plan.
- **Commit frequently**: Each task is a separate, self-contained extraction that should be committed independently.

---

### Task 1: Threshold Extraction (`threshold.py`)

**Files:**
- Create: `synaflow/execution/sync_engine/threshold.py`
- Modify: `synaflow/execution/sync_engine/executor.py`

**Interfaces:**
- Produces: `check_threshold`, `wrap_threshold_raise_if_manual`, `compute_completed_all_inputs_for_all`, `has_threshold`

- [ ] **Step 1: Create threshold.py and move pure functions**

```python
# synaflow/execution/sync_engine/threshold.py
from typing import Any
from synaflow.core.exceptions import ThresholdExceededException

def wrap_threshold_raise_if_manual(exc: BaseException, step_name: str) -> BaseException:
    # Copy implementation of _wrap_threshold_raise_if_manual from executor.py
    pass

def check_threshold(step_name: str, node: Any, invocation_count: int, error_count: int) -> None:
    # Copy implementation of _check_threshold from executor.py
    pass

def compute_completed_all_inputs_for_all(node: Any, arguments: dict, exc: ThresholdExceededException) -> bool:
    # Copy implementation of _compute_completed_all_inputs_for_all from executor.py
    pass

def has_threshold(node: Any) -> bool:
    # Copy implementation of _has_threshold from executor.py
    pass
```
*Note: Copy the exact implementations from `executor.py`, just removing the leading underscore from their names.*

- [ ] **Step 2: Remove functions from executor.py and update imports**

Modify `synaflow/execution/sync_engine/executor.py`:
1. Add `from .threshold import check_threshold, wrap_threshold_raise_if_manual, compute_completed_all_inputs_for_all, has_threshold`
2. Delete the original private `_check_threshold`, `_wrap_threshold_raise_if_manual`, etc.
3. Update all call sites in `executor.py` to use the public names (e.g., change `_has_threshold` to `has_threshold`).

- [ ] **Step 3: Run tests to verify extraction**

Run: `uv run pytest tests/`
Expected: PASS (100% of tests passing)

- [ ] **Step 4: Commit**

```bash
git add synaflow/execution/sync_engine/threshold.py synaflow/execution/sync_engine/executor.py
git commit -m "refactor(sync): extract threshold logic to pure functions"
```

---

### Task 2: Event Dispatch Extraction (`event_dispatch.py`)

**Files:**
- Create: `synaflow/execution/sync_engine/event_dispatch.py`
- Modify: `synaflow/execution/sync_engine/executor.py`

**Interfaces:**
- Consumes: `dag`, `run_id`, `overrides`
- Produces: `EventDispatcher` class with `pipeline_started`, `pipeline_completed`, `pipeline_failed`, `step_started`, `step_completed`, `step_failed`, `materialization_started`, `materialization_completed`, `materialization_failed`, `resolve_step_observers`, `resolve_pipeline_observers`.

- [ ] **Step 1: Create event_dispatch.py**

Create the `EventDispatcher` class, moving the logic from `_resolve_pipeline_observers`, `_resolve_step_observers`, `_dispatch_pipeline_event`, `_dispatch_step_event` (split this one into started/completed/failed), and `_dispatch_materialization_event`.

```python
# synaflow/execution/sync_engine/event_dispatch.py
from typing import Any
from synaflow.core.dag import Dag
from synaflow.execution.overrides import ExecutionOverrides
# ... import event types

class EventDispatcher:
    def __init__(self, dag: Dag, run_id: str, overrides: ExecutionOverrides | None):
        self._dag = dag
        self._run_id = run_id
        self._overrides = overrides

    # Implement observer resolution and event emission based on the spec
    # ...
```
*Note: Adapt the existing logic to use `self._dag` etc., and replace the generic enum routing with specific methods as defined in the spec.*

- [ ] **Step 2: Refactor executor.py to use EventDispatcher**

Modify `synaflow/execution/sync_engine/executor.py`:
1. In `PipelineExecutor.__init__`, instantiate `self.events = EventDispatcher(self.dag, self.run_id, overrides)`
2. Remove all `_resolve_*` and `_dispatch_*` methods that were moved.
3. Update call sites (e.g., `self._dispatch_pipeline_event(PipelineEvent.STARTED)` becomes `self.events.pipeline_started()`). Keep orchestration methods like `_emit_immediate_completion` inside the executor, but have them call `self.events.*`.

- [ ] **Step 3: Run tests to verify extraction**

Run: `uv run pytest tests/`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add synaflow/execution/sync_engine/event_dispatch.py synaflow/execution/sync_engine/executor.py
git commit -m "refactor(sync): extract EventDispatcher class"
```

---

### Task 3: Step Scope Extraction (`step_scope.py`)

**Files:**
- Create: `synaflow/execution/sync_engine/step_scope.py`
- Modify: `synaflow/execution/sync_engine/executor.py`

**Interfaces:**
- Consumes: `dag`, `outputs`, `overrides`, `resource_factories`
- Produces: `StepScope` class with `build_arguments`, `seed_runtime_inputs`, `close_managed_streams`, `attach_cleanup`, `resolve_materializer`.

- [ ] **Step 1: Create step_scope.py**

Move `_build_arguments`, `_seed_runtime_inputs`, `_resolve_materializer`, `_resolve_resource_argument`, `_attach_argument_cleanup`, and `_close_managed_stream_arguments` into the `StepScope` class.

```python
# synaflow/execution/sync_engine/step_scope.py
from typing import Any
from contextlib import ExitStack
from synaflow.core.dag import Dag
from synaflow.execution.overrides import ExecutionOverrides

class StepScope:
    def __init__(self, dag: Dag, outputs: dict, overrides: ExecutionOverrides | None, resource_factories: dict[str, Any]):
        self._dag = dag
        self._outputs = outputs
        self._overrides = overrides
        self._resource_factories = resource_factories

    # Implement methods, adapting them to use instance variables (self._dag, etc.)
    # Note: build_arguments should return (args_dict, stack)
```

- [ ] **Step 2: Refactor executor.py to use StepScope**

Modify `synaflow/execution/sync_engine/executor.py`:
1. In `PipelineExecutor.__init__`, instantiate `self.scope = StepScope(self.dag, self.outputs, overrides, resource_factories or {})`
2. Remove the moved methods from `PipelineExecutor`.
3. Update call sites in `_run_step`, `execute`, etc., to use `self.scope`. Example: `self.scope.seed_runtime_inputs(params)`. Ensure `_run_step` handles the `ExitStack` returned by `build_arguments`.

- [ ] **Step 3: Run tests to verify extraction**

Run: `uv run pytest tests/`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add synaflow/execution/sync_engine/step_scope.py synaflow/execution/sync_engine/executor.py
git commit -m "refactor(sync): extract StepScope class for arg/resource lifecycle"
```

---

### Task 4: Stream Publisher Extraction (`stream_publisher.py`)

**Files:**
- Create: `synaflow/execution/sync_engine/stream_publisher.py`
- Modify: `synaflow/execution/sync_engine/executor.py`

**Interfaces:**
- Consumes: `dag`, `outputs`, `events` (EventDispatcher), `step_output_observers`, `scope` (StepScope)
- Produces: `StreamPublisher` class with `publish`, `abort`, `cleanup`.

- [ ] **Step 1: Create stream_publisher.py**

Move `_publish_output`, `_publish_scalar_output`, `_publish_stream_to_multiple_consumers`, `_publish_stream_to_single_consumer`, `_materialize_stream_output`, `_materialize_with_events`, `_notify_observers`, `_observer_branch_names`, `_collect_observer_items`, `_start_observer_threads`, `_abort_fanouts`, and `_cleanup_fanouts` into `StreamPublisher`.

```python
# synaflow/execution/sync_engine/stream_publisher.py
from typing import Any
import threading
from synaflow.core.dag import Dag
from .event_dispatch import EventDispatcher
from .step_scope import StepScope
from .fanout import SyncFanout # Assuming fanout is available here

class StreamPublisher:
    def __init__(self, dag: Dag, outputs: dict, events: EventDispatcher, step_output_observers: list, scope: StepScope):
        self._dag = dag
        self._outputs = outputs
        self._events = events
        self._step_output_observers = step_output_observers
        self._scope = scope
        self._active_fanouts = []
        self._observer_threads = []

    def publish(self, step_name: str, output: Any, node: Any) -> None:
        # Implementation of _publish_output logic
        pass

    def abort(self, exception: BaseException | None = None) -> None:
        # Implementation of _abort_fanouts logic
        pass

    def cleanup(self) -> None:
        # Implementation of _cleanup_fanouts logic
        pass

    # Internal methods translated from executor.py...
```

- [ ] **Step 2: Refactor executor.py to use StreamPublisher**

Modify `synaflow/execution/sync_engine/executor.py`:
1. In `PipelineExecutor.__init__`, instantiate `self.publisher = StreamPublisher(self.dag, self.outputs, self.events, step_output_observers or [], self.scope)`
2. Remove the moved methods from `PipelineExecutor`.
3. Update call sites. `self._publish_output` becomes `self.publisher.publish`, `_abort_fanouts` becomes `self.publisher.abort`, etc.
4. Keep orchestration methods like `_emit_step_result`, `_emit_deferred_completion`, and `_wrap_deferred_output` inside the executor, but have them call `self.events.*` for emitting.

- [ ] **Step 3: Run tests to verify extraction**

Run: `uv run pytest tests/`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add synaflow/execution/sync_engine/stream_publisher.py synaflow/execution/sync_engine/executor.py
git commit -m "refactor(sync): extract StreamPublisher for fanout lifecycle"
```
