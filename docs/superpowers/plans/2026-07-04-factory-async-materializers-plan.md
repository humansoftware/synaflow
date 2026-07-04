# Factory Async Materializers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce strict async boundaries natively across the entire `synaflow` pipeline execution engine by ensuring that default materializers match the pipeline's runtime mode.

**Architecture:** We will make the materializer factories context-aware by passing `is_async_pipeline` into their contexts. This requires `dag_builder.py` to evaluate the pipeline's async nature before resolving defaults. Finally, we will remove the backdoors (`_has_default_materializer`) from the validation logic, and drastically simplify the `StreamPublisher`.

**Tech Stack:** Python 3.10+, Pytest

## Global Constraints

- **No new unit tests during this phase.** The user has explicitly stated in previous iterations of this branch: "nao vamos criar testes unitarios ainda nessa fase". Validation relies strictly on ensuring the existing 632 tests pass.
- **Do not use `__name__ == "..."` magic string checks.**
- **All code must be fully typed.**

---

### Task 1: Update Data Structures (Types and DagNode)

**Files:**
- Modify: `synaflow/core/types.py`
- Modify: `synaflow/core/dag.py`

**Interfaces:**
- Consumes: Existing dataclass definitions.
- Produces: `MaterializeContext` and `ErrorMaterializeContext` with `is_async_pipeline` flag. `DagNode` stripped of legacy flags.

- [ ] **Step 1: Update Types**

In `synaflow/core/types.py`:
Add `is_async_pipeline: bool = False` to both `MaterializeContext` and `ErrorMaterializeContext`.

```python
@dataclass
class MaterializeContext:
    pipeline_name: str
    dataset_name: str
    item_type: Any
    consumer_type: Any = None
    is_async_pipeline: bool = False

@dataclass
class ErrorMaterializeContext:
    pipeline_name: str
    dataset_name: str
    is_async_pipeline: bool = False
```

- [ ] **Step 2: Cleanup DagNode**

In `synaflow/core/dag.py`:
Remove `_has_default_materializer` and `_has_default_error_materializer` from `DagNode`.

```python
@dataclass
class DagNode:
    # ... existing fields ...

    # REMOVE THESE LINES:
    # _has_default_materializer: bool = False
    # _has_default_error_materializer: bool = False
```

- [ ] **Step 3: Run existing tests to verify failure/impact**

Run: `uv run pytest tests/`
Expected: Test failures since `_has_default_materializer` is referenced in `dag_builder.py` and `definition.py`.

- [ ] **Step 4: Commit**

```bash
git add synaflow/core/types.py synaflow/core/dag.py
git commit -m "refactor: add is_async_pipeline to contexts and remove legacy DagNode flags"
```

---

### Task 2: Reorder DAG Builder and Update Factories

**Files:**
- Modify: `synaflow/core/dag_builder.py`

**Interfaces:**
- Consumes: Updated Contexts.
- Produces: `build_dag` correctly orders consistency validation BEFORE materializer resolution, and factories return async consumers when flagged.

- [ ] **Step 1: Reorder compilation and fix references in `_resolve_materializers`**

In `synaflow/core/dag_builder.py`, within `build_dag`:
Swap the order of `_resolve_materializers` and `validate_sync_async_consistency`.

```python
    # Calculate requires_async_runner FIRST
    validate_sync_async_consistency(
        dag_obj,
        pipeline_name,
        steps,
        memory_materializer_factory,
        is_default_factory=is_default_factory,
    )

    # NOW resolve materializers
    _resolve_materializers(
        dag_obj,
        indexes,
        memory_materializer_factory,
        error_materializer_factory,
    )
```

Inside `_resolve_materializers`, remove all assignments to `node._has_default_materializer = True` and `node._has_default_error_materializer = True`.

When constructing the contexts in `_resolve_materializers`, pass `is_async_pipeline`:

```python
        if mat and is_factory(mat):
            ctx = MaterializeContext(
                pipeline_name=dag.name,
                dataset_name=name,
                item_type=node.output,
                consumer_type=resolve_materializer_consumer_type(name),
                is_async_pipeline=dag.requires_async_runner,
            )
            node.materializer = mat(ctx)
```

And for error materializers:

```python
        if err_mat and is_factory(err_mat):
            err_ctx = ErrorMaterializeContext(
                pipeline_name=dag.name,
                dataset_name=name,
                is_async_pipeline=dag.requires_async_runner,
            )
            node.error_materializer = err_mat(err_ctx)
```

- [ ] **Step 2: Update Factories**

In `synaflow/core/dag_builder.py`:

Modify `memory_materializer_factory`:
```python
def memory_materializer_factory(ctx: MaterializeContext):
    # ... existing infer logic up to the candidate loop ...
    tp = getattr(ctx.consumer_type, "__origin__", None) or ctx.consumer_type

    constructor = None
    if tp is not None:
        for candidate in ((list, MutableSequence), (set, MutableSet), (dict, MutableMapping)):
            try:
                if issubclass(tp, candidate):
                    constructor = candidate[0]
                    break
            except TypeError:
                continue
        if constructor is None:
            if tp is tuple:
                constructor = tuple
            elif is_scalar(tp):
                constructor = _identity
            elif tp in (AsyncIterator, Iterator, Iterable, AsyncIterable, AbcAsyncIterator, AbcIterator, AbcIterable, AbcAsyncIterable):
                constructor = list

    if constructor is None:
        raise ValueError(...)

    if ctx.is_async_pipeline:
        async def async_collection(stream):
            if isinstance(stream, (AsyncIterator, AbcAsyncIterator, AsyncGenerator)):
                items = [x async for x in stream]
            else:
                items = list(stream)
            return constructor(items)
        return async_collection

    return constructor
```

Modify `log_error_materializer_factory`:
```python
def log_error_materializer_factory(ctx: ErrorMaterializeContext):
    log = logging.getLogger("synaflow")

    def log_error(error_ctx) -> None:
        log.warning(
            "[%s] [%s] [%s] [%s] %s: %s",
            error_ctx.pipeline_name,
            error_ctx.dataset_name,
            error_ctx.step_name,
            error_ctx.run_id,
            type(error_ctx.exception).__name__,
            error_ctx.exception,
        )
        log.debug(traceback.format_exc())

    if ctx.is_async_pipeline:
        from synaflow.execution.adapters import async_adapter
        return async_adapter(log_error)

    return log_error
```

- [ ] **Step 3: Run existing tests to verify failure/impact**

Run: `uv run pytest tests/`
Expected: Test failures because `definition.py` still references the removed `_has_default_*` flags.

- [ ] **Step 4: Commit**

```bash
git add synaflow/core/dag_builder.py
git commit -m "feat: make materializer factories context-aware and reorder dag compilation"
```

---

### Task 3: Enforce Strict Validation

**Files:**
- Modify: `synaflow/core/definition.py`

**Interfaces:**
- Consumes: DAG constructed in Task 2.
- Produces: Strictly validated pipeline definition.

- [ ] **Step 1: Clean up `definition.py`**

In `synaflow/core/definition.py`, inside `_validate_no_sync_handlers`:

Remove the `_has_default_materializer` exception. Change:
```python
    for node in pipeline_def.dag.steps.values():
        if node.materializer is not None and not is_async_callable(node.materializer):
            if not getattr(node, "_has_default_materializer", False):
                mat_name = getattr(node.materializer, "__name__", str(node.materializer))
                raise TypeError(...)
```
To:
```python
    for node in pipeline_def.dag.steps.values():
        if node.materializer is not None and not is_async_callable(node.materializer):
            mat_name = getattr(node.materializer, "__name__", str(node.materializer))
            raise TypeError(
                f"Pipeline '{pipeline_def.name}': materializer '{mat_name}' "
                f"is synchronous but the pipeline runs asynchronously."
            )
```

Remove the `_has_default_error_materializer` exception and `async_adapter` fallback. Change:
```python
        if node.error_materializer is not None and not is_async_callable(node.error_materializer):
            if getattr(node, "_has_default_error_materializer", False):
                from synaflow.execution.adapters import async_adapter
                node.error_materializer = async_adapter(node.error_materializer)
            else:
                mat_name = getattr(node.error_materializer, "__name__", str(node.error_materializer))
                raise TypeError(...)
```
To:
```python
        if node.error_materializer is not None and not is_async_callable(node.error_materializer):
            mat_name = getattr(node.error_materializer, "__name__", str(node.error_materializer))
            raise TypeError(
                f"Pipeline '{pipeline_def.name}': error_materializer '{mat_name}' "
                f"is synchronous but the pipeline runs asynchronously."
            )
```

- [ ] **Step 2: Run test suite**

Run: `uv run pytest tests/`
Expected: Most tests should pass now, though `StreamPublisher` might still have minor integration issues if it relies on being fed `list` rather than an async consumer.

- [ ] **Step 3: Commit**

```bash
git add synaflow/core/definition.py
git commit -m "refactor: enforce strict async validation without default backdoors"
```

---

### Task 4: Simplify StreamPublisher

**Files:**
- Modify: `synaflow/execution/async_engine/stream_publisher.py`

**Interfaces:**
- Consumes: A strictly async `materializer` function.
- Produces: Cleaned up publisher logic.

- [ ] **Step 1: Remove defensive checks in `_apply_materializer`**

In `synaflow/execution/async_engine/stream_publisher.py`:

Since `definition.py` now guarantees `materializer` is async in an async pipeline, we don't need `_collect_async_iterator` anymore. We can remove the entire fallback logic.

Refactor `_apply_materializer` to simply await the materializer directly (and you can remove the `iscoroutinefunction` check as well since we know it's async, or keep a basic `await` since the framework strictly enforces it).
Because `memory_materializer_factory` now handles `AsyncGenerator` natively, we can just pass the stream to it.

```python
    async def _apply_materializer(
        self,
        step_name: str,
        materializer: Callable,
        value: Any,
    ) -> tuple[Any, bool, Exception | None]:
        if materializer is None:
            if isinstance(value, (AsyncIterator, AsyncGenerator, Iterator, Generator)):
                items, had_error, exc = await self._collect_async_iterator(step_name, value)
                return items, had_error, exc
            return value, False, None

        # Materializer is guaranteed to be async by validation.
        # It natively handles consuming the stream if needed.
        try:
            result = await materializer(value)
            return result, False, None
        except Exception as e:
            return None, True, e
```

*(Note: We still need `_collect_async_iterator` for the `materializer is None` case, so do not delete the `_collect_async_iterator` method itself!)*

- [ ] **Step 2: Run test suite**

Run: `uv run pytest tests/`
Expected: PASS (All 632 tests should pass)

- [ ] **Step 3: Commit**

```bash
git add synaflow/execution/async_engine/stream_publisher.py
git commit -m "refactor: simplify async StreamPublisher by relying on strict async materializers"
```
