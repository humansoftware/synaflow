# 2026-07-04: Strict Async Materializers via Context-Aware Factories

## Goal
Enforce strict async boundaries natively across the entire `synaflow` pipeline execution engine by ensuring that default materializers match the pipeline's runtime mode (sync vs async), thus removing all exceptions and dynamic runtime checking.

## Context
In previous iterations, the async engine required a defensive fallback in `StreamPublisher` because the DAG Builder injected synchronous default materializers (like `list` and `log_error`) universally. To bypass validation, flags like `_has_default_materializer` were added to `DagNode`. This creates "backdoors" in the validation rules and forces the engine to handle both sync and async code natively.

## Proposed Solution (Approach B)

### 1. Context Awareness
Add a boolean flag `is_async_pipeline: bool = False` to `MaterializeContext` and `ErrorMaterializeContext` in `synaflow/core/types.py`.

### 2. DAG Builder Execution Order
In `synaflow/core/dag_builder.py::build_dag()`, reorder the compilation steps:
- Move the `validate_sync_async_consistency` invocation to occur **before** `_resolve_materializers`.
- `validate_sync_async_consistency` already computes and sets `dag_obj.requires_async_runner`.
- Use `dag_obj.requires_async_runner` to set `is_async_pipeline` when instantiating `MaterializeContext` and `ErrorMaterializeContext` inside `_resolve_materializers`.

### 3. Context-Aware Factories
Update the default factories in `dag_builder.py` to inspect `ctx.is_async_pipeline` and return natively async functions if True:
- **`log_error_materializer_factory`**: Wraps its returned `log_error` function in `async_adapter` if `is_async_pipeline`.
- **`memory_materializer_factory`**: Returns an asynchronous consumer (`async def async_collection(stream): return constructor([x async for x in stream])`) if `is_async_pipeline`, which natively consumes the async stream and applies it to the target constructor (`list`, `set`, etc).

### 4. Strict Validation (Factory Contract Enforcement)
In `synaflow/core/definition.py`, the `_validate_no_sync_handlers` function can now be strictly applied without checking for `_has_default_materializer`. If it's an async pipeline, any synchronous materializer fails compilation.
This validation will now inherently act as the contract enforcer for factories: if the framework passes `is_async_pipeline=True` to a user's custom factory and that factory incorrectly returns a synchronous function, this strict validation will catch it and raise a `TypeError`. We will strip out all `_has_default_*` exceptions so the rule is absolute.

### 5. Cleanup `DagNode`
Remove `_has_default_materializer` and `_has_default_error_materializer` from `DagNode` in `synaflow/core/dag.py`.

### 6. Simplify the Async Engine
In `synaflow/execution/async_engine/stream_publisher.py`:
- Remove all fallback branches in `_apply_materializer` that conditionally handle synchronous materializers by executing `_collect_async_iterator` ahead of time.
- The `StreamPublisher` can now confidently assume that any materializer provided in an async pipeline is natively async and will consume the `AsyncGenerator` itself. This allows for a massive simplification of the publisher logic.

## Trade-offs
- **Pros**: `StreamPublisher` drops complexity. No runtime type checking. DAG validation is absolute and without exceptions.
- **Cons**: If a user implements a custom materializer for an async pipeline, they are now strictly required to accept an `AsyncGenerator` directly rather than a pre-collected list. This is mathematically correct for async environments anyway.
