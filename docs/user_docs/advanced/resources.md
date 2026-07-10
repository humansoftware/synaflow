# Resources & Factories

Resources are runtime dependencies injected into step parameters by name.

Typical examples:

- database pools
- per-step database connections
- transactions
- HTTP clients
- caches

They are different from `params`:

- `params` are user input data passed to `run(catalog.get_dag("users"), params)`
- `resources` are providers declared on the pipeline itself

## Production factories

Declare a production provider in `pipeline(resources=...)`:

```python
from typing import NamedTuple
from synaflow import pipeline, step, PipelineRegistry


class DB:
    ...

class Params(NamedTuple):
    user_id: int

def get_db() -> DB:
    return DB(...)

def load_user(db: DB, user_id: int):
    return db.fetch(user_id)

p = pipeline(
    name="users",
    params=Params,
    resources={"db": get_db},
    steps=[step("load_user", fn=load_user)],
)
catalog = PipelineRegistry()
catalog.add(p)

```

The return annotation is required. SynaFlow compiles the resource contract from
that type and calls the factory when the step is injected.

## Per-step resolution

Factories are called per step execution.

If two different steps depend on `db`, the factory is called twice by default.
If you want reuse, cache it explicitly:

```python
from functools import cache

@cache
def get_pool() -> Pool:
    return create_pool(...)
```

This keeps lifecycle decisions explicit in user code.

## Context-managed resources

If a factory returns a context manager, the executor enters it before calling
the step and injects the entered value:

```python
from contextlib import contextmanager

@contextmanager
def get_db() -> DB:
    with pool.connection() as conn:
        yield conn
```

That is the intended pattern for:

- per-step connections
- per-step transactions
- temporary sessions or handles

Important:

- the context wraps only the step call itself
- lazy output consumption after the step returns stays outside that context

## Common patterns

### Shared pool, fresh connection per step

```python
from contextlib import contextmanager
from functools import cache

@cache
def get_pool() -> Pool:
    return create_pool(...)

@contextmanager
def get_db() -> DB:
    with get_pool().connection() as conn:
        yield conn
```

### Transaction per step

```python
from contextlib import contextmanager

@contextmanager
def get_tx() -> Transaction:
    tx = db.begin()
    try:
        yield tx
        tx.commit()
    except Exception:
        tx.rollback()
        raise
    finally:
        tx.close()
```

### Async transaction per step

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def get_tx() -> Transaction:
    tx = await db.begin()
    try:
        yield tx
        await tx.commit()
    except Exception:
        await tx.rollback()
        raise
    finally:
        await tx.close()
```

## Overrides in tests

Tests can replace the production provider through `ExecutionOverrides`:

```python
from synaflow import ExecutionOverrides, run

overrides = ExecutionOverrides.empty(p)
overrides.resources["db"] = FakeDB()

run(catalog.get_dag("users"), Params(user_id=42), overrides=overrides)
```

Override providers can also be callables:

```python
overrides.resources["db"] = lambda: FakeDB()
```

And they can return context managers too:

```python
from contextlib import contextmanager

@contextmanager
def fake_tx() -> FakeTransaction:
    tx = FakeTransaction()
    try:
        yield tx
    finally:
        tx.closed = True

overrides.resources["tx"] = fake_tx
```

Override precedence is simple:

1. `ExecutionOverrides.resources`, when present
2. production resource factory declared on the pipeline

## Sub-pipelines

Included sub-pipelines share the same flat compiled resource contract.

That means:

- sub-pipeline resource names are lifted into the parent contract
- duplicate resource names must refer to the same declared contract entry
- executors still operate on the flattened DAG, not on nested runtime pipeline objects

## See also

- [How the DAG is Wired](../core-concepts/how-dag-is-wired.md)
- [Build vs Run](../core-concepts/build-vs-run.md)
- [Testability & Execution Overrides](testability.md)
