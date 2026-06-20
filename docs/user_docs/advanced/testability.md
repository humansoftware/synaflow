# Testability & Execution Overrides

SynaFlow now treats testability as a first-class runtime concern.

The pipeline `Dag` compiles the contract once, and tests can swap only the
concrete runtime dependencies they care about through `ExecutionOverrides`.

That means you can:

- silence production observers without patching module globals
- replace materializers surgically for one compiled step
- inject fake runtime resources like databases or API clients
- target included sub-pipelines through `Scope` instead of hand-written strings

## The mental model

Build time decides:

- topology
- dependency edges
- sync vs async semantics
- eager vs lazy materialization rules
- which compiled keys exist

Run time decides:

- which concrete materializer is used for a compiled key
- which observers are active
- which runtime resources back declared resource slots

Overrides do **not** mutate graph structure. They only replace concrete runtime
implementations for already-declared keys.

## `empty()` vs `from_production()`

```python
from synaflow import ExecutionOverrides

empty = ExecutionOverrides.empty(p)
prod = ExecutionOverrides.from_production(p)
```

Use `empty()` when you want aggressive test isolation:

- observers default to no-op
- materializers keep the compiled callable unless you replace one
- resources must be filled explicitly before execution

Use `from_production()` when you want a surgical patch on top of the normal
pipeline behavior:

- compiled observers stay active unless replaced
- compiled materializers stay active unless replaced
- resources are still runtime-only and must be supplied explicitly

## Replacing a materializer

```python
from synaflow import ExecutionOverrides

overrides = ExecutionOverrides.empty(p)
overrides.materializers["records"] = tuple
```

This is useful when a test wants a concrete collection protocol without
touching pipeline construction.

## Silencing or replacing observers

```python
from synaflow import ExecutionOverrides, Observer, PIPELINE_SCOPE

events = []

def record(ctx):
    events.append((ctx.event.value, getattr(ctx, "step_name", None)))

overrides = ExecutionOverrides.empty(p)
overrides.observers[PIPELINE_SCOPE] = [Observer(record)]
```

With `empty()`, any observer scope you do **not** override stays silent.

## Injecting runtime resources

Declare the resource in the pipeline contract:

```python
from typing import NamedTuple
from synaflow import pipeline, step

class DB:
    ...

class Params(NamedTuple):
    user_id: int

def load_user(db: DB, user_id: int):
    return db.fetch(user_id)

p = pipeline(
    name="users",
    params=Params,
    resources={"db": DB},
    steps=[step("load_user", fn=load_user)],
)
```

Provide the concrete runtime object in the test:

```python
from synaflow import ExecutionOverrides, run

fake_db = FakeDB()
overrides = ExecutionOverrides.empty(p)
overrides.resources["db"] = fake_db

run(p, Params(user_id=42), overrides=overrides)
```

If a declared resource is missing, execution fails loudly.

## Sub-pipelines and `Scope`

For included pipelines, use `Scope` to address compiled step keys safely.

```python
from synaflow import ExecutionOverrides, Observer, Scope

sub = Scope("payments")
overrides = ExecutionOverrides.empty(p)

overrides.materializers[sub.scope("normalize")] = list
overrides.observers[sub.scope("validate")] = [Observer(test_recorder)]
```

This avoids hardcoding strings like `"payments__validate"` directly in tests.

## Typical testing patterns

### 1. Full isolation

```python
overrides = ExecutionOverrides.empty(p)
overrides.resources["db"] = FakeDB()
```

Good for unit-style tests where side effects should be silent by default.

### 2. Surgical patch over production behavior

```python
overrides = ExecutionOverrides.from_production(p)
overrides.materializers["records"] = list
overrides.resources["db"] = FakeDB()
```

Good for integration-style tests where most of the production contract should
stay intact.

### 3. Deep patch in a sub-pipeline

```python
sub = Scope("billing").scope("fraud")
overrides = ExecutionOverrides.empty(p)
overrides.observers[sub("score")] = [Observer(spy)]
```

Good when only one nested compiled step needs different runtime behavior.

## Practical guidance

- prefer `empty()` for unit tests
- prefer `from_production()` for integration tests
- use `PIPELINE_SCOPE` only for pipeline-level observers
- use `Scope` for compiled step keys, especially in included sub-pipelines
- treat `resources` as runtime-only dependencies, not user input params
