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

## `params` vs `resources` vs step outputs

These concepts live in different parts of the contract:

| Concept | Declared at build time | Supplied at run time | Addressed by overrides |
|---|---|---|---|
| `params` | `pipeline(params=...)` | `run(catalog.get_dag("users"), params)` | No |
| `resources` | `pipeline(resources=...)` | `overrides.resources[...] = ...` | Yes |
| step outputs | producer return types | produced by execution | No |
| materializers | compiled per step key | default compiled callable or override | Yes |
| observers | compiled per scope | default compiled observers or override | Yes |

`params` are user input data. `resources` are runtime dependencies such as a
database client, HTTP client, or cache handle. They are declared as production
factories on the pipeline and resolved when a step is injected. Step outputs are
neither: they are created by the pipeline itself.

## `empty()` vs `from_production()`

```python
from synaflow import ExecutionOverrides

empty = ExecutionOverrides.empty(p)
prod = ExecutionOverrides.from_production(p)
```

Use `empty()` when you want aggressive test isolation:

- observers default to no-op
- materializers keep the compiled callable unless you replace one
- production resource factories stay available unless you replace them

Use `from_production()` when you want a surgical patch on top of the normal
pipeline behavior:

- compiled observers stay active unless replaced
- compiled materializers stay active unless replaced
- production resource factories stay active unless replaced

In both modes, override keys must already exist in the compiled contract.
Unknown step keys, observer scopes, or resource names fail loudly instead of
being ignored.

## Replacing a materializer

```python
from synaflow import ExecutionOverrides

overrides = ExecutionOverrides.empty(p)
overrides.materializers["records"] = tuple
```

This is useful when a test wants a concrete collection protocol without
touching pipeline construction.

If the pipeline includes sub-pipelines, prefer `Scope(...)`:

```python
from synaflow import Scope

sub = Scope("payments")
overrides.materializers[sub.scope("records")] = tuple
```

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

With `from_production()`, this pattern is useful for swapping only the global
observer layer while keeping compiled step-level observers intact:

```python
from synaflow import ExecutionOverrides, Observer, PIPELINE_SCOPE

overrides = ExecutionOverrides.from_production(p)
overrides.observers[PIPELINE_SCOPE] = [Observer(test_recorder)]
```

## Production resource factories

The full runtime model for production providers, per-step caching, and
context-managed cleanup is documented in
[Resources & Factories](resources.md). This section focuses on how tests replace
those providers.

Declare the production resource factory in the pipeline contract:

```python
from typing import NamedTuple
from synaflow import pipeline, step, PipelineRegistry


class DB: ...


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

With that declaration, production can run without overrides:

```python
run(catalog.get_dag("users"), Params(user_id=42))
```

Provide a different runtime object in the test only when you want to replace
that provider:

```python
from synaflow import ExecutionOverrides, run

fake_db = FakeDB()
overrides = ExecutionOverrides.empty(p)
overrides.resources["db"] = fake_db

run(catalog.get_dag("users"), Params(user_id=42), overrides=overrides)
```

The inverse is also validated: setting `overrides.resources["missing"] = ...`
for a name the pipeline never declared is rejected.

Overrides can also be resource factories, including context-managed ones:

```python
overrides.resources["db"] = lambda: FakeDB()
```

## Context-managed resources

If a production resource factory returns a context manager, the executor enters
it before calling the step and injects the entered value:

```python
from contextlib import contextmanager


@contextmanager
def get_db() -> DB:
    with pool.connection() as conn:
        yield conn
```

This is useful for per-step connections and transactions.

The same pattern also works in `ExecutionOverrides.resources` when a test wants
replacement logic plus cleanup.

## Shared resources across included sub-pipelines

Resources are part of the flat compiled contract, even when steps come from
included pipelines. That means production configuration and tests must satisfy
the same declared names:

```python
overrides = ExecutionOverrides.from_production(p)
overrides.resources["db"] = FakeDB()
overrides.resources["payments_api"] = FakePaymentsAPI()
```

If two included pipelines declare the same resource name, they must refer to
the exact same contract entry. Conflicting definitions are a build-time error.

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

`Scope` is just a key helper. Executors still run the flat compiled DAG and do
not understand sub-pipelines as a separate runtime concept.

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

### 2b. Keep production observers except one scope

```python
from synaflow import PIPELINE_SCOPE

overrides = ExecutionOverrides.from_production(p)
overrides.observers[PIPELINE_SCOPE] = []
overrides.resources["db"] = FakeDB()
```

Good when the test wants normal execution semantics but no global telemetry.

### 3. Deep patch in a sub-pipeline

```python
sub = Scope("billing").scope("fraud")
overrides = ExecutionOverrides.empty(p)
overrides.observers[sub("score")] = [Observer(spy)]
```

Good when only one nested compiled step needs different runtime behavior.

### 4. Assert missing resource wiring fails loudly

```python
overrides = ExecutionOverrides.empty(p)
run(
    catalog.get_dag("users"), Params(user_id=42), overrides=overrides
)  # raises: missing resource "db"
```

Good for validating production wiring or test harness setup itself.

## Practical guidance

- prefer `empty()` for unit tests
- prefer `from_production()` for integration tests
- use `PIPELINE_SCOPE` only for pipeline-level observers
- use `Scope` for compiled step keys, especially in included sub-pipelines
- treat `resources` as runtime dependencies provided by factories, not user input params
- expect unknown override keys to fail loudly
- remember that override entries replace the runtime value for that key; they do
  not create new compiled keys

## See also

- [Build vs Run](../core-concepts/build-vs-run.md)
- [How the DAG is Wired](../core-concepts/how-dag-is-wired.md)
- [Resources & Factories](resources.md)
- [Custom Materializers](custom-materializers.md)
- [Custom Observers](custom-observers.md)
