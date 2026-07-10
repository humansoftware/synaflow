# Introduction

Welcome to SynaFlow! Here's how the documentation is organized so you can find
what you need.

## If you're new here

Start with the [**Tutorial**](../tutorial/hello-world.md) — a step-by-step guide
that builds a real pipeline from scratch across four levels:

1. **Hello World** — split a message into characters
2. **Lowercase Transform** — convert each character with EACH mode
3. **Character Counter** — count frequencies into a dictionary
4. **Key-Value Printer** — iterate and print the results

Every page has **Sync / Async tabs** so you can follow along in either style.

## Understanding the concepts

Once you've built something, dive into [**Core Concepts**](../core-concepts/how-dag-is-wired.md):

- [**How the DAG is Wired**](../core-concepts/how-dag-is-wired.md) — step names,
  parameter names, and type hints: the three rules that build the graph.
- [**Build vs Run**](../core-concepts/build-vs-run.md) — the strict separation
  between DAG compilation and execution that enables custom runners, external
  orchestrator export, and deterministic behavior.
- [**Lockstep Data Flow**](../core-concepts/lockstep-flow.md) — how one item
  flows entirely through the pipeline before the next is produced, guaranteeing
  extreme memory efficiency.
- [**Max In Flight**](../core-concepts/max-in-flight.md) — how to let a
  producing stream get a bounded number of items ahead of the next consumer
  stage without giving up lazy streaming, including real sync and async HTTP
  examples for I/O-bound workloads.
- [**Event-Based Processing**](../core-concepts/event-based.md) — how lazy
  streaming makes the framework idempotent by default and naturally suited for
  processing events individually or in time windows.
- [**Materialization & Error Policies**](../core-concepts/materialization.md) —
  when data is collected into memory, how to force materialization, and how
  `on_error=STOP` / `on_error=CONTINUE` affect the data flow.
- [**Semantic Naming**](../core-concepts/semantic-naming.md) — smart binding:
  using singular, plural, and suffixes naturally without exact name matching.
- [**DAG Construction**](../core-concepts/dag-construction.md) — the build-time
  validation, JSON export, and execution levels.
- [**Sync & Async Parity**](../core-concepts/sync-async.md) — identical
  semantics for sync and async, and how to convert between them.
- [**Examples**](../core-concepts/examples.md) — auto-generated diagrams and
  source code for every pipeline in the test corpus.

## Going deeper

The [**Advanced**](../advanced/custom-materializers.md) section covers:

- [**Testability & Overrides**](../advanced/testability.md) — replace
  materializers, observers, and resource providers without mutating the
  compiled DAG contract.
- [**Resources & Factories**](../advanced/resources.md) — declare production
  resource providers, use context managers for per-step cleanup, and override
  them in tests.
- [**Custom Materializers**](../advanced/custom-materializers.md) — write your
  own disk, database, or cloud-backed collectors.
- [**Custom Observers**](../advanced/custom-observers.md) — monitor pipeline
  and step lifecycle events for logging, metrics, or tracing.
- [**Export Guidance**](../advanced/export-guidance.md) — compile SynaFlow DAGs
  into Airflow, Prefect, or custom orchestrators.

If you're coming from another ecosystem, the [**Comparisons**](../comparisons/java-streams.md)
section maps SynaFlow concepts to Java Streams and LINQ.

## Quick reference

```python
from collections.abc import Generator, Iterator
from typing import NamedTuple
from synaflow import pipeline, step, run, PipelineRegistry


class Params(NamedTuple):
    count: int

def producer(count: int) -> Generator[int, None, None]:
    yield from range(count)

def consumer(producer: Iterator[int]) -> None:
    for x in producer:
        print(x)

p = pipeline(
    name="quickstart",
    params=Params,
    steps=[
        step("producer", fn=producer),
        step("consumer", fn=consumer),
    ],
)
catalog = PipelineRegistry()
catalog["quickstart"] = p

run(catalog.get_dag("quickstart"), Params(count=5))
```

## Next

Jump into the [Tutorial](../tutorial/hello-world.md) and build your first pipeline.
