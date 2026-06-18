# SynaFlow 🌊🧠

> 📖 **Full documentation:** [humansoftware.github.io/synaflow](https://humansoftware.github.io/synaflow/)

**Write plain Python functions. SynaFlow builds the pipeline for you.**

[![PyPI](https://img.shields.io/pypi/v/synaflow?color=blue)](https://pypi.org/project/synaflow/)
[![License](https://img.shields.io/github/license/humansoftware/synaflow)](https://github.com/humansoftware/synaflow/blob/main/LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/synaflow)](https://pypi.org/project/synaflow/)

**SynaFlow** is a lightweight, pure-Python pipeline engine that uses **Type Hints** to automatically wire and execute Directed Acyclic Graphs (DAGs) with lockstep streaming and optional bounded handoff.

**Why the name?** **Synapse** + **Flow**. Just like synapses automatically wire neurons together, SynaFlow automatically wires your functions together based on their types. "Flow" represents the lazy, streaming nature of how data moves through those connections.

## Quickstart

```python
from collections.abc import Generator, Iterator
from typing import NamedTuple
from synaflow import pipeline, step, run

class Params(NamedTuple):
    count: int

def producer(count: int) -> Generator[int, None, None]:
    yield from range(count)

def transformer(producer: Iterator[int]) -> Generator[int, None, None]:
    for val in producer:
        yield val * 10

def consumer(transformer: Iterator[int]) -> None:
    for x in transformer:
        print(f"Consumed: {x}")

p = pipeline(
    name="example",
    params=Params,
    steps=[
        step("producer", fn=producer),
        step("transformer", fn=transformer),
        step("consumer", fn=consumer),
    ],
)

run(p, Params(count=5))
```

Three functions, three `step()` calls, zero manual wiring. SynaFlow reads the
type hints and wires the DAG automatically.

## What makes it different

### Type-hint wiring

Parameter names match producer names — SynaFlow connects them automatically.
Singular/plural/suffix synonyms work too (`item` → `items`, `user_list` → `users`).

### Lazy streaming with bounded handoff

SynaFlow streams lazily by default. Multiple consumers can stay lockstep, one
consumer can stay lazy while another materializes, and when you need a bounded
window between stages you can set `max_in_flight` on the producing step.

This is especially useful for I/O-bound pipelines where one step starts work
and the next resolves it, such as HTTP requests, RPC calls, or object-store
reads.

### Static validation at build time

Type errors, missing dependencies, circular graphs, mode conflicts — all caught
when `pipeline(...)` is called. If it compiles, it's valid. No runtime surprises.

### Build your own runner

The DAG compiles to a deterministic JSON contract. Write custom runners or
auto-generate native DAGs for Airflow, Prefect, or Dagster.

## How it compares

| | SynaFlow | Hamilton | Airflow / Prefect / Dagster |
|---|---|---|---|
| **Auto wiring** | ✅ type hints + smart binding | ✅ type hints (exact names) | ❌ explicit `A >> B` |
| **Lazy streaming** | ✅ lockstep + bounded handoff | ❌ DataFrame-centric | ❌ task-based |
| **Smart binding** | ✅ singular/plural/suffix | ❌ | ❌ |
| **Scope** | In-process micro | Feature engineering | Cluster orchestration |
| **DAG export** | ✅ JSON | ✅ | ✅ |
| **Sync/async parity** | ✅ identical | ❌ | ✅ |

Detailed comparisons: [Hamilton](https://humansoftware.github.io/synaflow/comparisons/hamilton/) ·
[Java Streams](https://humansoftware.github.io/synaflow/comparisons/java-streams/) ·
[LINQ](https://humansoftware.github.io/synaflow/comparisons/linq/)

## Installation

```bash
pip install synaflow
```

## Documentation

Start here: **[humansoftware.github.io/synaflow](https://humansoftware.github.io/synaflow/)**

| Section | Description |
|---|---|
| [Tutorial](https://humansoftware.github.io/synaflow/tutorial/hello-world/) | 5-level step-by-step guide building a pipeline from scratch |
| [Core Concepts](https://humansoftware.github.io/synaflow/core-concepts/how-dag-is-wired/) | How the DAG is wired, lockstep flow, max in flight, build vs run, event-based processing |
| [Examples](https://humansoftware.github.io/synaflow/core-concepts/examples/) | Every corpus pipeline with auto-generated diagrams and source code |
| [Comparisons](https://humansoftware.github.io/synaflow/comparisons/hamilton/) | Detailed comparisons with Hamilton, Java Streams, and LINQ |
| [Design Philosophy](docs/DESIGN_PHILOSOPHY.md) | Architectural decisions, contracts, and design rationale |

## License

MIT License
