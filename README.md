# SynaFlow 🌊🧠

**SynaFlow** is a lightweight, pure-Python pipeline engine that uses **Type Hints** to magically wire and execute Directed Acyclic Graphs (DAGs).

It solves the "dependency hell" and boilerplate associated with building data pipelines by automatically inferring the flow of data based exclusively on Python's static type annotations.

## The Problem It Solves

Building data pipelines usually involves two headaches:
1. **Explicit Wiring:** You have to manually define which function outputs go to which function inputs (e.g., `A >> B >> C`), creating verbose and fragile architectures.
2. **Memory Explosions vs. Lazy Evaluation:** Passing large datasets around usually means holding them entirely in memory (Lists) or dealing with complex generator management. If you have multiple consumers for a single generator, you usually have to write clunky `itertools.tee` boilerplate yourself.

## The SynaFlow Solution

SynaFlow looks at the **Type Hints** of your functions and automatically wires everything together for you. If `Step A` outputs an `int` and `Step B` requires an `int`, SynaFlow connects them instantly.

Furthermore, SynaFlow has a **smart lockstep streaming engine**:
- If a producer yields a `Generator` and a consumer expects an `Iterator`, SynaFlow streams the data lazily without ever holding it in memory.
- If multiple consumers want that same generator, SynaFlow automatically forks it (`tee`) and drives them in parallel (lockstep).
- If one consumer explicitly asks for a `list`, SynaFlow automatically materializes the data only for that specific branch.

## How is it different from other frameworks?

There are many amazing orchestration frameworks out there, but SynaFlow fills a very specific gap: **In-process Streaming Micro-Orchestration**.

### vs. Hamilton
[Hamilton](https://github.com/DAGWorks-Inc/hamilton) is a fantastic tool that also uses Python function signatures to build DAGs. However, Hamilton is heavily geared towards DataFrames and feature engineering, generally expecting functions to return concrete values (columns/scalars). **SynaFlow**, on the other hand, is built from the ground up to support **Native Generators and Lazy Streaming**. While Hamilton maps functions to columns, SynaFlow maps functions to continuous data streams, automatically interleaving multiple consumers in lockstep without memory spikes.

### vs. Airflow / Prefect / Dagster
These are **Macro-Orchestrators**. They are designed to orchestrate heavy, distributed tasks across clusters, Docker containers, and different machines. They rely on state databases and massive IO overhead. **SynaFlow is a Micro-Orchestrator**. It runs entirely within a single Python process. You would use Airflow to trigger a daily job, but you would use SynaFlow *inside* that job to smartly route and stream millions of rows between your Python functions.

## Quickstart

```python
from typing import NamedTuple
from collections.abc import Generator, Iterator
from synaflow import pipeline, step, run

# Define the data required to start your pipeline
class MyParams(NamedTuple):
    count: int

# 1. Producer outputs a stream
def producer(count: int) -> Generator[int, None, None]:
    yield from range(count)

# 2. Transformer consumes the stream lazily
def transformer(producer: Iterator[int]) -> Generator[int, None, None]:
    for val in producer:
        yield val * 10

# 3. Consumer automatically gets the stream!
def consumer(transformer: Iterator[int]) -> None:
    for x in transformer:
        print(f"Consumed: {x}")

# SynaFlow reads the Type Hints and wires the DAG automatically!
my_pipeline = pipeline(
    name="example",
    params=MyParams,
    steps=[
        step("producer", fn=producer),
        step("transformer", fn=transformer),
        step("consumer", fn=consumer)
    ]
)

# Run it
run(my_pipeline, MyParams(count=5))

# Export the DAG as JSON
print(my_pipeline.to_dict())
```

When you export the pipeline using `my_pipeline.to_dict()`, you get a precise representation of the nodes, their inferred dependencies, and their return types:

```json
{
  "producer": {
    "deps": {
      "count": "int"
    },
    "output": "Generator[int, None, None]",
    "fn": "producer",
    "on_error": "stop",
    "needs_materialize": false
  },
  "transformer": {
    "deps": {
      "producer": "Iterator[int]"
    },
    "output": "Generator[int, None, None]",
    "fn": "transformer",
    "on_error": "stop",
    "needs_materialize": false
  },
  "consumer": {
    "deps": {
      "transformer": "Iterator[int]"
    },
    "output": "None",
    "fn": "consumer",
    "on_error": "stop",
    "needs_materialize": false
  },
  "count": {
    "deps": {},
    "output": "int",
    "fn": null,
    "on_error": null,
    "needs_materialize": false
  }
}
```

## Advanced Features
- **Auto-DAG compilation and validation** before execution.
- Strict type-checking: Pipeline refuses to run if type annotations are incompatible.
- Easily export DAG structures as JSON (`my_pipeline.to_dict()`) for snapshot testing or UI rendering.

## License
MIT License
