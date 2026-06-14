# SynaFlow 🌊🧠

**SynaFlow** is a lightweight, pure-Python pipeline engine that uses **Type Hints** to magically wire and execute Directed Acyclic Graphs (DAGs).

=== "Sync"

    ```python
    from collections.abc import Generator, Iterator
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
            print(x)

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

=== "Async"

    ```python
    from collections.abc import AsyncGenerator, AsyncIterator
    from synaflow import pipeline, step, async_run

    class Params(NamedTuple):
        count: int

    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def transformer(producer: AsyncIterator[int]) -> AsyncGenerator[int, None]:
        async for val in producer:
            yield val * 10

    async def consumer(transformer: AsyncIterator[int]) -> None:
        async for x in transformer:
            print(x)

    p = pipeline(
        name="example",
        params=Params,
        steps=[
            step("producer", fn=producer),
            step("transformer", fn=transformer),
            step("consumer", fn=consumer),
        ],
    )

    async_run(p, Params(count=5))
    ```

## Why SynaFlow?

Building data pipelines usually involves two headaches:

1. **Explicit Wiring:** You manually define which function outputs go to which inputs (`A >> B >> C`), creating verbose architectures.
2. **Memory Explosions:** Passing large datasets around means holding them entirely in memory or dealing with complex generator management.

SynaFlow solves both by reading your **Type Hints** and automatically wiring the DAG, with a **lockstep streaming engine** that forks generators lazily — never holding entire datasets in memory unless you explicitly ask for it.

## How It Compares

| | SynaFlow | Hamilton | Airflow / Prefect |
|---|---|---|---|
| **Type-hint wiring** | ✅ | ✅ | ❌ |
| **Lazy streaming** | ✅ (lockstep tee) | ❌ (DataFrame-centric) | ❌ (task-based) |
| **Scope** | In-process micro-orchestration | Feature engineering | Cluster orchestration |
| **DAG export** | ✅ (JSON) | ✅ | ✅ |

## Next Steps

- Read the [Installation guide](getting-started/installation.md)
- Follow the [Step-by-step tutorial](tutorial/hello-world.md)
- Explore [Core Concepts](core-concepts/dag-construction.md)
