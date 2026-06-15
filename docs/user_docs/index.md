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

| | SynaFlow | Hamilton | Flyte / Metaflow | Dagster / Prefect | Airflow |
|---|---|---|---|---|---|
| **Auto wiring** | ✅ type hints + smart binding | ✅ type hints (exact names) | ❌ explicit `A >> B` | ❌ explicit | ❌ explicit |
| **Lazy streaming** | ✅ lockstep tee | ❌ DataFrame-centric | ❌ | ❌ | ❌ |
| **Smart binding** | ✅ singular/plural/suffix | ❌ | ❌ | ❌ | ❌ |
| **Scope** | In-process micro | Feature engineering | Task orchestration | Asset/workflow orchestration | DAG scheduling |
| **DAG export** | ✅ JSON | ✅ | ✅ | ✅ | ✅ |
| **Sync/async parity** | ✅ identical | ❌ | ✅ | ✅ | ❌ |
| **Memory model** | One item per step | Full DataFrame | Task I/O boundary | Task I/O boundary | Task I/O boundary |

**SynaFlow** is not a replacement for Airflow or Dagster — it's a
**micro-orchestrator** that runs *inside* a single Python process. Use those tools
to schedule jobs; use SynaFlow inside the job to route and stream millions of rows
between Python functions with zero boilerplate.

But because SynaFlow strictly separates **build-time** (DAG compilation) from
**run-time** (execution), you are free to write your own runner. The DAG JSON is a
deterministic execution contract — you can compile a SynaFlow pipeline and
auto-generate a native DAG for **Airflow**, **Dagster**, or **Prefect**, running
the same business logic at cluster scale without changing a line of user code.
See [Export Guidance](advanced/export-guidance.md) for details.

For a deeper comparison with Hamilton, see the
[Design Philosophy](https://github.com/humansoftware/synaflow/blob/main/docs/DESIGN_PHILOSOPHY.md#25-detailed-comparison-synaflow-vs-hamilton).

## Next Steps

- Read the [Installation guide](getting-started/installation.md)
- Follow the [Step-by-step tutorial](tutorial/hello-world.md)
- Explore [Core Concepts](core-concepts/dag-construction.md)
