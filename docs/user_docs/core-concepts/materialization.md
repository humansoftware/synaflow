# Materialization & Error Policies

SynaFlow streams lazily by default, but gives you precise control over when data
is materialized and how errors are handled.

## Forcing Materialization

Use `force_materialize=True` on a step to materialize its output **regardless of
what any consumer asks for**.

=== "Sync"

    ```python
    from collections.abc import Generator, Iterator
    from synaflow import pipeline, step, run

    def producer() -> Generator[int, None, None]:
        yield from range(1_000_000)

    def lazy(producer: Iterator[int]) -> None:      # streams — no materialization
        for x in producer:
            pass

    def cache(producer: Iterator[int]) -> Iterator[int]:
        for x in producer:
            yield x

    p = pipeline(
        name="materialize_example",
        params=type("P", (NamedTuple,), {}),
        steps=[
            step("producer", fn=producer),
            step("cache", fn=cache, force_materialize=True),   # ← forces materialization
            step("lazy", fn=lazy),
        ],
    )
    ```

=== "Async"

    ```python
    from collections.abc import AsyncGenerator, AsyncIterator
    from synaflow import pipeline, step, async_run

    async def producer() -> AsyncGenerator[int, None]:
        for i in range(1_000_000):
            yield i

    async def lazy(producer: AsyncIterator[int]) -> None:
        async for x in producer:
            pass

    async def cache(producer: AsyncIterator[int]) -> AsyncIterator[int]:
        async for x in producer:
            yield x

    p = pipeline(
        name="materialize_example",
        params=type("P", (NamedTuple,), {}),
        steps=[
            step("producer", fn=producer),
            step("cache", fn=cache, force_materialize=True),
            step("lazy", fn=lazy),
        ],
    )
    ```

**When to use it:**

- **Debugging** — inspect intermediate data without changing consumer types.
- **Caching** — persist an expensive computation so it's not re-run.
- **Audit logging** — write a snapshot of data at a specific pipeline stage.
- **Side effects** — materialize to trigger a write to disk or database.

## Implicit Materialization

Materialization also happens automatically when:

| Condition | Example |
|---|---|
| Consumer asks for `list[T]`, `set[T]`, `dict[K,V]` | `def fn(data: list[int])` |
| Consumer asks for `tuple[T, ...]` | `def fn(data: tuple[int, ...])` |
| `on_error=STOP` on the producer (see below) | All downstream consumers materialize |

These are **build-time decisions**. When `pipeline(...)` is compiled, SynaFlow
decides whether each producer output must be materialized and resolves the
materializer callable for that producer. If a producer is marked for
materialization, all of its consumers read from the materialized output. The
runtime executors do not re-decide eager vs lazy edges; they follow the
compiled DAG contract.

## Error Policies: `OnError.CONTINUE` vs `OnError.STOP`

Every step has an `on_error` policy that controls what happens when the step's
function raises an exception.

### `OnError.CONTINUE` (default)

The failing item is discarded and the pipeline continues with the next item.

=== "Sync"

    ```python
    from collections.abc import Generator, Iterator
    from synaflow import pipeline, step, run, OnError

    def producer() -> Generator[int, None, None]:
        for i in range(5):
            yield i

    def fragile(producer: int) -> int:
        if producer == 2:
            raise ValueError("item 2 is poison")
        return producer * 10

    def consumer(fragile: Iterator[int]) -> None:
        for x in fragile:
            print(x)

    p = pipeline(
        name="continue_example",
        params=type("P", (NamedTuple,), {}),
        steps=[
            step("producer", fn=producer),
            step("fragile", fn=fragile, on_error=OnError.CONTINUE),
            step("consumer", fn=consumer),
        ],
    )
    run(p, p.params_type())
    # Output: 0, 10, 30, 40  (item 2 skipped)
    ```

=== "Async"

    ```python
    from collections.abc import AsyncGenerator, AsyncIterator
    from synaflow import pipeline, step, async_run, OnError

    async def producer() -> AsyncGenerator[int, None]:
        for i in range(5):
            yield i

    async def fragile(producer: int) -> int:
        if producer == 2:
            raise ValueError("item 2 is poison")
        return producer * 10

    async def consumer(fragile: AsyncIterator[int]) -> None:
        async for x in fragile:
            print(x)

    p = pipeline(
        name="continue_example",
        params=type("P", (NamedTuple,), {}),
        steps=[
            step("producer", fn=producer),
            step("fragile", fn=fragile, on_error=OnError.CONTINUE),
            step("consumer", fn=consumer),
        ],
    )
    async_run(p, p.params_type())
    # Output: 0, 10, 30, 40  (item 2 skipped)
    ```

### `OnError.STOP`

The pipeline halts immediately. All downstream consumers have their inputs
**materialized before execution** so they can inspect the partial data.

```python
step("fragile", fn=fragile, on_error=OnError.STOP)
```

When `on_error=STOP` is set:

1. This producer is marked to materialize its output before publication.
2. All downstream consumers read from that materialized output.
3. If the step fails partway through, consumers receive the partial data that
   was successfully produced.
4. A `PipelineStopException` is raised with `step_name` and `cause`.

This guarantees transactional integrity — you can inspect what was processed
before the failure.

In other words, `OnError.STOP` changes the compiled materialization plan, not
just the runtime error behavior.

## Error Materializers

When a step fails, an **error materializer** captures the exception and
partial output. Configure per-step or per-pipeline:

```python
from synaflow import disk_error_materializer, log_error_materializer

# Per-pipeline default
p = pipeline(
    name="robust",
    params=Params,
    steps=[...],
    error_materializer_factory=disk_error_materializer("/tmp/errors"),
)

# Per-step override
step("critical", fn=do_work,
     error_materializer=log_error_materializer)
```

## Summary

| Mechanism | When it triggers | Effect |
|---|---|---|
| Consumer type: `list[T]` | Always (build-time) | Marks the producer output for materialization |
| `force_materialize=True` | Always (build-time) | Marks the producer output for materialization |
| `on_error=STOP` | Build-time rule | Marks the producer output for materialization |
| `on_error=CONTINUE` | Runtime (per item) | Skips failed item, pipeline keeps running |
| Error materializer | Runtime (per failure) | Captures exception + partial data |
