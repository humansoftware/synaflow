# Sync & Async Parity

SynaFlow provides identical semantics for synchronous and asynchronous pipelines.

=== "Sync"

    ```python
    from collections.abc import Generator, Iterator
    from synaflow import pipeline, step, run

    def producer() -> Generator[int, None, None]:
        yield from range(3)

    def consumer(producer: Iterator[int]) -> None:
        for x in producer:
            print(x)

    p = pipeline(
        name="sync_example",
        params=type("P", (NamedTuple,), {}),
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )
    run(p, p.params_type())
    ```

=== "Async"

    ```python
    from collections.abc import AsyncGenerator, AsyncIterator
    from synaflow import pipeline, step, async_run

    async def producer() -> AsyncGenerator[int, None]:
        for i in range(3):
            yield i

    async def consumer(producer: AsyncIterator[int]) -> None:
        async for x in producer:
            print(x)

    p = pipeline(
        name="async_example",
        params=type("P", (NamedTuple,), {}),
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )
    async_run(p, p.params_type())
    ```

## Key Rules

- A pipeline is either **fully sync** or **fully async** — mixing `Iterator` with `async def` raises an error.
- Both engines share the same DAG representation and execution semantics.
- Observers, materializers, and error handling behave identically.
- Async observers are detected via `inspect.isawaitable` and awaited automatically.

## Converting a Sync Pipeline to Async

1. Replace `Generator[T, None, None]` with `AsyncGenerator[T, None]`
2. Replace `Iterator[T]` with `AsyncIterator[T]`
3. Add `async` to all step functions
4. Use `async_run()` instead of `run()`

The pipeline definition and step configuration stay the same.
