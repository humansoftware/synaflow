# Tutorial — Level 4: Materializers

Materializers control how data is collected and stored. By default, SynaFlow streams data lazily, but you can materialize to `list`, `dict`, or even disk.

## Default: Lazy Streaming

When a consumer asks for `Iterator[T]`, no materialization happens:

```python
def consumer(producer: Iterator[int]) -> None:
    for x in producer:   # streams one item at a time
        pass
```

## Eager: list, set, dict

When a consumer asks for `list[T]`, SynaFlow automatically collects the stream:

=== "Sync"

    ```python
    from collections.abc import Generator
    from synaflow import pipeline, step, run

    def producer() -> Generator[int, None, None]:
        yield from range(100)

    def consumer(producer: list[int]) -> None:
        print(len(producer))  # 100 — all items in memory

    p = pipeline(
        name="eager",
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
    from collections.abc import AsyncGenerator
    from synaflow import pipeline, step, async_run

    async def producer() -> AsyncGenerator[int, None]:
        for i in range(100):
            yield i

    async def consumer(producer: list[int]) -> None:
        print(len(producer))  # 100

    p = pipeline(
        name="eager",
        params=type("P", (NamedTuple,), {}),
        steps=[
            step("producer", fn=producer),
            step("consumer", fn=consumer),
        ],
    )
    async_run(p, p.params_type())
    ```

## Custom Materializer: Write to Disk

You can replace the default in-memory materializer with a disk-backed one:

```python
from synaflow import pipeline, step, run, disk_materializer

p = pipeline(
    name="disk_pipeline",
    params=Params,
    steps=[
        step("producer", fn=producer),
        step("consumer", fn=consumer,
             materializer=disk_materializer(base_dir="/tmp/data")),
    ],
)
```

## Force Materialization

Use `force_materialize=True` to materialize a step's output regardless of what consumers ask for:

```python
step("cache", fn=expensive_computation, force_materialize=True)
```

## Next

Dive deeper into [Core Concepts](../core-concepts/dag-construction.md).
