# Tutorial — Level 3: Observers

Observers let you monitor pipeline and step lifecycle events — useful for logging, metrics, and tracing.

=== "Sync"

    ```python
    from collections.abc import Generator, Iterator
    from typing import NamedTuple
    from synaflow import pipeline, step, run, Observer

    class Params(NamedTuple):
        count: int = 3

    def gen(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def doubler(gen: int) -> int:
        return gen * 2

    def printer(doubler: Iterator[int]) -> None:
        for x in doubler:
            pass

    def log_events(ctx):
        print(f"[{ctx.step_name}] {ctx.event.value}")

    p = pipeline(
        name="tutorial",
        params=Params,
        steps=[
            step("gen", fn=gen),
            step("doubler", fn=doubler),
            step("printer", fn=printer),
        ],
        observers=[Observer(log_events)],
    )

    run(p, Params(count=3))
    ```

=== "Async"

    ```python
    from collections.abc import AsyncGenerator, AsyncIterator
    from typing import NamedTuple
    from synaflow import pipeline, step, async_run, Observer

    class Params(NamedTuple):
        count: int = 3

    async def gen(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i

    async def doubler(gen: int) -> int:
        return gen * 2

    async def printer(doubler: AsyncIterator[int]) -> None:
        async for x in doubler:
            pass

    async def log_events(ctx):
        print(f"[{ctx.step_name}] {ctx.event.value}")

    p = pipeline(
        name="tutorial",
        params=Params,
        steps=[
            step("gen", fn=gen),
            step("doubler", fn=doubler),
            step("printer", fn=printer),
        ],
        observers=[Observer(log_events)],
    )

    async_run(p, Params(count=3))
    ```

**Output:**

```
[gen] step_started
[gen] step_completed
[doubler] step_started
[doubler] step_completed
[printer] step_started
[printer] step_completed
```

You can also attach observers to individual steps:

```python
step("doubler", fn=doubler, observers=[Observer(my_handler)])
```

## Next

Upgrade your pipeline with persistent storage in [Level 4](materializers.md).
