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

These are **build-time decisions**. When `pipeline(...)` is compiled, SynaFlow
decides whether each producer output must be materialized and resolves the
materializer callable for that producer. If a producer is marked for
materialization, all of its consumers read from the materialized output. The
runtime executors do not re-decide eager vs lazy edges; they follow the
compiled DAG contract.

!!! note "Breaking change (v0.21.0)"
    `on_error=STOP` **no longer forces** the producer to materialize. Previously,
    setting `on_error=STOP` had the side effect of marking the producer for
    materialization so downstream consumers could inspect partial data on
    failure. This side effect is now removed — `on_error=STOP` is purely a
    runtime policy (raise `PipelineStopException` on first error). To get
    partial data visibility, set `force_materialize=True` explicitly.

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

The pipeline halts immediately on the first error and raises a
`PipelineStopException` with `step_name` and `cause`. Lazy consumers simply
stop receiving items (the upstream stream ends); consumers that have already
received items keep what they have.

```python
step("fragile", fn=fragile, on_error=OnError.STOP)
```

!!! warning "Breaking change (v0.21.0)"
    `on_error=STOP` used to mark the producer for materialization so that
    downstream consumers could inspect partial data after a failure. That
    side effect is gone: `on_error=STOP` is now a **runtime-only** policy.
    If you want consumers to receive a materialized snapshot of the partial
    output (including items produced before the failure), set
    `force_materialize=True` on the step explicitly.

    **Before (v0.20.x):** `on_error=STOP` forced materialization — consumers
    could read partial data.

    **After (v0.21.0+):** `on_error=STOP` halts on first error; the stream
    simply ends. To restore the old "inspect partial data" behavior, add
    `force_materialize=True`.

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

Error handlers receive a single error context object with execution metadata
such as `run_id`, `step_name`, `mode`, `on_error`, current success/error
counters, and the captured exception.

## Error Thresholds (v0.21.0)

Steps in `EACH` mode can enforce an error threshold so the pipeline
halts when too many invocations fail, even with `on_error=CONTINUE`.
Two knobs are available:

| Parameter | Meaning |
|---|---|
| `error_threshold_absolute: int` | Fail the step after `N` failed invocations |
| `error_threshold_pct: float` | Fail the step when the failure rate reaches `P` |

When the threshold is exceeded **after all inputs are consumed** (not mid-stream),
a `ThresholdExceededException` is raised. It propagates out of `run()` /
`async_run()` and emits `StepEvent.FAILED` + `PipelineEvent.FAILED` to
configured observers.

```python
from synaflow import pipeline, step

step("fragile", fn=process,
     error_threshold_absolute=5)          # halt on 5th failure
step("brittle", fn=validate,
     error_threshold_pct=0.3)             # halt when 30%+ failed
```

### Constraints

- Only meaningful with `mode=EACH` (the executor must know how many
  invocations occurred). Build-time rejects `mode=ALL` + threshold.
- Cannot be combined with `on_error=STOP` (logical conflict — STOP halts
  on the first error, so the counter can never reach 2).
- Values: `error_threshold_pct` must be in `(0.0, 1.0]`;
  `error_threshold_absolute` must be `>= 1`.

### Escape hatch — manual raise in `ALL`-mode steps

When you manage your own iteration inside an `ALL`-mode step, you can
`raise ThresholdExceededException(...)` manually with the counts you
tracked. The executor treats this as a step failure and emits the same
`FAILED` events. Configure an error materializer if you need those
exceptions logged.

## Summary

| Mechanism | When it triggers | Effect |
|---|---|---|
| Consumer type: `list[T]` | Always (build-time) | Marks the producer output for materialization |
| `force_materialize=True` | Always (build-time) | Marks the producer output for materialization |
| `error_threshold_absolute / pct` | Runtime (after all inputs consumed) | Halts pipeline when threshold exceeded, raises `ThresholdExceededException` |
| `on_error=CONTINUE` | Runtime (per item) | Skips failed item, pipeline keeps running |
| `on_error=STOP` | Runtime (first error) | Halts pipeline, raises `PipelineStopException` |
| Error materializer | Runtime (per failure) | Captures exception + partial data |
