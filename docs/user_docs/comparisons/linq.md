# SynaFlow & LINQ

[LINQ](https://learn.microsoft.com/en-us/dotnet/csharp/linq/) (Language
Integrated Query) is .NET's functional streaming API. Like Java Streams and
SynaFlow, it chains transformations over data — `Select`, `Where`, `GroupBy`,
`ToDictionary`. The pattern is universal: **MapReduce on typed streams**.

## Conceptual mapping

| LINQ | SynaFlow |
|---|---|
| `source.Select(x => f(x))` | EACH mode: consumer `(T) → U` over producer `Iterator[T]` |
| `source.Where(x => pred(x))` | EACH mode with conditional `yield` |
| `source.ToList()` | Consumer `list[T]` triggers materialization |
| `source.ToDictionary(...)` | Consumer `dict[K,V]` from `Iterator[tuple[K,V]]` |
| `source.GroupBy(x => key)` | EACH mode → dict materializer (MapReduce shuffle) |
| `source.Aggregate(seed, fn)` | ALL mode with accumulator |
| `source.SelectMany(x => col)` | EACH mode yielding multiple items — implicitly flattened |
| Custom `IEnumerable<T>` pipeline | `Generator[T]` → `Iterator[T]` chain |

## Side-by-side example

=== "C# (LINQ)"

    ```csharp
    var result = Enumerable.Range(0, 10)
        .Select(n => n * 2)
        .Where(n => n > 5)
        .ToList();
    // result = [6, 8, 10, 12, 14, 16, 18]
    ```

=== "SynaFlow (Sync)"

    ```python
    from collections.abc import Generator, Iterator
    from synaflow import pipeline, step, run, PipelineRegistry


    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)


    def doubler(producer: int) -> int:  # EACH: Select
        return producer * 2


    def big_enough(doubler: int) -> int:  # EACH: Where (with yield)
        if doubler > 5:
            yield doubler


    def collector(big_enough: list[int]) -> None:  # ToList
        print(big_enough)


    p = pipeline(
        name="linq_demo",
        params=type("P", (NamedTuple,), {"count": 10}),
        steps=[
            step("producer", fn=producer),
            step("doubler", fn=doubler),
            step("big_enough", fn=big_enough),
            step("collector", fn=collector),
        ],
    )
    catalog = PipelineRegistry()
    catalog.add(p)
    run(catalog.get_dag("linq_demo"), p.params_type()(count=10))
    # Output: [6, 8, 10, 12, 14, 16, 18]
```

## Key differences

| | LINQ | SynaFlow |
|---|---|---|
| **Execution** | Deferred (lazy) by default | Lazy by default |
| **Auto wiring** | Explicit method chaining | Type hints wire DAG automatically |
| **Parallelism** | `.AsParallel()` / PLINQ | Sync/async parity, custom runners |
| **Persistence** | In-memory only | Disk, S3, Redis, DB via materializers |
| **Smart binding** | ❌ | ✅ singular/plural/suffix resolution |
| **Multi-consumer** | Single pipeline, single consumer | Auto `tee` for multiple consumers in lockstep, with bounded `max_in_flight` when needed |
| **Where it runs** | .NET CLR | Single Python process (or export to Airflow/Prefect) |

## Deferred execution

Both LINQ and SynaFlow use **lazy evaluation** — nothing runs until you consume
the result (`.ToList()` in LINQ, a terminal step or `run()` in SynaFlow). This
is the foundation for memory efficiency and composability.

```csharp
// LINQ: nothing executes yet
var query = numbers.Where(n => n > 5).Select(n => n * 2);
// Executes now
var result = query.ToList();
```

```python
# SynaFlow: pipeline is defined, not executed
p = pipeline(...)
# Executes now
run(catalog.get_dag("linq_demo"), params)
```

## Grouping & aggregation

=== "LINQ"

    ```csharp
    var groups = items
        .GroupBy(i => i.Category)
        .ToDictionary(g => g.Key, g => g.Sum(i => i.Value));
    ```

=== "SynaFlow"

    ```python
    def group_by_category(items: Iterator[Item]) -> dict[str, int]:
        result = {}
        for item in items:
            result[item.category] = result.get(item.category, 0) + item.value
        return result
```

In SynaFlow, `GroupBy` is just a step that accumulates in a local dict and
returns it. No special grouping operator needed — plain Python is the query
language.

## When SynaFlow goes further

### Persistence

LINQ streams are in-memory by design. SynaFlow's materializers let
`ToList()` / `ToDictionary()` target **disk, S3, Redis, or any backend**
without changing the consumer code. See
[Java Streams comparison](java-streams.md) for more on the
protocol-over-concrete-type design.

### Bounded streams (vs. standard LINQ / TPL)

Standard LINQ (`IEnumerable`) executes purely pull-based in lockstep on a single thread. It has no concept of a "bounded ahead" buffer between streaming steps.

In C#, when you need to let a producer task run ahead of a consumer task up to a specific limit (e.g. for I/O-bound operations), you typically transition to:
*   **`System.Threading.Channels`** (using a bounded channel `Channel.CreateBounded<T>(N)`).
*   **TPL Dataflow blocks** (using `BoundedCapacity` on blocks like `TransformBlock`).

SynaFlow embeds this capability directly into your DAG definition via `max_in_flight=N` on the step without requiring you to write channel or queue plumbing.
