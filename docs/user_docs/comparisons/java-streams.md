# SynaFlow & the Java Streams API

SynaFlow draws direct inspiration from the Java Streams API. If you've written
`stream.map().filter().collect()` in Java, SynaFlow will feel familiar — with one
critical difference: persistence backends.

## Conceptual mapping

| Java Streams | SynaFlow |
|---|---|
| `stream.map(f)` | EACH mode: consumer `(T) → U` over producer `Iterator[T]` |
| `stream.filter(p)` | EACH mode with conditional `yield` |
| `stream.collect(toList())` | Consumer `list[T]` triggers `Iterator[T] → list[T]` |
| `stream.collect(toSet())` | Consumer `set[T]` triggers `Iterator[T] → set[T]` |
| `stream.collect(toMap(k,v))` | Consumer `dict[K,V]` triggers `Iterator[tuple[K,V]] → dict[K,V]` |
| `stream.forEach(f)` | EACH mode terminal step (no downstream consumers) |
| `stream.iterator()` | Lazy consumer `Iterator[T]` |
| `Collector` interface | `MaterializerFactory` + returned `Materializer` callable |
| `stream.flatMap(...)` | EACH mode producing an iterable, implicitly flattened |

## Side-by-side example

=== "Java"

    ```java
    var result = IntStream.range(0, 10)
        .map(n -> n * 2)
        .filter(n -> n > 5)
        .boxed()
        .collect(Collectors.toList());
    // result = [6, 8, 10, 12, 14, 16, 18]
    ```

=== "SynaFlow (Sync)"

    ```python
    from collections.abc import Generator, Iterator
    from synaflow import pipeline, step, run, PipelineRegistry


    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)


    def doubler(producer: int) -> int:  # EACH: map
        return producer * 2


    def big_enough(doubler: int) -> int:  # EACH: filter
        if doubler > 5:
            yield doubler


    def collector(big_enough: list[int]) -> None:  # collect(toList)
        print(big_enough)


    p = pipeline(
        name="streams",
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
    run(catalog.get_dag("streams"), p.params_type()(count=10))
    # Output: [6, 8, 10, 12, 14, 16, 18]
```

=== "SynaFlow (Async)"

    ```python
    from collections.abc import AsyncGenerator, AsyncIterator
    from synaflow import pipeline, step, async_run, PipelineRegistry


    async def producer(count: int) -> AsyncGenerator[int, None]:
        for i in range(count):
            yield i


    async def doubler(producer: int) -> int:
        return producer * 2


    async def big_enough(doubler: int) -> int:
        if doubler > 5:
            yield doubler


    async def collector(big_enough: list[int]) -> None:
        print(big_enough)


    p = pipeline(
        name="streams",
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
    async_run(catalog.get_dag("streams"), p.params_type()(count=10))
```

## No nested streams — by design

In Java, `stream.flatMap(...)` returns a stream, but you can't accidentally nest
streams. SynaFlow takes the same position: **`Iterator[Iterator[T]]` does not
exist in user-facing code.**

When an EACH-mode step produces an iterable, SynaFlow implicitly flattens it.
When it produces a scalar, outputs are auto-collected into `ListType`. Two
consecutive `T → Iterator[T]` steps still produce a flat `Iterator[T]`:

=== "Sync"

    ```python
    from collections.abc import Generator, Iterator


    def step1() -> Generator[int, None, None]:
        yield from range(3)  # produces Iterator[int]


    def step2(step1: int) -> Generator[str, None, None]:
        yield str(step1)  # EACH mode, produces Generator[str]
        yield str(step1 * 10)  #


    # step2's output is ListType(str) → flat list of strings
    # NOT Iterator[Iterator[str]]
```

=== "Async"

    ```python
    from collections.abc import AsyncGenerator, AsyncIterator


    async def step1() -> AsyncGenerator[int, None]:
        for i in range(3):
            yield i


    async def step2(step1: int) -> AsyncGenerator[str, None]:
        yield str(step1)
        yield str(step1 * 10)
```

This is deliberate. Every SynaFlow pipeline operates on **flat, typed streams**.
There is no use case that requires nested iterators — if you need grouping, use
a `dict` materializer. If you need lateral expansion, yield multiple items from
an EACH step (as shown above). The framework handles flattening transparently.

## The key difference: where data lives

Java Streams are **purely in-memory**. A `Collector` always produces an
in-memory collection (`ArrayList`, `HashSet`, `HashMap`).

SynaFlow's materializer can target **any storage backend**. The consumer only
declares the **protocol** (`list`, `set`, `dict`); the materializer factory
decides the concrete storage:

| Java | SynaFlow |
|---|---|
| `Collectors.toList()` → `ArrayList` in RAM | `list[T]` → `DiskBackedList` on SSD, or S3-backed, or in-memory |
| `Collectors.toSet()` → `HashSet` in RAM | `set[T]` → Redis-backed set, or in-memory |
| `Collectors.toMap(...)` → `HashMap` in RAM | `dict[K,V]` → SQLite with LRU cache, or in-memory |
| Always in-memory | Disk, network, S3, database — any backend with the right protocol |

```python
from synaflow import pipeline, step, run, disk_materializer, PipelineRegistry


# Consumer asks for list[T] — transparently stored on disk
p = pipeline(
    name="big_data",
    params=Params,
    steps=[...],
    memory_materializer_factory=disk_materializer("/mnt/ssd"),
)
catalog = PipelineRegistry()
catalog.add(p)
```

The consumer code doesn't change — it still says `def fn(data: list[int])`.
Whether that list lives in RAM, on SSD, in S3, or in a database is entirely
up to the materializer factory configured at the pipeline root. This is the
core of the **protocol-over-concrete-type** design. The consumer only knows
the **interface** (`MutableSequence` for `list`, `MutableMapping` for `dict`);
the factory decides the **backend**.

## Materializer = Collector (but not only at the end)

| Java Streams Collector | SynaFlow equivalent |
|---|---|
| `Collectors.toList()` | `memory_materializer` (default: returns `list`) |
| `Collectors.toSet()` | Materializer that returns `set` |
| `Collectors.toMap(...)` | Materializer that takes `Iterator[tuple[K,V]]` → `dict[K,V]` |
| `Collectors.groupingBy(...)` | EACH mode → dict materializer (MapReduce shuffle) |
| Custom `Collector<T,A,R>` | Custom `MaterializerFactory` returning `Callable[[Iterator], Iterable]` |

In Java Streams, collectors only apply to **terminal operations** — you collect
at the end. SynaFlow materializers can appear **anywhere in the pipeline**, not
just at the end. A step in the middle can materialize to disk, and downstream
steps read from that persisted data:

```python
step(
    "cache",
    fn=expensive_computation,
    materializer=disk_materializer("/mnt/ssd"),
    force_materialize=True,
)
step("downstream", fn=downstream)  # reads from cached data
```

This enables patterns like checkpointing, intermediate persistence, and
incremental processing that have no equivalent in the Java Streams API.

## When to prefer SynaFlow over Java Streams

- You need **persistence** (disk, database, cloud storage).
- You need **lazy lockstep** consumption across multiple consumers without
  manual `tee` management.
- You need a **bounded ahead window** (like `max_in_flight`) for I/O-bound
  patterns. In Java, this pattern requires moving to Project Reactor, RxJava,
  or JDK 9+ `java.util.concurrent.Flow` to use reactive backpressure / `prefetch`
  buffers. Standard Java Streams are synchronous and lockstep-only.
- You need **sync/async parity** — the same pipeline definition runs in both.
- Your data doesn't fit in memory but you still want the Streams-like API.
