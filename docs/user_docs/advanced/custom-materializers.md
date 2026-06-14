# Custom Materializers

Materializers control how stream outputs are collected. You can write custom ones for any storage backend.

## Default Materializers

| Consumer type | Default behavior |
|---|---|
| `Iterator[T]` | No materialization (lazy stream) |
| `list[T]` | `list()` — all items in memory |
| `set[T]` | `set()` — all items in memory |
| `dict[K,V]` | `dict()` — key-value pairs from `Iterator[tuple[K,V]]` |

## Writing a Custom Materializer

A materializer is a callable `(Iterator[T]) -> Iterable[T]`:

```python
from collections.abc import Iterator

def count_materializer(stream: Iterator[int]) -> list[int]:
    """Materialize and log the count."""
    result = list(stream)
    print(f"Materialized {len(result)} items")
    return result

step("consumer", fn=consumer, materializer=count_materializer)
```

## Materializer Factories

For context-aware materialization (e.g., file paths based on pipeline/step names), use a **factory**:

```python
from synaflow.types import MaterializeContext

def disk_factory(ctx: MaterializeContext) -> callable:
    path = f"/data/{ctx.pipeline_name}/{ctx.dataset_name}.json"
    def disk_materializer(stream):
        data = list(stream)
        with open(path, "w") as f:
            json.dump(data, f)
        return data
    return disk_materializer

p = pipeline(
    name="my_pipeline",
    params=Params,
    steps=[...],
    memory_materializer_factory=disk_factory,
)
```

The `MaterializeContext` provides:

| Field | Description |
|---|---|
| `pipeline_name` | Name of the current pipeline |
| `dataset_name` | Name of the step producing the data |
| `item_type` | The type of items in the stream |

## Error Materializers

When a step fails, error materializers capture the exception and partial output:

```python
from synaflow import log_error_materializer, disk_error_materializer

step("processor", fn=process,
     error_materializer=disk_error_materializer("/tmp/errors"))
```
