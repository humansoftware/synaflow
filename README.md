# SynaFlow

SynaFlow is a lightweight, pure-Python pipeline engine that uses Type Hints to magically wire and execute Directed Acyclic Graphs (DAGs).

## Features

- **No Boilerplate:** Uses standard Python Type Hints (`inspect.signature`) to build the dependency graph.
- **Generator Lockstep Execution:** Efficiently streams data using native Python generators, ensuring low memory consumption.
- **Auto-Materialization:** Automatically infers when a collection needs to be materialized into a list or evaluated lazily as an iterator.

## Quickstart

```python
from typing import NamedTuple
from collections.abc import Generator, Iterator
from synaflow import pipeline, step, run

class MyParams(NamedTuple):
    count: int

def producer(count: int) -> Generator[int, None, None]:
    yield from range(count)

def transformer(producer: int) -> int:
    return producer * 10

def consumer(transformer: Iterator[int]) -> None:
    for x in transformer:
        print(x)

# The DAG is auto-wired!
my_pipeline = pipeline(
    name="example",
    params=MyParams,
    steps=[
        step("producer", fn=producer),
        step("transformer", fn=transformer),
        step("consumer", fn=consumer)
    ]
)

# Run it
run(my_pipeline, MyParams(count=5))
```

## License
MIT
