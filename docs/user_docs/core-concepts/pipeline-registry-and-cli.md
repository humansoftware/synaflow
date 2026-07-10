# Pipeline Registry & CLI

Once you have more than a couple of pipelines in a project, two questions
start to dominate the day:

- **Where do they live?** A file, a directory, a package?
- **How do I run one from the shell** without writing a driver script every time?

`PipelineRegistry` answers the first. The `synaflow` CLI answers the second.

## The PipelineRegistry class

`PipelineRegistry` is a validated, name-keyed mapping of
`(name → PipelineDef, Dag)`. Adding a pipeline compiles its Dag immediately,
so an imported catalog never contains an invalid pipeline. Adding a root also
registers every pipeline reachable through `include()`.

```python
from typing import NamedTuple

from synaflow import PipelineRegistry, pipeline, step


class HelloParams(NamedTuple):
    x: int = 0


def hello(x: int) -> int:
    return x


hello_pipeline = pipeline(
    name="hello",
    params=HelloParams,
    steps=[step("hello", fn=hello)],
)
catalog = PipelineRegistry()
catalog.add(hello_pipeline)
```

After that:

```python
dag = catalog.get_dag("hello")   # already compiled during catalog.add(...)
dag2 = catalog.get_dag("hello")  # same compiled Dag
```

!!! note
    `catalog[name]` gives you the **`PipelineDef`**.
    `catalog.get_dag(name)` gives you the **compiled `Dag`**.
    A registered definition must not be mutated. `catalog.add(...)` is
    idempotent for the same instance and rejects a different instance with
    the same name.

### Loading a catalog module

`PipelineRegistry.from_module("myproject.pipelines")` imports that module
and returns its `catalog` attribute. The module must expose
`catalog = PipelineRegistry()` at the top level.

This is the convention the CLI uses. Put your pipelines and their explicit
`catalog.add(...)` calls in a single module:

```python title="myproject/pipelines.py"
from typing import NamedTuple
from synaflow import PipelineRegistry, pipeline, step


class DailyParams(NamedTuple):
    x: int = 0


def greet(x: int) -> None:
    print(f"hello, x = {x}")


_greet = pipeline(
    name="greet",
    params=DailyParams,
    steps=[step("greet", fn=greet)],
    exports="status/loaded.json",
)

catalog = PipelineRegistry()
catalog.add(_greet)
```

## The `synaflow` CLI

If you ran `uv add synaflow` (or `pip install synaflow`), the install also
gives you a `synaflow` console script. It is a thin adapter over the public
Python API; you can do everything it does from `python -m synaflow`.

In both cases you point at a catalog module with `--catalog`:

```bash
synaflow --catalog myproject.pipelines list
synaflow --catalog myproject.pipelines info greet
synaflow --catalog myproject.pipelines dag greet
synaflow --catalog myproject.pipelines run greet
```

The CLI dispatches sync vs async automatically based on the
compiled `Dag.requires_async_runner` flag — you don't pick the engine.

### `list` — what's registered?

```bash
$ synaflow --catalog myproject.pipelines list
greet	1 steps
```

`--json` emits JSON instead:

```bash
$ synaflow --catalog myproject.pipelines list --json
[
  {
    "name": "greet",
    "steps": 1
  }
]
```

### `info` — the declared shape (no compilation)

```bash
$ synaflow --catalog myproject.pipelines info greet
name: greet
params: DailyParams
exports: status/loaded.json
steps (1): greet
```

`info` only reads the `PipelineDef`; it never calls `build_dag`. The
`steps (1)` count is the *declared* step count, before any sub-pipeline
includes are expanded.

`--json` returns the same fields as a JSON object — useful for tooling.

### `dag` — the compiled Dag, as JSON

```bash
$ synaflow --catalog myproject.pipelines dag greet
{
  "name": "greet",
  "steps": [
    {
      "name": "greet",
      "fn": "greet",
      ...
    }
  ]
}
```

This **does** compile the `Dag` and prints whatever shape
`Dag.to_dict()` produces. Use it for debugging or for piping the compiled
structure into another tool.

### `run` — execute a pipeline

```bash
# All default params (works when every field has a default).
synaflow --catalog myproject.pipelines run greet

# From a JSON file (object with one key per params field).
synaflow --catalog myproject.pipelines run greet --params-file params.json

# Direct flags are generated from the pipeline's Params fields.
# --x overrides values from --params-file.
synaflow --catalog myproject.pipelines run greet --x 99
synaflow --catalog myproject.pipelines run greet --params-file p.json --x 99
```

Each params field becomes a kebab-case flag: `initial_date` becomes
`--initial-date`. Values are parsed as JSON when possible, otherwise kept as
strings (`--x 42` → int, `--name '"alice"'` → str, `--raw hello` → str).

`--param key=value` remains available for compatibility with v0.28.0, but new
scripts should prefer direct flags. If both forms provide the same field, the
direct flag wins.

Unknown fields and missing required fields are reported as `synaflow:
Unknown params field(s) for DailyParams: [...]` etc. — not as a stack trace.

## End-to-end: a real catalog in 30 seconds

```python title="myproject/pipelines.py"
from typing import NamedTuple
from synaflow import PipelineRegistry, pipeline, step


class DailyParams(NamedTuple):
    x: int = 0
    label: str = "default"


def ingest(x: int) -> int:
    print(f"ingest: x = {x}")
    return x


def transform(x: int, label: str) -> dict:
    return {"label": label, "value": x * 2}


_ingest = pipeline(
    name="daily_ingest",
    params=DailyParams,
    steps=[step("ingest", fn=ingest), step("transform", fn=transform)],
    exports="status/loaded.json",
)


catalog = PipelineRegistry()
catalog.add(_ingest)
```

```bash
$ synaflow --catalog myproject.pipelines list
daily_ingest	2 steps

$ synaflow --catalog myproject.pipelines info daily_ingest
name: daily_ingest
params: DailyParams
exports: status/loaded.json
steps (2): ingest, transform

$ synaflow --catalog myproject.pipelines run daily_ingest \
    --x 7 --label '"first run"'
ingest: x = 7
$ echo $?
0
```

The catalog module is the single source of truth — Python code,
the CLI, and any future automation read from the same `PipelineRegistry`.
