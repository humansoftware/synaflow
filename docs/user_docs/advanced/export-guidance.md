# Export Guidance

SynaFlow's DAG JSON is a **strict execution contract** for downstream orchestrators.

## The Contract

Every pipeline exports a deterministic JSON via `pipeline.to_dict()`:

```json
{
  "name": "example",
  "params": {"count": "int"},
  "steps": {
    "producer": {
      "deps": {"count": "int"},
      "output": "Stream[int, None, None]",
      "fn": "producer",
      "on_error": "continue",
      "mode": "all",
      "materializer": "memory_materializer",
      "error_materializer": "log_error_materializer",
      "each_mode_deps": [],
      "pipeline": "example",
      "parent_pipeline": null
    }
  }
}
```

## Field Reference

| Field | Description |
|---|---|
| `deps` | Parameter name → expected type. Dependencies to resolve before executing. |
| `output` | The type this step produces. |
| `mode` | `"all"` (single call with full input) or `"each"` (called per item). |
| `each_mode_deps` | Which deps must be unrolled item-by-item in EACH mode. |
| `on_error` | `"continue"` (skip failed items) or `"stop"` (halt pipeline). |
| `materializer` | Name of the materializer callable. |
| `error_materializer` | Name of the error handler callable. |
| `needs_materialize_reasons` | Optional debug-only reasons explaining why this producer was compiled as materialized. |

## Exporting to Airflow

```python
dag_json = pipeline.to_dict()

# Compile into an Airflow DAG
for step_name, node in dag_json["steps"].items():
    task = PythonOperator(
        task_id=step_name,
        python_callable=node["fn"],
    )
    for dep in node["deps"]:
        task.set_upstream(dag.get_task(dep))
```

## Exporting to Prefect

```python
from prefect import flow, task

@flow
def synaflow_to_prefect(dag_json):
    tasks = {}
    for name, node in dag_json["steps"].items():
        @task(name=name)
        def step_fn():
            ...
        tasks[name] = step_fn

    for name, node in dag_json["steps"].items():
        for dep in node["deps"]:
            tasks[name].set_upstream(tasks[dep])
```

## Key Guarantees

- The JSON is **deterministic** — same pipeline always produces the same structure.
- All **semantic decisions** (mode, each_mode_deps, producer-level materialization) are pre-computed at build time.
- External runners do **not** need to re-infer semantics — they only need to execute the contract.
