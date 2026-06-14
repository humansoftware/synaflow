# Custom Observers

Observers receive lifecycle events from pipelines, steps, and materializers.

## Observer API

```python
from synaflow import Observer

def my_handler(ctx):
    print(f"[{ctx.step_name}] {ctx.event.value}")

p = pipeline(
    observers=[Observer(my_handler)],
    ...
)
```

## Event Types

### Pipeline Events

| Event | When |
|---|---|
| `PipelineEvent.STARTED` | Pipeline execution begins |
| `PipelineEvent.COMPLETED` | All steps finished successfully |
| `PipelineEvent.FAILED` | Pipeline stopped due to error |

### Step Events

| Event | Context fields |
|---|---|
| `StepEvent.STARTED` | `step_name`, `mode` |
| `StepEvent.COMPLETED` | `step_name`, `success_count`, `error_count` |
| `StepEvent.FAILED` | `step_name`, `exception` |

### Materialization Events

| Event | When |
|---|---|
| `MaterializationEvent.STARTED` | Materializer invoked |
| `MaterializationEvent.COMPLETED` | Materialization finished |
| `MaterializationEvent.FAILED` | Materializer raised exception |

## Per-Step Observers

Attach observers to individual steps for targeted monitoring:

```python
step("critical_step", fn=do_work,
     observers=[Observer(alert_if_slow)])
```

## Async Observers

Async handlers are detected automatically and awaited:

```python
async def async_handler(ctx):
    await metrics.push(ctx.step_name, ctx.event.value)

p = pipeline(observers=[Observer(async_handler)])
```

## Failure Isolation

Observer failures are **logged and swallowed** — they never affect pipeline execution. If an observer raises, the pipeline continues normally.
