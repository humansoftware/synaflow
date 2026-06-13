# Synaflow Roadmap

This roadmap outlines the planned features and architectural evolutions for the Synaflow framework.

## ✅ Completed

- **DAG Model** — `Dag`/`DagNode` dataclasses with separated `params` and `steps`, `materialized_deps`, `materializer` pre-computed at build time, `each_inputs` helper
- **Dict support** — `dict[K,V]` as both producer and consumer; `Iterator[tuple[K,V]]` → `dict`
- **Sub-Pipelines** — `include()` macro expansion with nested pipeline flattening
- **Materializer architecture** — step-level → pipeline-level → global default resolution; `force_materialize`; `identity` for scalars; custom type validation; default factory with protocol detection
- **Error materializer** — `ErrorMaterializeContext` + `default_error_materializer_factory` + pipeline-level config
- **No silent wrapping** — scalar producer cannot feed iterable consumer
- **Executor rewrite** — single-file sync and async executors; `step_output_observers` for test injection; composite key fan-out (no TeeWrapper); zip multi-stream unroll
- **PipelineStopException** — carries `step_name` + `cause` + `raise ... from`

## 🚧 In Progress / Next

- **Executor DI** — inject dependencies (stream manager, resolver, runner) via constructor instead of hardcoded instantiation
- **Async engine parity** — sync and async executors share the same architecture (done); need per-consumer materialization for async queues
- **Test coverage** — add CI coverage threshold (80%)

## 📋 Planned

- **Error interceptors** — attach error handlers at step or pipeline level; route failed records to sinks, logs, or dead-letter queues
- **Timeouts** — per-step execution timeouts with graceful abort
- **Observability** — telemetry decorators for success/failure/timeout tracking
- **Decorators** — pluggable step decorators for cross-cutting concerns
- **Regex validation** — step name and pipeline name validation patterns
- **CI/CD** — GitHub Pages for docs, coverage reports
