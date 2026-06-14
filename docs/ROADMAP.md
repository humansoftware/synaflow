# Synaflow Roadmap

This roadmap outlines the planned features and architectural evolutions for the Synaflow framework.

## ✅ Completed

- **DAG Model** — `Dag`/`DagNode` dataclasses with separated `params` and `steps`, `materialized_deps`, `materializer` pre-computed at build time, `each_inputs` helper, resolved `mode` + `each_mode_deps` in the compiled graph and JSON
- **Dict support** — `dict[K,V]` as both producer and consumer; `Iterator[tuple[K,V]]` → `dict`
- **Sub-Pipelines** — `include()` macro expansion with nested pipeline flattening
- **Materializer architecture** — step-level → pipeline-level → global default resolution; `force_materialize`; `identity` for scalars; custom type validation; default factory with protocol detection
- **Error materializer** — `ErrorMaterializeContext` + `log_error_materializer_factory` + pipeline-level config
- **Explicit step mode** — `StepMode.AUTO | EACH | ALL` with build-time validation and DAG-resolved semantics
- **No silent wrapping** — scalar producer cannot feed iterable consumer
- **Executor rewrite** — single-file sync and async executors; `step_output_observers` for test injection; composite key fan-out (no TeeWrapper); zip multi-stream unroll
- **Sync/async parity fixes** — async iteration failure handling, branch-aware materialization context, uneven unroll termination, and preserved valid prefixes under `OnError.CONTINUE`
- **PipelineStopException** — carries `step_name` + `cause` + `raise ... from`
- **Observer/runtime contract coverage** — mixed fan-out, partial stream failure, and explicit-mode corpus specs
- **Observer System** — unified lifecycle observer system for pipeline, step, and materialization lifecycles with sync/async parity, fire-and-forget execution, and compiled resolution in DAG JSON

## 🚧 In Progress / Next

- **Test coverage** — add CI coverage threshold (80%)
- **Documentation/export guidance** — document how downstream orchestrator exporters should consume `mode` and `each_mode_deps` from DAG JSON rather than re-infer behavior

## 📋 Planned

- **Timeouts** — per-step execution timeouts with graceful abort
- **Decorators** — pluggable step decorators for cross-cutting concerns
- **Regex validation** — step name and pipeline name validation patterns
- **CI/CD** — GitHub Pages for docs, coverage reports
