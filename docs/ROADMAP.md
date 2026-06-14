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
- **Lifecycle observer system** — `Observer`, `PipelineEvent`/`StepEvent`/`MaterializationEvent`, per-event context dataclasses, sync/async dispatch, build-time normalization, DAG JSON metadata, fire-and-forget with failure isolation
- **Test Coverage CI** — Dual metrics (Total vs Patch coverage), 80% patch threshold, non-blocking GitHub status checks, and pre-commit hooks
- **Semantic Naming & Smart Binding** — Base Dataset name normalization via `inflect`, smart binding for dependency resolution, duplicate dataset/param detection, `binding_map` in DAG JSON

## 🚧 In Progress / Next

- **Documentation Portal** — Build a comprehensive MkDocs-Material site with step-by-step tutorials, sync/async code tabs, export guidance for downstream orchestrators, and auto-generated Mermaid DAG visualizations.

## 📋 Planned

- **Timeouts** — per-step execution timeouts with graceful abort
