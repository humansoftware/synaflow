# Spec: Export Guidance Documentation

> **NOTE:** Once this specification is fully implemented and the official documentation is updated, this file MUST be deleted.

## Objective
Write comprehensive documentation explaining how downstream exporters (e.g., adapters for Airflow, Dagster, Prefect) should consume the serialized DAG JSON.

## Motivation
SynaFlow is executor-agnostic and resolves complex execution semantics (like `mode` and `each_mode_deps`) at build time. This intelligence is exported in the `pipeline.to_dict()` JSON. Downstream orchestrators must NOT re-infer this behavior; they must strictly follow what is encoded in the DAG JSON to maintain architectural parity.

## Implementation Plan
1. Create a new documentation file or section (e.g., `docs/EXPORT_GUIDANCE.md` or append to an existing one).
2. Clearly explain the fields available in the DAG JSON for each node, especially:
   * `mode` (AUTO, EACH, ALL)
   * `each_mode_deps` (which dependencies are streams being consumed item-by-item)
   * `materializer` and `force_materialize`
3. Provide an example of how a custom runner should read these fields to construct its own graph, ensuring it respects the pre-calculated semantic rules.
4. Emphasize that the DAG JSON is the strict execution contract and orchestrators should not second-guess it.
