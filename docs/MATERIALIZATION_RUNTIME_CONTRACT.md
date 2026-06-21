# Materialization Runtime Contract

This document exists to prevent future regressions in the DAG builder and
executors.

## Core Rule

Materialization is decided once, during DAG construction, at the **producer**
level.

That means:

- `Dag.needs_materialize(step_name)` answers the only runtime question that
  matters for stream publication.
- If producer `P` needs materialization, **all** consumers of `P` must read from
  the materialized output.
- Executors must not try to recompute edge-level decisions such as "this
  consumer is eager but that one is lazy".

## Why This Design Was Chosen

Previous implementations spread the logic across the DAG builder and the
executors. That made the system harder to reason about and easy to break,
because runtime code started re-deriving rules that were already known at build
time.

The accepted simplification is:

1. The builder computes whether each producer output must be materialized.
2. The executors only branch on that compiled flag.
3. Any consumer-facing detail kept in the DAG is diagnostic data, not runtime
   policy.

## Meaning of the DAG Fields

- `DagNode.materialize_output`
  The compiled runtime contract for a producer. Executors should use this.

- `DagNode._materialized_deps`
  Private, consumer-side diagnostic view derived from the producer flags. It is
  useful for JSON snapshots and inspection, but executors should not depend on
  it for behavior.

## Current Rules That Force Producer Materialization

A producer becomes materialized when any of these is true:

- The producer has `on_error=STOP`.
- The producer has `force_materialize=True`.
- Any consumer requests a materialized type such as `list[T]`.
- Any consumer has multiple lazy stream dependencies that would otherwise be
  consumed concurrently.
- A downstream `force_materialize=True` step propagates that requirement to its
  producer dependencies.
- A stream producer that is already marked for materialization propagates that
  requirement to its own lazy upstream stream dependencies.

## Refactor Guidance

When changing this area:

- start from `synaflow/core/dag_builder.py`
- treat `_plan_materialization(...)` as the single source of truth
- avoid adding eager/lazy policy logic back into the executors
- if a new rule is needed, prefer expressing it as "producer X now needs
  materialization" instead of creating per-edge runtime behavior
