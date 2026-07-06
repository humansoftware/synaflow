# Execution Plan Design

## Decision

Shift execution-shape decisions from runtime to design time.

The DAG builder should compile an execution plan that tells the executor:

- whether a step publishes a value or a deferred stream
- whether completion is immediate or tied to stream exhaustion
- whether a deferred step must drain because it is terminal or only feeds barrier-only consumers
- whether publishing uses direct stream handoff, materialization, or fanout

The executor should interpret that plan. It should not reclassify a step output by
looking at the concrete Python object except to validate contract violations.

## Why

Runtime `isinstance(..., Iterator)` checks allow arbitrary objects to redefine
execution semantics. `MagicMock` is the clearest example: a step that should
publish a plain value can be misclassified as a lazy stream because the mock
exposes magic iterator methods dynamically.

That produces deadlocks and hidden behavior changes in the executor. The same
class of bug previously appeared in async/sync context manager detection.

## Compiled Plan

The builder now compiles three debug-visible structures per step:

- `OutputContract`
  - `runtime_kind`: `value`, `sync_stream`, `async_stream`
  - `completion_policy`: `immediate`, `on_exhaustion`
  - `drain_policy`: `none`, `terminal`, `barrier_only`
- `ConsumerContract`
  - `item`, `stream`, `materialized`, `barrier_only`
- `PublishPlan`
  - `publish_value`, `publish_stream`, `publish_materialized`,
    `publish_sync_fanout`, `publish_async_fanout`
  - plus handoff strategy

## Migration Path

1. Compile the execution plan in the DAG builder.
2. Make executors consume the compiled plan for drain/completion/publication.
3. Leave runtime shape checks only as contract validation.
4. Remove remaining runtime reclassification branches (`isinstance(output, Iterator)`
   deciding semantics) once all call sites read the compiled plan.

## Expected Outcome

- better locality: topology bugs are solved in the builder
- smaller executor surface area
- fewer runtime false positives from mocks and dynamic objects
- easier reasoning: the DAG becomes the authoritative execution contract
