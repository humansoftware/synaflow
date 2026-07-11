# AGENTS.md

Entry point for AI agents working on SynaFlow. Humans should read
`HACKING.md` instead — the canonical contribution workflow lives there.

## Before writing any code, read

1. `HACKING.md` — contribution workflow, dev setup, philosophy, testing patterns, coding style
2. `docs/DESIGN_PHILOSOPHY.md` — what problems SynaFlow solves and why decisions were made
3. `docs/CODING_AND_TESTING_STANDARDS.md` — conventions, naming, test organization
4. `docs/ROADMAP.md` — what's done, what's in progress, what's planned
5. `docs/MATERIALIZATION_RUNTIME_CONTRACT.md` — the accepted builder/runtime split for stream materialization

These documents define the project's technical direction. Every change must be consistent with them.

## Tool-agnostic conventions

Quick-reference rules. The full rationale lives in the docs above.

- **Type hints are 100% mandatory.** SynaFlow routes dependencies via runtime
  type introspection (`inspect.Signature`). Untyped parameters break DAG compilation.
- **Fail fast/fail loud at build time.** Never silently coerce or wrap mismatched types.
- **Sync/async parity is a core invariant.** Any change to
  `synaflow/execution/sync_engine` must be mirrored in `async_engine` and vice versa.
- **Materializer is resolved at build time.** Observer contract is unchanged across changes.
- **DAG JSON is the externalized contract.** It is the public boundary between design-time and runtime.
- **Corpus tests are spec compliance.** `tests/execution/{sync_engine,async_engine}/corpus`
  are never weakened — they are the spec. Fix the implementation instead.

## Standard verification commands

These are the recipes for the standard gates. Run them as you would any other
terminal command — output is what the human reviews.

| Task | Command |
|---|---|
| Full test suite | `uv run pytest tests/ -q` |
| Corpus spec tests | `uv run pytest tests/execution/sync_engine/corpus tests/execution/async_engine/corpus -q` |
| Sync/async parity | `uv run pytest tests/execution/sync_engine/test_runner_materialization.py tests/execution/async_engine/test_async_runner_materialization.py tests/execution/sync_engine/test_runner_max_in_flight.py tests/execution/async_engine/test_async_runner_max_in_flight.py -q` |
| Lint (format + F401) | `uvx ruff format --check --exclude boilerplates/` then `uvx ruff check --select F401 --exclude boilerplates/` |
| Coverage | `uv run pytest --cov=synaflow --cov-report=term -q` |
| Docs build (strict) | `uv run mkdocs build --strict` |

## Agent roles (when multiple agents collaborate)

When multiple agents are working on the same change, each takes one of these roles.

### Build agent

Implements code per the agreed design. Reads `HACKING.md` §3 (philosophy),
§4 (testing patterns), §5 (coding style) before editing. Verifies with
`uv run pytest tests/ -q` after each non-trivial change.

### Plan agent

Plans changes against `docs/DESIGN_PHILOSOPHY.md`,
`docs/CODING_AND_TESTING_STANDARDS.md`, and `docs/ROADMAP.md`. Respects
build/run separation, deep modules, Open/Closed via factories, and the
DAG JSON as the externalized contract. Outputs a step-by-step plan naming
affected files in `synaflow/core`, `synaflow/execution/{sync_engine,async_engine}`,
and the corpus/tests that will assert the new contract.

### Architect agent (read-only)

Validates a proposed diff against SynaFlow's architecture. Reads
`docs/DESIGN_PHILOSOPHY.md`, `docs/CODING_AND_TESTING_STANDARDS.md`,
`docs/ROADMAP.md` and the diff. Checks: KISS/YAGNI/DRY, SOLID, deep
modules, build/run separation, sync/async parity, no silent type
wrapping/coercion, materializer resolved at build time, observer
contract unchanged, DAG JSON contract preserved. Reports violations as a
concrete checklist with `file:line` references. Does not edit files.

### Reviewer agent (read-only)

Reviews the diff for: 100% type hints, Given-When-Then test names, tests
organized by responsibility (`tests/core` vs `tests/execution`), corpus
packs updated when topology changes, observer contract tests added when
observer/laziness behavior changes, no `@staticmethod`-only modules,
module docstrings present. Flags any spec/corpus test weakened instead of
fixed. Does not edit files.

## PR descriptions

Draft PR descriptions in Conventional Commits style
(feat/fix/refactor/test/docs/ci/chore) with three sections: **Summary**,
**Changes**, **Tests**. Match the repo's existing PR style — see recent
merged PRs for examples.
