# Coding and Testing Standards

## 1. Production Code

### 1.1. Functions Over Stateless Classes
If something can be a set of functions in a module, it should be. Classes are reserved for when behavior is tied to instance state. A module with `@staticmethod` or `@classmethod` only is a code smell — prefer plain functions.

Example: `build_dag()` is a module-level function, not `DagBuilder.build()`.

### 1.2. Pythonic Code (Fluent Python)
Write Python that looks like Python, not Java or C++ in disguise.

- **Iterables and generators first**: prefer `yield`, `itertools`, comprehensions over manual index loops.
- **Protocols over inheritance**: use `collections.abc` (`Sequence`, `MutableMapping`) to check capabilities, not concrete types.
- **First-class functions**: pass callables, use closures and partials where they simplify the API. Functions are objects — treat them as such.
- **Data classes for data, classes for behavior**: `@dataclass` for plain data structures; classes with `__init__` logic for stateful objects.
- **Context managers and decorators**: use `with` and `@` to wrap cross-cutting concerns cleanly.

### 1.3. Pragmatic Thinking (The Pragmatic Programmer)
- **DRY is about knowledge, not code**: duplication of intent and understanding is the real enemy. Two pieces of code that happen to look similar but encode different knowledge are not duplication.
- **Orthogonality**: unrelated things should be independent. A change in one module should not ripple through unrelated modules.
- **Tracer bullets**: build a thin but functional vertical slice to get real feedback early, then flesh it out.
- **Prototype to learn, not to ship**: write throwaway code to explore unknowns, then throw it away and build properly.
- **Don't live with broken windows**: fix bad design, wrong decisions, and poor code as soon as you notice them.

### 1.4. Complexity Management (A Philosophy of Software Design)
- **Deep modules**: modules that do a lot with simple interfaces. The best modules are deep: they have a small, clean surface area hiding significant implementation complexity.
- **Information hiding**: every module should hide as much as possible. The interface should expose only what the caller truly needs to know.
- **Strategic vs. tactical programming**: invest in good design. Working code is not enough. The goal is working code with a design that lets you keep making progress.
- **Design it twice**: for anything non-trivial, sketch at least two designs before committing. Different approaches reveal different trade-offs.
- **Complexity is incremental**: it doesn't come from one big bad decision; it comes from hundreds of small compromises. Fight each one.

### 1.5. Clean Code, Python Edition (Clean Code in Python)
- **Zen of Python meets SOLID**: the principles of `import this` — explicit is better than implicit, simple is better than complex — are Python's native expression of SOLID.
- **Refactor over-engineered code**: when you catch yourself building for scenarios that might never happen (YAGNI violation), simplify aggressively. Use Python's dynamic features to eliminate boilerplate without losing clarity.
- **Extract without over-abstracting**: when two pieces of code share logic (DRY), extract a function. When three share it, extract a module. But don't pre-emptively abstract — wait for the pattern to emerge.
- **SOLID**:
  - *Single Responsibility*: each module/class/function has one reason to change.
  - *Open/Closed*: open for extension (inject dependencies, provide factories), closed for modification.
  - *Liskov Substitution*: subtypes must be substitutable for their base types without breaking behavior.
  - *Interface Segregation*: no massive interfaces. Users implement only what they need.
  - *Dependency Inversion*: depend on abstractions (protocols), not concrete implementations.
- **Type hints are documentation**: annotate signatures. They communicate intent and enable static analysis.
- **Meaningful names**: a name should answer why it exists, what it does, and how it's used. No abbreviations unless universally understood.
- **Small functions**: a function does one thing, at one level of abstraction. If you need comments to separate sections, extract functions.
- **Composition over inheritance**: share behavior through composed objects, not deep class hierarchies.

### 1.6. Domain-Driven Design with TDD (Cosmic Python / Architecture Patterns)
The core business logic must be isolated from infrastructure.

- **Dependency Inversion**: business rules never import I/O, databases, network, or framework code. Infrastructure depends on the domain, not the other way around.
- **Repository and Unit of Work**: abstract data access behind interfaces. The domain calls `repo.get(id)`, not `db.query(...)`.
- **Message Bus**: orchestrate use cases through a central bus so side effects (logging, events) are composed, not hardcoded.
- **TDD workflow**: red (write a failing test) → green (make it pass minimally) → refactor. Tests drive design, not verification after the fact.
- **Test at the right level**: unit tests for domain logic, integration tests for adapters, end-to-end sparingly.

### 1.7. YAGNI, DRY, KISS
- **YAGNI**: don't build abstraction before you have at least two concrete use cases.
- **DRY**: reuse through composition, not inheritance. Duplication is cheaper than the wrong abstraction.
- **KISS**: prefer straightforward solutions. Fancy patterns only when justified by real complexity.

### 1.8. Error Handling
- **Fail fast, fail loud**: validation errors at build time. Don't silently coerce types or swallow exceptions.
- **Internal errors propagate**: the framework raises clear, descriptive exceptions. It never catches and hides internal bugs.
- **User-facing errors**: use `ValueError` for invalid configuration, `RuntimeError` for engine mismatches.

### 1.9. Imports at Module Top
All imports go at the top of the module. Lazy imports inside functions are only acceptable to break verified circular dependencies. Inter-module dependency cycles should be resolved through design, not hidden by deferred imports. Module-level imports make dependencies explicit and enable static analysis.

### 1.10. Module Docstrings
Every module starts with a `"""` docstring that explains:
- **What** the module is responsible for (its single purpose)
- **How** it achieves it (key functions, classes, or patterns)
- **Why** it exists separately (what makes it a distinct concern)

Keep it concise — a paragraph is usually enough. The docstring is the first thing in the file, before imports.

## 2. Tests

### 2.1. Test Naming: Given-When-Then
Test names follow the `test_given_X_when_Y_then_Z` convention:

```python
def test_given_iterator_of_pairs_when_consumer_wants_dict_then_dag_builds(): ...
```

This makes test intent self-documenting and failures immediately understandable.

### 2.2. Test Organization
Tests are organized by **what is being tested**, not by the topology used. Each test module covers a single responsibility:

```
tests/core/
├── test_dag_builder_compatibility.py   — type pair compatibility table
├── test_dag_builder_materializer.py    — materializer resolution
├── test_dag_builder_materialized_deps.py — materialized dependency computation
├── test_dag_builder_validation.py      — error cases and edge conditions
```

### 2.3. Test Data Modules
Reusable test data lives in `_`-prefixed modules (e.g., `_dag_builder_data.py`) that pytest does not collect. Fixtures and helper factories go in `conftest.py`.

### 2.4. Design-Time vs Run-Time Tests
Tests are separated by concern:
- **Design-time (build)**: test DAG compilation, validation, type compatibility. Lives under `tests/core/`.
- **Run-time**: test pipeline execution, materialization, error handling. Lives under `tests/execution/`.

Whenever a semantic decision is compiled into the DAG (`mode`, `each_mode_deps`, `materialized_deps`, serialized JSON shape), prefer asserting the compiled DAG directly in core tests instead of letting execution tests rediscover the same rule indirectly.

### 2.5. Shared Corpus (Specification Compliance Tests)

Pipelines with identical topology but different execution modes (sync vs async) share a corpus pattern. Each engine directory has its own corpus files, but the topology and expected DAG structure are identical. Tests iterate over these corpus packs to validate both engines.

These are **specification compliance tests**. Each pack is a specification: "given this topology and these inputs, the framework MUST produce this DAG structure and these outputs." The tests verify that the entire stack — from `build_dag` through `PipelineExecutor` — honors that specification.

They are not unit tests (they cross the entire stack), nor are they strictly integration tests (they don't verify that two specific modules work together — they verify that the implementation as a whole satisfies a predefined contract). A pack change is a spec change; a test failure means the implementation no longer meets the spec.

A corpus pack bundles the specification:

```python
pack = PipelinePack(
    pipeline=pipeline_def,          # the compiled DAG
    input_params=Params(...),       # input values
    step_results={...},             # expected output per step
    json_dag={...},                 # expected serialized DAG structure
    expected_execution_levels=[...],# expected topological levels
)
```

Tests consume these packs at different levels, each verifying a different part of the spec:
- `test_corpus_dag` — every pack compiles without crashing (spec: valid topologies compile)
- `test_corpus_execution_levels` — every pack's DAG structure matches `json_dag` and produces the correct execution levels (spec: DAG shape is deterministic)
- `test_step_results` — every pack's pipeline executes and produces the expected step outputs (spec: runtime produces correct results)

The serialized DAG spec is expected to include resolved execution decisions, not just topology. If the framework resolves `mode`, `each_mode_deps`, or similar semantics at build time, corpus JSON should freeze them as part of the public contract.

When a module changes, its unit tests verify the module in isolation. These tests verify that the change didn't violate the spec — that the contract between the framework and its users is still intact.

### 2.6. Future Features (xfail)
Features not yet implemented are documented as `@pytest.mark.xfail` tests in `test_*_future.py` modules. These serve as living specifications. When the feature is implemented, the `xfail` marker is removed.

### 2.7. Test Doubles
- **Mocks**: use `unittest.mock` for verifying interactions (was this called? with what args?).
- **Stubs**: provide canned answers to calls made during the test.
- **Fakes**: lightweight implementations that work but are unsuitable for production (e.g., in-memory repository).
- Prefer fakes over mocks when the interface is stable and the fake is simple to write.

### 2.8. Observer Contract Tests
`step_output_observers` are not incidental debug hooks. They are part of the runtime contract and should be tested explicitly when behavior depends on:
- stream laziness vs materialization
- mixed fan-out
- partial stream failure under `OnError.CONTINUE`

If a regression would change what an observer sees, add or update a dedicated runtime test instead of relying only on step-result assertions.
