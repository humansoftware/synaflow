# Hacking on SynaFlow

Thank you for your interest in contributing to **SynaFlow**!
This document serves as the compass for anyone looking to build, debug, or extend the framework. We take software engineering principles, clean code, and architectural boundaries very seriously.

## 1. How to Contribute

To prevent wasted effort, we follow an **Issue-First** contribution model:

1. **Open an Issue:** Before writing a single line of code, open an Issue on GitHub.
   - **For Bugs:** Provide a clear reproducible example (a minimal DAG that fails).
   - **For Features:** Explain the use case that is currently impossible or poorly handled by the framework. Provide a mock example of how the code *should* look.
2. **Negotiate the Solution:** Maintainers and the community will discuss the architectural impact of your proposal. We will align on *how* the change should be made.
3. **Open a PR:** Once the design is approved, you are welcome to fork the repository, write the code, and submit a Pull Request.

## 2. Parallel-Agent Worktrees

Use one worktree and one branch per parallel agent. Keep the primary
checkout for coordination; do not make task changes there while another
agent owns its branch.

Create agent worktrees only under the sibling directory
`../synaflow-worktrees/`, naming both the directory and branch as
`codex/<agent>-<task>`:

```bash
git fetch origin
mkdir -p ../synaflow-worktrees
git worktree add ../synaflow-worktrees/<agent>-<task>   -b codex/<agent>-<task> origin/main
```

After the PR is merged, and only after confirming the worktree has no
intended uncommitted changes, clean it up:

```bash
git fetch origin --prune
git worktree remove ../synaflow-worktrees/<agent>-<task>
git branch -d codex/<agent>-<task>
git worktree prune
```

Never use `--force` for cleanup unless the user has explicitly approved
discarding the worktree's uncommitted files. Do not remove a worktree
currently owned by another active agent.

!!! note "Squash-merged branches"
    When a PR is squash-merged, the branch's individual commits are not
    reachable from `main`, so `git branch -d` will fail with "not
    fully merged". Use `git branch -d` first (soft warning); fall back
    to `git branch -D` only if you have verified the merge commit is on
    `main` and you want to discard the branch.

## 3. Development Setup

We use `uv` for lightning-fast dependency management and virtual environments.

```bash
# 1. Clone the repository
git clone https://github.com/humansoftware/synaflow.git
cd synaflow

# 2. Install dependencies (including development tools)
uv pip install -e ".[dev]"

# 3. Setup pre-commit hooks
pre-commit install
```

### Running Tests
We enforce 100% test passing before any commit.
```bash
# Run the entire test suite
uv run pytest tests/
```

## 4. Core Architectural Philosophy

SynaFlow is built under strict adherence to several core principles. If a Pull Request violates these, it will be rejected regardless of how useful the feature is.

### KISS (Keep It Simple, Stupid) & YAGNI (You Aren't Gonna Need It)
- Do not introduce complex abstractions for theoretical future use cases.
- Build exactly what is needed to solve the current problem.
- Keep the public API surface (`pipeline`, `step`) as minimal as mathematically possible.

### DRY (Don't Repeat Yourself) & Clean Code
- The code must read like English.
- Avoid magic strings at all costs (use Enums like `OnError` and distinct classes like `TeeWrapper`).
- Methods should be short and do exactly one thing.

## 5. Testing Patterns

We treat tests as our **Universal Contract**, not just implementation checks.

### The Universal DAG Contract
Our Runner tests (e.g., `tests/test_runner_materialization.py`) are **Contract Tests**.
They are parameterized using the `run_pipeline` fixture. This means the exact same test file validates the Synchronous Runner today, and will validate any future Asynchronous or Distributed Runners tomorrow.

- **Do NOT assert strict micro-ordering:** Different runners (like async) execute parallel nodes non-deterministically. Do not write tests like `assert call_order == ["A", "B"]`.
- **DO assert causality and completeness:** Verify that consumer "B" eventually received all items from producer "A", regardless of interleaving.
Example: `assert [v for k,v in call_order if k == "B"] == [1, 2, 3]`

### Corpus-Driven Validation
All structural compilation and DAG validation logic is tested against our `tests/corpus/`.
If you invent a new topological challenge (e.g., a complex fan-out/fan-in loop), add it to the `corpus` directory. The automated parameterized suite in `test_corpus_validation.py` will automatically pick it up and ensure the DAG compiler never breaks on your edge case.

### Corpus vs. Unit Tests: What goes where?
When contributing a new test, ask yourself: **"What exactly am I testing?"** and consider the trade-offs:
- **Add to `tests/corpus/`** when you want to validate a **macroscopic DAG topology** (e.g., a new shape of nodes like "diamond within a diamond"). Adding to the corpus is highly leveraged because it automatically tests DAG compilation, cycle detection, and topological sorting across the entire framework.
- **Write a specific Unit/Contract Test** when you want to validate a **microscopic behavioral rule** (e.g., "how does the runner behave if a consumer expects an `Iterator[int]` but the generator yields a `string`?"). Specific test files (like `test_runner_materialization.py`) should be used for testing execution semantics, error handling (`on_error`), and edge cases of type compatibility.

Adding everything to the corpus would make execution tests slow and confusing, while adding pure topological shapes to unit tests creates unnecessary boilerplate. Separate the "shape of the graph" from the "rules of the engine".

## 6. Coding Style

We use `pre-commit` to enforce styling automatically, but keep the following in mind:
- **Type Hinting:** 100% mandatory. SynaFlow relies on runtime type introspection (`inspect.Signature`) to route dependencies. If you don't type hint it, the DAG won't compile.
- **Docstrings:** Use docstrings for classes and complex methods. Explain *why*, not just *what*.

Happy Hacking!
