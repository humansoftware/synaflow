# Spec: Smart Binding & Semantic Step Naming

> **NOTE:** Once this specification is fully implemented and the official documentation is updated, this file MUST be deleted.

## Objective
Implement a "Smart Binding" system for dependencies and eliminate ambiguity in step names. The framework will allow users to reference datasets using natural synonyms (singular, plural, or suffixes like `_list`), while guaranteeing that each pipeline has a unique set of produced Base Datasets.

## Architecture & Rules
1. **Base Dataset Name:** Every step produces exactly one Base Dataset. The Base Dataset name is derived from the step name by transforming it into its absolute plural form (e.g., `user`, `users`, and `user_list` all resolve to `users`).
2. **Smart Binding:** A user can refer to a dataset in their function parameters using ANY of its synonyms (e.g., `user` or `users_list`). The system resolves these back to the Base Dataset.
3. **Compound Names:** For compound names separated by `_` (e.g., `fetched_securities`), the `inflect` library is applied **only to the last word** (`securities` -> `security`).

## Python Modules to Modify
1. **`pyproject.toml`** (and lock files): Add `inflect` as a project dependency.
2. **`synaflow/core/naming.py`** (New Module):
   * `get_base_dataset_name(name: str) -> str`: Removes suffixes like `_list`, splits by `_`, retrieves the last word, uses `inflect.plural()` on it, and reconstructs the string. Returns the absolute plural base name (e.g., `"fetched_securities"`).
   * `_get_dataset_synonyms(base_name: str) -> set[str]`: Returns valid variations for the base name (singular, plural, and common suffixes).
3. **`synaflow/core/dag_dependencies.py`**:
   * Update `validate_and_resolve_dependencies`: When `param_name` is not directly found in `produced`, call `get_base_dataset_name(param_name)` and check if it matches the `get_base_dataset_name` of any produced step.
4. **`synaflow/core/dag_steps.py`** (or builder):
   * Implement build-time validations:
     - **No Duplicate Base Datasets:** Raise a ValueError if two steps in the pipeline map to the same Base Dataset Name.
     - **No Duplicate Parameters:** Raise a ValueError if a single function requires two parameters that map to the same Base Dataset (e.g., `def func(user, users):`).

## Tests to Add & Modify
1. **`tests/core/test_naming.py`** (New):
   * Test `get_base_dataset_name` with edge cases (uncountable nouns like `data`, and irregular plurals like `person`/`people`, `status`).
   * Test compound names (`fetched_securities`).
2. **Corpus Pipelines (`tests/corpus/` or equivalent)**:
   * **DO NOT add a new pipeline.** Modify the existing example pipelines/fixtures in the corpus to make use of the mixed rules.
   * Example: If an existing pipeline has an `items` producer, change the EACH consumer step to accept `item` (singular), and the ALL consumer to accept `item_list`. This ensures Smart Binding is rigorously tested end-to-end across all test suites (sync and async).
3. **Validation Error Tests**:
   * Add/update unit tests to ensure the framework rejects DAGs with colliding Base Dataset Names (e.g., a `record` step and a `records` step in the same pipeline).

## Documentation to Update
1. **`docs/DESIGN_PHILOSOPHY.md`**:
   * Add a new section detailing the **Base Dataset Name** and **Smart Binding** concepts.
   * Explicitly explain that naming should focus on *Nouns* (Option 2) and demonstrate how the framework enables clean code (Singular for EACH, Plural for ALL) without creating state collisions under the hood.
