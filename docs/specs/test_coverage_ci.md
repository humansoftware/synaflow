# Spec: Test Coverage CI Threshold

> **NOTE:** Once this specification is fully implemented and the official documentation is updated, this file MUST be deleted.

## Objective
Enforce a minimum test coverage threshold of 80% using `pytest-cov`, fail the build if the threshold is not met, and automatically post a coverage report comment on GitHub Pull Requests.

## Configuration & Architecture
1. **Tooling:** We will use `pytest-cov` to calculate coverage.
2. **Threshold:** The 80% fail-under threshold MUST be configured directly inside `pyproject.toml` (under `[tool.pytest.ini_options]` or `[tool.coverage.report]`). This ensures that running `pytest` locally also validates the coverage, keeping developers aware before they even push to CI.
3. **Exclusions:** The coverage report should strictly exclude:
   * The `tests/` directory.
   * The `scripts/` directory.
   * (Other files like pure type-definition files or setup configurations might be added to this list if discovered during implementation).
4. **CI GitHub Action:** The GitHub Actions workflow must be updated to not only run the tests and fail if below 80%, but also to use an Action (e.g., `pytest-coverage-comment` or a similar bot) to post a table of the coverage on the PR.

## Implementation Plan
1. Update `pyproject.toml` to include:
   * `pytest-cov` as a dependency (if not already present).
   * Configuration blocks to `omit = ["tests/*", "scripts/*"]`.
   * Configuration block `fail_under = 80`.
2. Update the `.github/workflows/` YAML file for tests:
   * Ensure it runs `pytest --cov=synaflow`.
   * Add a step to post the coverage comment to the Pull Request. (Note: Ensure the action has `pull-requests: write` permissions).
3. Test the setup by deliberately lowering a file's coverage and ensuring the PR action reports it and fails.
