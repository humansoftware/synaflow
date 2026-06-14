# Spec: Dual Test Coverage Checks (Total and Patch)

> **NOTE:** Once this specification is fully implemented and the official documentation is updated, this file MUST be deleted.

## Objective
Enforce test coverage visibility on Pull Requests, specifically differentiating between **Total Project Coverage** and **Patch Coverage** (coverage of new or modified lines). The primary goal is to ensure AI agents and developers are prompted to write tests for any new code they introduce, while keeping the rule "skippable" (non-blocking) so it doesn't prevent emergency merges.

## Architecture & Rules
1. **Dual Metrics:** The CI must calculate and report two separate metrics:
   - **Total Coverage:** The overall coverage of the entire repository.
   - **Patch Coverage:** The coverage of lines added or modified in the Pull Request.
2. **Thresholds:** The Patch Coverage threshold must be set to **80%**.
3. **Non-Blocking (Soft Fail / Warning):**
   - The coverage checks should act as warnings. They must NOT hard-block the user from merging the PR.
   - Even if Patch Coverage is 0%, the status should indicate the failure (e.g., a red X or a yellow warning), but the GitHub repository rules should permit the user to merge anyway.
4. **Two GitHub Statuses:** The CI must create **two distinct Status Checks** (Check Runs) on the GitHub PR UI:
   - One specifically for **Total Coverage** (showing the percentage).
   - One specifically for **New Code / Patch Coverage** (showing the percentage and indicating failure if below 80%).

## Implementation Plan
1. **Tooling & Action Selection:**
   - Use a GitHub Action capable of separating total vs patch coverage and generating custom PR check runs.
   - *Developer Note:* While `Codecov` natively does this (posting `codecov/project` and `codecov/patch` statuses without blocking merges unless configured), if avoiding external services, use an action like `pytest-coverage-comment` or a custom script that reads `coverage json` and uses the GitHub REST API (`gh api`) to create two Check Runs.
2. **Workflow Configuration:**
   - Run `pytest --cov=synaflow` without a hard `--cov-fail-under` that returns an exit code 1 (which would abruptly fail the whole GitHub Actions job).
   - Pass the coverage artifact to the reporting action.
   - Configure the reporting action to require 80% on the patch.
3. **Pre-commit Hook:**
   - Update `.pre-commit-config.yaml` to include a local hook that runs `pytest` (and optionally checks coverage) before allowing a commit. This catches failing tests early on the developer's machine.
4. **Testing the CI:**
   - Create a dummy PR that adds untested code to verify that the two statuses appear, the Patch status shows failure, but the Merge button remains green/clickable.
