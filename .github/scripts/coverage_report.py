"""
Compute total and patch test coverage from pytest-cov JSON output
and post two GitHub Check Runs on the pull request.
"""

import json
import os
import subprocess
import sys


def get_total_coverage(coverage_json_path="coverage.json"):
    """Extract total coverage percentage from coverage.json."""
    with open(coverage_json_path) as f:
        data = json.load(f)
    totals = data["totals"]
    num_statements = totals["num_statements"]
    if num_statements == 0:
        return 100.0
    return (totals["covered_lines"] / num_statements) * 100


def get_changed_lines():
    """Return dict of {filepath: set(line_numbers)} changed in this PR.

    Only tracks changes inside the `synaflow/` package directory.
    """
    result = subprocess.run(
        ["git", "merge-base", "origin/main", "HEAD"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("Warning: could not find merge-base, falling back to HEAD~1")
        result = subprocess.run(
            ["git", "rev-parse", "HEAD~1"],
            capture_output=True,
            text=True,
        )
    merge_base = result.stdout.strip()

    result = subprocess.run(
        ["git", "diff", merge_base, "HEAD", "--unified=0", "--", "synaflow/"],
        capture_output=True,
        text=True,
    )
    diff_output = result.stdout

    changed = {}
    current_file = None

    for line in diff_output.split("\n"):
        if line.startswith("+++ b/") and not line.endswith("/dev/null"):
            current_file = line[6:]
        elif line.startswith("@@") and current_file:
            parts = line.split()
            new_part = parts[2]
            new_start = int(new_part[1:].split(",")[0])
            new_count_str = new_part[1:].split(",")
            new_count = int(new_count_str[1]) if len(new_count_str) > 1 else 1

            if current_file not in changed:
                changed[current_file] = set()
            for offset in range(new_count):
                changed[current_file].add(new_start + offset)

    return changed


def get_patch_coverage(coverage_json_path, changed_lines):
    """Compute coverage percentage for lines changed in the PR."""
    with open(coverage_json_path) as f:
        data = json.load(f)

    files_data = data["files"]
    total_trackable = 0
    covered_trackable = 0

    for file_path, file_info in files_data.items():
        executed = set(file_info.get("executed_lines", []))
        missing = set(file_info.get("missing_lines", []))
        trackable = executed | missing

        for changed_file, changed_line_nums in changed_lines.items():
            if _paths_intersect(changed_file, file_path):
                for line_num in changed_line_nums:
                    if line_num in trackable:
                        total_trackable += 1
                        if line_num in executed:
                            covered_trackable += 1

    if total_trackable == 0:
        return 100.0, 0

    percent = (covered_trackable / total_trackable) * 100
    return percent, total_trackable


def _paths_intersect(a, b):
    """Check if two relative file paths refer to the same file."""
    return a == b or a.endswith("/" + b) or b.endswith("/" + a)


def get_head_sha():
    """Return the PR head commit SHA for check-run targeting."""
    sha = os.environ.get("PR_HEAD_SHA", "")
    if sha:
        return sha
    # Fallback: GITHUB_SHA for push events or manual runs
    return os.environ.get("GITHUB_SHA", "")


def create_check_run(name, conclusion, title, summary):
    """Create a GitHub Check Run via the gh CLI."""
    head_sha = get_head_sha()
    repo = os.environ["GITHUB_REPOSITORY"]
    cmd = [
        "gh",
        "api",
        f"/repos/{repo}/check-runs",
        "-X",
        "POST",
        "-f",
        f"name={name}",
        "-f",
        f"head_sha={head_sha}",
        "-f",
        "status=completed",
        "-f",
        f"conclusion={conclusion}",
        "-f",
        f"output[title]={title}",
        "-f",
        f"output[summary]={summary}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR: failed to create check run '{name}':")
        print(result.stderr)
        sys.exit(1)
    print(f"Created check run: {name} — {title}")


def main():
    coverage_path = "coverage.json"
    if not os.path.exists(coverage_path):
        print("Error: coverage.json not found. Tests may have failed.")
        sys.exit(1)

    total_pct = get_total_coverage(coverage_path)
    print(f"Total coverage: {total_pct:.1f}%")

    changed_lines = get_changed_lines()
    patch_pct, trackable_count = get_patch_coverage(coverage_path, changed_lines)
    print(
        f"Patch coverage: {patch_pct:.1f}% ({trackable_count} trackable lines changed)"
    )

    create_check_run(
        name="Total Coverage",
        conclusion="success",
        title=f"Total Coverage: {total_pct:.1f}%",
        summary=f"Overall project test coverage is **{total_pct:.1f}%**.",
    )

    patch_conclusion = "success" if patch_pct >= 80 else "failure"
    patch_summary = (
        f"Coverage of new/modified lines: **{patch_pct:.1f}%** "
        f"(threshold: 80%, {trackable_count} trackable lines changed)."
    )
    create_check_run(
        name="Patch Coverage",
        conclusion=patch_conclusion,
        title=f"Patch Coverage: {patch_pct:.1f}%",
        summary=patch_summary,
    )


if __name__ == "__main__":
    main()
