"""
Compute total and patch test coverage from pytest-cov JSON output.

CI mode (default): compares against origin/main, posts two GitHub Check Runs.
Pre-commit mode (--precommit): compares against HEAD (staged changes),
exits non-zero if patch coverage < 80%.
"""

import json
import os
import subprocess
import sys

PATCH_THRESHOLD = 80
TOTAL_THRESHOLD = 80


def get_total_coverage(coverage_json_path="coverage.json"):
    """Extract total coverage percentage from coverage.json."""
    with open(coverage_json_path) as f:
        data = json.load(f)
    totals = data["totals"]
    num_statements = totals["num_statements"]
    if num_statements == 0:
        return 100.0
    return (totals["covered_lines"] / num_statements) * 100


def format_coverage_report(coverage_json_path="coverage.json"):
    """Build a markdown table of per-file coverage from coverage.json."""
    with open(coverage_json_path) as f:
        data = json.load(f)

    rows = []
    for file_path, file_info in sorted(data["files"].items()):
        totals = file_info["summary"]
        num_statements = totals["num_statements"]
        if num_statements == 0:
            continue
        pct = (totals["covered_lines"] / num_statements) * 100
        missing = totals["missing_lines"]
        rows.append(f"| {file_path} | {num_statements} | {missing} | {pct:.1f}% |")

    if not rows:
        return "No tracked files."
    header = "| File | Stmts | Miss | Cover |\n| --- | --- | --- | --- |"
    return header + "\n" + "\n".join(rows)


def get_changed_lines_pr():
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
    return _parse_diff(result.stdout)


def get_changed_lines_precommit():
    """Return dict of {filepath: set(line_numbers)} for staged changes."""
    result = subprocess.run(
        ["git", "diff", "--cached", "HEAD", "--unified=0", "--", "synaflow/"],
        capture_output=True,
        text=True,
    )
    return _parse_diff(result.stdout)


def _parse_diff(diff_output):
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


def run_ci():
    """CI mode: report total coverage via the job summary and patch coverage as a Check Run."""
    coverage_path = "coverage.json"
    if not os.path.exists(coverage_path):
        print("Error: coverage.json not found. Tests may have failed.")
        sys.exit(1)

    total_pct = get_total_coverage(coverage_path)
    print(f"Total coverage: {total_pct:.1f}%")

    changed_lines = get_changed_lines_pr()
    patch_pct, trackable_count = get_patch_coverage(coverage_path, changed_lines)
    print(
        f"Patch coverage: {patch_pct:.1f}% ({trackable_count} trackable lines changed)"
    )

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as f:
            f.write(f"## Total Coverage: {total_pct:.1f}%\n")
            f.write(f"(threshold: {TOTAL_THRESHOLD}%)\n\n")
            f.write(format_coverage_report(coverage_path))
            f.write("\n")

    patch_conclusion = "success" if patch_pct >= PATCH_THRESHOLD else "failure"
    patch_summary = (
        f"Coverage of new/modified lines: **{patch_pct:.1f}%** "
        f"(threshold: {PATCH_THRESHOLD}%, {trackable_count} trackable lines changed)."
    )
    create_check_run(
        name="Patch Coverage",
        conclusion=patch_conclusion,
        title=f"Patch Coverage: {patch_pct:.1f}%",
        summary=patch_summary,
    )

    if total_pct < TOTAL_THRESHOLD:
        print(
            f"FAIL: total coverage {total_pct:.1f}% is below {TOTAL_THRESHOLD}% threshold"
        )
        sys.exit(1)


def run_precommit():
    """Pre-commit mode: check patch coverage of staged changes, exit non-zero if < threshold."""
    coverage_path = "coverage.json"
    if not os.path.exists(coverage_path):
        print("Error: coverage.json not found. Tests may have failed.")
        sys.exit(1)

    changed_lines = get_changed_lines_precommit()
    if not changed_lines:
        print("No changes in synaflow/ package, skipping patch coverage check.")
        sys.exit(0)

    pct, trackable = get_patch_coverage(coverage_path, changed_lines)
    if trackable == 0:
        print("Patch coverage: N/A (no trackable lines changed)")
        sys.exit(0)

    print(f"Patch coverage: {pct:.1f}% ({trackable} trackable lines)")

    if pct < PATCH_THRESHOLD:
        print(f"FAIL: patch coverage {pct:.1f}% is below {PATCH_THRESHOLD}% threshold")
        sys.exit(1)

    print(f"PASS: patch coverage {pct:.1f}% meets {PATCH_THRESHOLD}% threshold")
    sys.exit(0)


def main():
    if "--precommit" in sys.argv:
        run_precommit()
    else:
        run_ci()


if __name__ == "__main__":
    main()
