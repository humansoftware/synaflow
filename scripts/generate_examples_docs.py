#!/usr/bin/env python3
"""
Generate the Examples documentation page with Mermaid diagrams
and inline sync/async source code for every corpus pipeline.

Usage:
    python scripts/generate_examples_docs.py > docs/user_docs/core-concepts/examples.md
"""

import subprocess
import sys
from pathlib import Path

REPO_URL = "https://github.com/humansoftware/synaflow/blob/main"


def _extract_code(path: Path) -> str:
    """Read source file, stopping before test infrastructure lines."""
    lines = []
    for line in path.read_text().split("\n"):
        if line.startswith("from tests.common") or line.startswith("pack ="):
            break
        lines.append(line)
    return "\n".join(lines).rstrip() + "\n"


def main():
    corpus_dir = Path(__file__).resolve().parent.parent / "tests" / "execution"
    pipelines = sorted(
        {
            f.stem
            for f in (corpus_dir / "sync_engine" / "corpus").glob("*.py")
            if f.stem != "__init__"
        }
    )

    print("# Examples")
    print()
    print(
        "Every SynaFlow pipeline can be visualized with "
        "[`scripts/visualize_dag.py`]"
        f"({REPO_URL}/scripts/visualize_dag.py)."
    )
    print()

    for name in pipelines:
        result = subprocess.run(
            ["uv", "run", "python", "scripts/visualize_dag.py", name],
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parent.parent,
        )
        if result.returncode != 0:
            print(f"<!-- {name}: error -->", file=sys.stderr)
            continue

        sync_path = corpus_dir / "sync_engine" / "corpus" / f"{name}.py"
        async_path = corpus_dir / "async_engine" / "corpus" / f"{name}.py"

        print(f"## {name}")
        print()

        if sync_path.exists() and async_path.exists():
            print('=== "Sync"')
            print()
            print("    ```python")
            for line in _extract_code(sync_path).split("\n"):
                print(f"    {line}")
            print("    ```")
            print()
            print('=== "Async"')
            print()
            print("    ```python")
            for line in _extract_code(async_path).split("\n"):
                print(f"    {line}")
            print("    ```")
        elif sync_path.exists():
            print("```python")
            print(_extract_code(sync_path))
            print("```")

        print()
        print(
            f"[:fontawesome-brands-github: Sync source]({REPO_URL}/{sync_path})"
            f" | "
            f"[:fontawesome-brands-github: Async source]({REPO_URL}/{async_path})"
        )
        print()
        print(result.stdout)
        print()

    print("---")
    print("*Diagrams auto-generated from the test corpus.*")


if __name__ == "__main__":
    main()
