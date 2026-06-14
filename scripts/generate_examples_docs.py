#!/usr/bin/env python3
"""
Generate the Examples documentation page with Mermaid diagrams
for every corpus pipeline.

Usage:
    python scripts/generate_examples_docs.py > docs/user_docs/core-concepts/examples.md
"""

import subprocess
import sys
from pathlib import Path

REPO_URL = "https://github.com/humansoftware/synaflow/blob/main"


def main():
    corpus_dir = Path(__file__).resolve().parent.parent / "tests" / "execution"
    pipelines = set()

    for engine in ("sync_engine", "async_engine"):
        for f in (corpus_dir / engine / "corpus").glob("*.py"):
            name = f.stem
            if name == "__init__":
                continue
            pipelines.add(name)

    print("# Examples")
    print()
    print(
        "Every SynaFlow pipeline can be visualized with "
        "[`scripts/visualize_dag.py`]"
        f"({REPO_URL}/scripts/visualize_dag.py)."
    )
    print()

    for name in sorted(pipelines):
        result = subprocess.run(
            ["uv", "run", "python", "scripts/visualize_dag.py", name],
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parent.parent,
        )
        if result.returncode != 0:
            print(f"<!-- {name}: error -->", file=sys.stderr)
            continue

        print(f"## {name}")
        print()

        sync_path = f"tests/execution/sync_engine/corpus/{name}.py"
        async_path = f"tests/execution/async_engine/corpus/{name}.py"
        print(f"[:fontawesome-brands-github: Source]({REPO_URL}/{sync_path})")
        print()

        print(result.stdout)
        print()

    print("---")
    print("*Diagrams auto-generated from the test corpus.*")


if __name__ == "__main__":
    main()
