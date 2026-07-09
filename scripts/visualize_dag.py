"""
Generate a Mermaid.js flowchart from a SynaFlow pipeline JSON export.

Usage:
    python scripts/visualize_dag.py <pipeline_name>  # reads from corpus
    python scripts/visualize_dag.py --json path/to/dag.json

Output: writes a Mermaid markdown diagram to stdout.
"""

import argparse
import importlib
import json
import sys
from pathlib import Path


def _load_corpus_json(pipeline_name: str) -> dict:
    corpus_dir = Path(__file__).resolve().parent.parent / "tests" / "execution"
    for engine in ("sync_engine", "async_engine"):
        corpus_file = corpus_dir / engine / "corpus" / f"{pipeline_name}.py"
        if corpus_file.exists():
            break
    else:
        print(f"Pipeline '{pipeline_name}' not found in corpus.", file=sys.stderr)
        sys.exit(1)

    sys.path.insert(0, str(corpus_dir.parent.parent))
    mod = importlib.import_module(f"tests.execution.{engine}.corpus.{pipeline_name}")
    return mod.pack.json_dag


def _generate_mermaid(dag: dict) -> str:
    lines = ["```mermaid", "flowchart TD"]
    steps = dag.get("steps", {})

    for step_name, node in steps.items():
        label = f"{step_name}<br/><i>{node.get('output', '')}</i>"
        safe = step_name.replace("-", "_")
        lines.append(f'    {safe}["{label}"]')

    for step_name, node in steps.items():
        safe_src = step_name.replace("-", "_")
        for dep_name in node.get("deps", {}):
            safe_dep = dep_name.replace("-", "_")
            lines.append(f"    {safe_dep} --> {safe_src}")

    lines.append("```")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate a Mermaid flowchart from a SynaFlow pipeline"
    )
    parser.add_argument("name", nargs="?", help="Pipeline name from test corpus")
    parser.add_argument("--json", help="Path to a pipeline JSON file")
    args = parser.parse_args()

    if args.json:
        with open(args.json) as f:
            dag = json.load(f)
    elif args.name:
        dag = _load_corpus_json(args.name)
    else:
        parser.print_help()
        sys.exit(1)

    print(_generate_mermaid(dag))


if __name__ == "__main__":
    main()
