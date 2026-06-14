#!/bin/bash
set -e
uv run pytest --cov=synaflow --cov-report=json -q tests/
python .github/scripts/coverage_report.py --precommit
