# Nullable CLI Flags Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support direct CLI flags for nullable primitive pipeline parameters (e.g. `int | None` and `Optional[int]`).

**Architecture:** Unwrap two-member unions that contain `NoneType` in `_direct_param_type` in `synaflow/cli.py`.

**Tech Stack:** Python 3.10+, typing, argparse.

## Global Constraints

- Type hints are 100% mandatory.
- Sync/async parity must be preserved (not directly affected here, but keep in mind).

---

### Task 1: Nullable CLI Flags Support

**Files:**
- Modify: `tests/cli/test_cli.py`
- Modify: `synaflow/cli.py`

**Interfaces:**
- Consumes: `_direct_param_type`
- Produces: Updated behavior for `_direct_param_type` supporting unions.

- [ ] **Step 1: Write the failing tests**

Write the following test functions in `tests/cli/test_cli.py`:
```python
def test_given_nullable_primitive_flags_then_cli_builds_typed_params():
    from typing import NamedTuple

    class Params(NamedTuple):
        portfolio_id: int | None = None
        force: bool = False

    seen = []

    def capture(portfolio_id: int | None, force: bool) -> None:
        seen.append((portfolio_id, force))

    p = pipeline(
        name="nullable_primitive",
        params=Params,
        steps=[step("capture", fn=capture)],
    )
    catalog = PipelineRegistry()
    catalog.add(p)

    # 1. Supplied flag case
    result = SynaflowCli(catalog=catalog).main(
        [
            "run",
            "nullable_primitive",
            "--portfolio-id",
            "123",
            "--force",
        ]
    )
    assert result == 0
    assert seen == [(123, True)]

    # 2. Omitted flag case
    seen.clear()
    result = SynaflowCli(catalog=catalog).main(
        [
            "run",
            "nullable_primitive",
        ]
    )
    assert result == 0
    assert seen == [(None, False)]


def test_given_optional_primitive_flags_then_cli_builds_typed_params():
    from typing import Optional, NamedTuple

    class Params(NamedTuple):
        portfolio_id: Optional[int] = None

    seen = []

    def capture(portfolio_id: Optional[int]) -> None:
        seen.append(portfolio_id)

    p = pipeline(
        name="optional_primitive",
        params=Params,
        steps=[step("capture", fn=capture)],
    )
    catalog = PipelineRegistry()
    catalog.add(p)

    # 1. Supplied flag case
    result = SynaflowCli(catalog=catalog).main(
        [
            "run",
            "optional_primitive",
            "--portfolio-id",
            "456",
        ]
    )
    assert result == 0
    assert seen == [456]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cli/test_cli.py -k "primitive_flags_then_cli" -v`
Expected: FAIL with unrecognized arguments/errors.

- [ ] **Step 3: Write minimal implementation**

Modify `_direct_param_type` in `synaflow/cli.py` to:
```python
def _direct_param_type(field_type: Any) -> Any | None:
    import types
    from typing import Union

    origin = get_origin(field_type)
    if origin in (Union, getattr(types, "UnionType", None)):
        args = get_args(field_type)
        if len(args) == 2 and type(None) in args:
            field_type = args[0] if args[1] is type(None) else args[1]

    if field_type in {str, int, float, bool, bytes}:
        return field_type
    if get_origin(field_type) is list and get_args(field_type)[0] in {
        str,
        int,
        float,
        bool,
        bytes,
    }:
        return field_type
    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cli/test_cli.py -k "primitive_flags_then_cli" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/cli/test_cli.py synaflow/cli.py
git commit -m "feat: support direct CLI flags for nullable primitive pipeline params"
```
