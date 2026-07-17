# Support Direct CLI Flags for Nullable Primitive Pipeline Params

## Problem
When a pipeline's parameter class contains fields of type `T | None` or `Optional[T]` (where `T` is a supported primitive like `int`, `float`, `str`, `bool`, or `bytes`), `SynaflowCli` fails to create a direct command line flag for them (e.g. `--portfolio-id`).

## Proposed Solution (Approach 1)
Modify `_direct_param_type` in `synaflow/cli.py` to check if `field_type` is a two-member union containing `NoneType` (representing a nullable type). If it is, unwrap it to get the underlying primitive/type `T`, then apply the existing checks.

## Implementation Details
1. In `synaflow/cli.py`, import `types` and verify `Union`/`UnionType`.
2. Update `_direct_param_type` to:
   - Identify union types.
   - If the union has exactly 2 arguments and `type(None)` is one of them, extract the other argument.
   - Run the primitive and list-of-primitive checks on the extracted type.
3. Write test cases in `tests/cli/test_cli.py`:
   - An optional primitive with default `None`.
   - `int | None` (Python 3.10+ style) and `typing.Optional[int]` (typing style).
   - Test both omitted and supplied flag cases.
