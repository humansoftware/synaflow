import os
import re

SYNC_DIR = "tests/sync"
ASYNC_DIR = "tests/async"


def translate_sync_to_async(content: str) -> str:
    # Basic imports
    content = content.replace(
        "from synaflow import run",
        "from synaflow import async_run\nimport asyncio\nfrom typing import AsyncGenerator, AsyncIterator",
    )

    # Replace run_pipeline fixture parameter and invocation
    content = re.sub(
        r"def test_([a-zA-Z0-9_]+)\(run_pipeline\):",
        r"@pytest.mark.asyncio\nasync def test_\1():",
        content,
    )
    content = content.replace("run_pipeline(", "await async_run(")
    content = content.replace("run(", "await async_run(")

    # Replace types
    content = content.replace("Generator[", "AsyncGenerator[")
    content = content.replace("Iterator[", "AsyncIterator[")

    # Replace yield from range(...) with async yield loop
    content = re.sub(
        r"yield from range\((.*?)\)",
        r"for _i in range(\1):\n            yield _i",
        content,
    )

    # Replace for loops on iterators with async for
    # We look for "for x in items:" or similar
    content = re.sub(
        r"for ([a-zA-Z0-9_]+) in ([a-zA-Z0-9_]+):", r"async for \1 in \2:", content
    )

    # Replace def with async def for step functions inside tests
    # Step functions are usually named like a, b, s1, s2, c, gen
    # This is tricky with regex. Let's just blindly make ALL defs async EXCEPT the spy_materialize
    content = re.sub(r"\bdef ([a-zA-Z0-9_]+)\(", r"async def \1(", content)

    # Revert test defs back to async def (they already are, but the previous regex hit them)
    # Actually, previous regex made them "async async def"
    content = content.replace("async async def test_", "async def test_")

    # spy_materialize shouldn't be async if we pass it to `materialize=` unless we update `async_run` to await it
    # Currently async_run materializes syncly via `materialize_fn`.
    content = content.replace("async def spy_materialize", "def spy_materialize")

    # Some async generators need to be awaited? No, async functions don't need await if they are just defining generators.
    # But if a function returns an int `async def s1(val) -> int: return val`, that's fine.

    # Some for loops shouldn't be async! E.g. "for _i in range(count):"
    content = content.replace("async for _i in range", "for _i in range")

    return content


for filename in os.listdir(SYNC_DIR):
    if not filename.endswith(".py"):
        continue

    with open(os.path.join(SYNC_DIR, filename), "r") as f:
        content = f.read()

    async_content = translate_sync_to_async(content)

    new_filename = filename.replace("test_runner_", "test_async_runner_")
    with open(os.path.join(ASYNC_DIR, new_filename), "w") as f:
        f.write(async_content)
