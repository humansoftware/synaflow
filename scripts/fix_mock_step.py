import glob

files = glob.glob("tests/async/test_async_runner_*.py")
for f in files:
    with open(f, "r") as file:
        content = file.read()
    content = content.replace("async def mock_step", "def mock_step")
    content = content.replace(
        "from unittest.mock import MagicMock, call",
        "from unittest.mock import AsyncMock as MagicMock, call",
    )
    # also fix AsyncGenerator[int | str, None, None] which was missed
    content = content.replace(
        "AsyncGenerator[int | str, None, None]", "AsyncGenerator[int | str, None]"
    )
    # fix list(s1) in async context:
    content = content.replace("list(s1)", "[x async for x in s1]")
    with open(f, "w") as file:
        file.write(content)
