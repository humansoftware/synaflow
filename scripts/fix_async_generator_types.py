import glob
import os

files = glob.glob("tests/async/test_async_runner_*.py")
for f in files:
    with open(f, "r") as file:
        content = file.read()
    content = content.replace(
        "AsyncGenerator[int, None, None]", "AsyncGenerator[int, None]"
    )
    content = content.replace(
        "AsyncGenerator[str, None, None]", "AsyncGenerator[str, None]"
    )
    with open(f, "w") as file:
        file.write(content)
