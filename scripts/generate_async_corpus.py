import os
import re

SYNC_DIR = "tests/corpus/sync"
ASYNC_DIR = "tests/corpus/async"

def translate(content: str) -> str:
    content = content.replace("from collections.abc import Generator, Iterator", "from collections.abc import AsyncGenerator, AsyncIterator")
    content = content.replace("Generator[", "AsyncGenerator[")
    content = content.replace("Iterator[", "AsyncIterator[")
    content = re.sub(r"yield from range\((.*?)\)", r"for _i in range(\1):\n        yield _i", content)
    content = re.sub(r"for ([a-zA-Z0-9_]+) in ([a-zA-Z0-9_]+):", r"async for \1 in \2:", content)
    content = re.sub(r"\bdef ([a-zA-Z0-9_]+)\(", r"async def \1(", content)
    content = content.replace("async def __init__", "def __init__")
    content = content.replace("async for _i in range", "for _i in range")
    return content

os.makedirs(ASYNC_DIR, exist_ok=True)

for filename in os.listdir(SYNC_DIR):
    if not filename.endswith(".py") or filename == "__init__.py": continue
    with open(os.path.join(SYNC_DIR, filename), "r") as f:
        content = f.read()
    
    async_content = translate(content)
    
    with open(os.path.join(ASYNC_DIR, filename), "w") as f:
        f.write(async_content)

# create __init__.py for both
sync_init = "EXAMPLES = {}\n"
for filename in os.listdir(SYNC_DIR):
    if not filename.endswith(".py") or filename == "__init__.py": continue
    mod = filename[:-3]
    sync_init += f"from .{mod} import {mod}_pipeline\n"
    sync_init += f"EXAMPLES['sync_{mod}'] = {mod}_pipeline\n"
with open(os.path.join(SYNC_DIR, "__init__.py"), "w") as f: f.write(sync_init)

async_init = "EXAMPLES = {}\n"
for filename in os.listdir(ASYNC_DIR):
    if not filename.endswith(".py") or filename == "__init__.py": continue
    mod = filename[:-3]
    async_init += f"from .{mod} import {mod}_pipeline\n"
    async_init += f"EXAMPLES['async_{mod}'] = {mod}_pipeline\n"
with open(os.path.join(ASYNC_DIR, "__init__.py"), "w") as f: f.write(async_init)
