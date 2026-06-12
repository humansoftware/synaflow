import asyncio
from typing import AsyncGenerator

from .constants import EOF_MARKER


async def queue_to_async_gen(queue: asyncio.Queue) -> AsyncGenerator:
    while True:
        item = await queue.get()
        if isinstance(item, Exception):
            raise item
        if item is EOF_MARKER:
            break
        yield item


async def async_list(gen: AsyncGenerator) -> list:
    return [x async for x in gen]
