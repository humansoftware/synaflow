import asyncio
from dataclasses import dataclass
from typing import AsyncGenerator

from .constants import EOF_MARKER


@dataclass
class AsyncQueueBranch:
    queue: asyncio.Queue
    active: bool = True

    async def put(self, item) -> None:
        while self.active:
            try:
                self.queue.put_nowait(item)
                return
            except asyncio.QueueFull:
                await asyncio.sleep(0.001)

    async def put_terminal(self, item) -> None:
        if not self.active:
            return
        while self.active:
            try:
                self.queue.put_nowait(item)
                return
            except asyncio.QueueFull:
                await asyncio.sleep(0.001)

    async def get(self):
        return await self.queue.get()

    def close(self) -> None:
        self.active = False


async def queue_to_async_gen(queue: asyncio.Queue | AsyncQueueBranch) -> AsyncGenerator:
    branch = queue if isinstance(queue, AsyncQueueBranch) else None
    q = queue.queue if branch is not None else queue
    try:
        while True:
            item = await q.get()
            if isinstance(item, Exception):
                raise item
            if item is EOF_MARKER:
                break
            yield item
    finally:
        if branch is not None:
            branch.close()


async def async_list(gen: AsyncGenerator) -> list:
    return [x async for x in gen]
