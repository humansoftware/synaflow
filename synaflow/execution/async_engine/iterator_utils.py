import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field

from .constants import EOF_MARKER


@dataclass
class AsyncQueueBranch:
    queue: asyncio.Queue
    active: bool = True
    _close_reason: str | None = field(default=None, repr=False)
    _failure: BaseException | None = field(default=None, repr=False)

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Mirror SyncQueueIterator: a closed branch never blocks again —
        # it reports why it closed (exhausted / failed / closed early).
        if self.active is False:
            if self._close_reason == "exhausted":
                raise StopAsyncIteration
            if self._close_reason == "failed" and self._failure is not None:
                raise self._failure
            raise RuntimeError(
                "Branch of a fan-out stream was closed before being fully"
                " consumed.  Fan-out branch streams are only valid while the"
                " pipeline is running; keep a materialized (list/set/dict)"
                " output if you need the data after the run."
            )
        item = await self.queue.get()
        if isinstance(item, Exception):
            self.close(reason="failed")
            self._failure = item
            raise item
        if item is EOF_MARKER:
            self.close(reason="exhausted")
            raise StopAsyncIteration
        return item

    async def put(self, item) -> None:
        # Bounded put that stays responsive to branch closure: block on
        # the queue's put properly, but re-check ``active`` periodically
        # so a closed branch (consumer gone) never strands the pump.
        # ``wait_for`` cancellation of ``Queue.put`` is safe — the
        # internal waiter is removed and no item is consumed.
        while self.active:
            try:
                await asyncio.wait_for(self.queue.put(item), timeout=0.05)
                return
            except asyncio.TimeoutError:
                continue

    async def get(self):
        return await self.queue.get()

    def close(self, reason: str = "closed") -> None:
        if self.active is False:
            return
        self.active = False
        self._close_reason = reason

    def terminate(self, exception: BaseException) -> None:
        """Close the branch and deliver ``exception`` to the next consumer.

        Used by executor cleanup so a branch that was never fully
        consumed raises loudly after the run instead of blocking on
        ``queue.get()`` forever.
        """
        self.close(reason="failed")
        self._failure = exception
        try:
            self.queue.put_nowait(exception)
        except asyncio.QueueFull:
            # Queue is full of unread items; drop the oldest so the
            # terminal marker is guaranteed to arrive.
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover - defensive
                pass
            self.queue.put_nowait(exception)


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
