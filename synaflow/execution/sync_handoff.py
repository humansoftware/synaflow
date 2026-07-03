from __future__ import annotations

import queue
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Callable


EOF_MARKER = object()


@dataclass
class ExceptionMarker:
    exception: BaseException


class SyncQueueIterator(Iterator):
    """Blocking iterator over a bounded queue-backed branch."""

    def __init__(self, branch_name: str, q: queue.Queue, owner: "SyncFanout") -> None:
        self._branch_name = branch_name
        self._queue = q
        self._owner = owner
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        item = self._queue.get()
        if item is EOF_MARKER:
            self.close()
            raise StopIteration
        if isinstance(item, ExceptionMarker):
            self.close()
            raise item.exception
        return item

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._owner.close_branch(self._branch_name)

    def __del__(self) -> None:  # pragma: no cover - best-effort cleanup only
        self.close()


class SyncFanout:
    """Bounded sync handoff for lazy fan-out branches."""

    def __init__(
        self,
        source: Iterator,
        *,
        max_in_flight: int,
        branches: list[str],
        queue_factory: Callable[..., queue.Queue] = queue.Queue,
    ) -> None:
        self._source = source
        self._queues = {
            branch: queue_factory(maxsize=max_in_flight) for branch in branches
        }
        self._active_branches = set(branches)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def lazy_iterator(self, branch_name: str) -> SyncQueueIterator:
        return SyncQueueIterator(branch_name, self._queues[branch_name], self)

    def close_branch(self, branch_name: str) -> None:
        with self._lock:
            self._active_branches.discard(branch_name)
            if not self._active_branches:
                self._stop.set()

    def abort(self, exception: BaseException | None = None) -> None:
        self._stop.set()
        marker: object = EOF_MARKER if exception is None else ExceptionMarker(exception)
        for branch_name, q in self._queues.items():
            self._put_terminal(branch_name, q, marker)

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join()

    def _pump(self) -> None:
        try:
            for item in self._source:
                if self._stop.is_set():
                    break
                for branch_name, q in self._queues.items():
                    self._put_item(branch_name, q, item)
        except BaseException as exc:
            self.abort(exc)
            return

        for branch_name, q in self._queues.items():
            self._put_terminal(branch_name, q, EOF_MARKER)

    def _put_item(self, branch_name: str, q: queue.Queue, item: Any) -> None:
        while not self._stop.is_set():
            with self._lock:
                if branch_name not in self._active_branches:
                    return
            try:
                q.put(item, timeout=0.05)
                return
            except queue.Full:
                continue

    def _put_terminal(self, branch_name: str, q: queue.Queue, marker: object) -> None:
        while True:
            with self._lock:
                if branch_name not in self._active_branches:
                    return
            try:
                q.put(marker, timeout=0.05)
                return
            except queue.Full:
                if marker is not EOF_MARKER:
                    # For exceptions, fail fast by dropping unread items to make room
                    try:
                        q.get_nowait()
                    except queue.Empty:
                        pass
                continue
