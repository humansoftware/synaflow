from __future__ import annotations

import queue
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any


EOF_MARKER = object()


@dataclass
class ExceptionMarker:
    exception: BaseException


class SyncMaterializedValue:
    """Thread-safe holder for a branch that becomes available later."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._value: Any = None
        self._exception: BaseException | None = None

    def set_result(self, value: Any) -> None:
        self._value = value
        self._event.set()

    def set_exception(self, exception: BaseException) -> None:
        self._exception = exception
        self._event.set()

    def result(self) -> Any:
        self._event.wait()
        if self._exception is not None:
            raise self._exception
        return self._value


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
    """Bounded sync handoff for lazy and eager fan-out branches."""

    def __init__(
        self,
        source: Iterator,
        *,
        max_in_flight: int,
        lazy_branches: list[str],
        eager_branches: list[str],
    ) -> None:
        self._source = source
        self._queues = {
            branch: queue.Queue(maxsize=max_in_flight) for branch in lazy_branches
        }
        self._eager_items = {branch: [] for branch in eager_branches}
        self._active_lazy = set(lazy_branches)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._eager_results = {
            branch: SyncMaterializedValue() for branch in eager_branches
        }

    def start(self) -> None:
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def lazy_iterator(self, branch_name: str) -> SyncQueueIterator:
        return SyncQueueIterator(branch_name, self._queues[branch_name], self)

    def eager_value(self, branch_name: str) -> SyncMaterializedValue:
        return self._eager_results[branch_name]

    def close_branch(self, branch_name: str) -> None:
        with self._lock:
            self._active_lazy.discard(branch_name)
            if not self._active_lazy and not self._eager_results:
                self._stop.set()

    def abort(self, exception: BaseException | None = None) -> None:
        self._stop.set()
        marker: object = EOF_MARKER if exception is None else ExceptionMarker(exception)
        for q in self._queues.values():
            self._put_terminal(q, marker)
        if exception is not None:
            for holder in self._eager_results.values():
                holder.set_exception(exception)

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join()

    def _pump(self) -> None:
        try:
            for item in self._source:
                if self._stop.is_set():
                    break
                for items in self._eager_items.values():
                    items.append(item)
                for branch_name, q in self._queues.items():
                    self._put_item(branch_name, q, item)
        except BaseException as exc:
            self.abort(exc)
            return

        for q in self._queues.values():
            self._put_terminal(q, EOF_MARKER)
        for branch_name, holder in self._eager_results.items():
            holder.set_result(list(self._eager_items[branch_name]))

    def _put_item(self, branch_name: str, q: queue.Queue, item: Any) -> None:
        while not self._stop.is_set():
            with self._lock:
                if branch_name not in self._active_lazy:
                    return
            try:
                q.put(item, timeout=0.05)
                return
            except queue.Full:
                continue

    def _put_terminal(self, q: queue.Queue, marker: object) -> None:
        while True:
            try:
                q.put(marker, timeout=0.05)
                return
            except queue.Full:
                try:
                    q.get_nowait()
                except queue.Empty:
                    continue
