"""Unit tests for ``wait_for_workers_after_shutdown``.

Tests inject ``_enumerate_threads``, ``_is_alive``, ``_sleep``,
``_monotonic``, and ``_log`` so the suite exercises pure logic without
real threads or real time.  Keeps total runtime in the low ms.
"""

import os
import threading
from typing import Any

from synaflow.execution.sync_engine.executor import (
    wait_for_workers_after_shutdown,
)


def _make_thread(name: str) -> threading.Thread:
    """Construct (but never start) a ``threading.Thread`` for its name attr."""
    return threading.Thread(name=name)


def _alive_for_first_n_polls(n: int) -> Any:
    """Return an ``_is_alive`` that is True for the first ``n`` polls
    (per-thread) and False thereafter.
    """
    counts: dict[int, int] = {}

    def is_alive(t: threading.Thread) -> bool:
        cur = counts.get(id(t), 0) + 1
        counts[id(t)] = cur
        return cur <= n

    return is_alive


def _logged_once_after(seconds: float) -> Any:
    """Return (``_log_sink``, ``_monotonic``) so that ``_monotonic`` returns
    whatever ``_log_sink.was_called_at_seconds`` says, simulating time.
    """
    log_at = {"value": None}
    initial = {"value": 0.0}

    def monotonic() -> float:
        return initial["value"]

    def log(msg: str, *args: Any, **kw: Any) -> None:
        log_at["value"] = initial["value"]

    def advance(seconds_to_add: float) -> None:
        initial["value"] += seconds_to_add

    return log, monotonic, advance, log_at


def test_returns_when_no_threads():
    """No threads → return 1 poll, no sleep, no log."""
    sleeps: list[float] = []
    logs: list[tuple] = []

    polls = wait_for_workers_after_shutdown(
        _enumerate_threads=lambda: [],
        _is_alive=lambda _t: False,
        _sleep=sleeps.append,
        _monotonic=lambda: 0.0,
        _log=lambda *a, **kw: logs.append((a, kw)),
        _process_pid=12345,
    )
    assert polls == 1
    assert sleeps == []
    assert logs == []


def test_filters_threads_outside_prefix():
    """Threads not matching the prefix are skipped, even when is_alive=True."""
    other = _make_thread("other-pool-worker_0")
    logs: list[tuple] = []

    polls = wait_for_workers_after_shutdown(
        _enumerate_threads=lambda: [other],
        _is_alive=lambda _t: True,
        _sleep=lambda _s: None,
        _monotonic=lambda: 0.0,
        _log=lambda *a, **kw: logs.append((a, kw)),
        _process_pid=1,
    )
    assert polls == 1
    assert logs == []


def test_logs_once_then_returns_when_workers_clear_first_poll():
    """Worker is alive on poll 1 → log; cleared on poll 2 → return 2."""
    t = _make_thread("synaflow-worker_0")
    logs: list[tuple] = []
    sleeps: list[float] = []

    polls = wait_for_workers_after_shutdown(
        _enumerate_threads=lambda: [t],
        _is_alive=_alive_for_first_n_polls(1),
        _sleep=sleeps.append,
        _monotonic=lambda: 0.0,
        _log=lambda *a, **kw: logs.append((a, kw)),
        _process_pid=4242,
    )
    assert polls == 2
    # poll 1: alive → log + sleep.  poll 2: cleared → return.
    assert sleeps == [0.5]
    assert len(logs) == 1
    args = logs[0][0]
    assert args[2] == 4242  # pid (msg, count, pid, names)
    assert args[3] == ["synaflow-worker_0"]
    assert "step function" in args[0]


def test_logs_each_log_window_until_workers_clear():
    """Workers persist across log windows: log fires once per window."""
    t = _make_thread("synaflow-worker_0")
    logs: list[tuple] = []
    sleeps: list[float] = []
    mono = {"now": 0.0}

    # 3 polls, each advances 0.5s.  log_every=1s, so logs at poll 1
    # (mono=0.0) and poll 3 (mono=1.0, gap=1.0≥log_every).
    polls = wait_for_workers_after_shutdown(
        log_every_seconds=1.0,
        _enumerate_threads=lambda: [t],
        _is_alive=_alive_for_first_n_polls(3),
        _sleep=lambda s: (sleeps.append(s), mono.update({"now": mono["now"] + s})),
        _monotonic=lambda: mono["now"],
        _log=lambda *a, **kw: logs.append((a, kw)),
        _process_pid=7,
    )
    assert polls == 4
    assert sleeps == [0.5, 0.5, 0.5]
    assert len(logs) == 2


def test_logs_at_most_once_per_window_even_with_many_short_polls():
    """Polls inside one log window produce only the first log line."""
    t = _make_thread("synaflow-worker_0")
    logs: list[tuple] = []
    sleeps: list[float] = []
    mono = {"now": 0.0}

    # 4 polls.  log_every=10s.  First poll logs (last_log was None);
    # subsequent polls within <10s do not log.  Total alive_for=4 → 5 polls,
    # then cleared.  After 5 polls mono=2.5 < 10, so only 1 log.
    polls = wait_for_workers_after_shutdown(
        log_every_seconds=10.0,
        _enumerate_threads=lambda: [t],
        _is_alive=_alive_for_first_n_polls(4),
        _sleep=lambda s: (sleeps.append(s), mono.update({"now": mono["now"] + s})),
        _monotonic=lambda: mono["now"],
        _log=lambda *a, **kw: logs.append((a, kw)),
        _process_pid=7,
    )
    assert polls == 5
    assert len(logs) == 1


def test_logs_multiple_workers_in_single_line():
    """Multiple alive workers → all names appear in one log line."""
    a = _make_thread("synaflow-worker_0")
    b = _make_thread("synaflow-worker_1")
    logs: list[tuple] = []
    sleeps: list[float] = []

    polls = wait_for_workers_after_shutdown(
        _enumerate_threads=lambda: [a, b],
        _is_alive=_alive_for_first_n_polls(1),
        _sleep=sleeps.append,
        _monotonic=lambda: 0.0,
        _log=lambda *a, **kw: logs.append((a, kw)),
        _process_pid=99,
    )
    assert polls == 2
    assert len(logs) == 1
    args = logs[0][0]
    assert args[1] == 2  # count
    assert sorted(args[3]) == ["synaflow-worker_0", "synaflow-worker_1"]


def test_process_pid_defaults_to_os_getpid():
    """``_process_pid=None`` resolves to ``os.getpid()`` at call time."""
    seen_pids: list[int] = []
    t = _make_thread("synaflow-worker_0")

    def fake_log(_msg: str, _count: int, pid: int, _names: Any) -> None:
        seen_pids.append(pid)

    wait_for_workers_after_shutdown(
        _enumerate_threads=lambda: [t],
        _is_alive=_alive_for_first_n_polls(1),
        _sleep=lambda _s: None,
        _monotonic=lambda: 0.0,
        _log=fake_log,
        _process_pid=None,
    )
    assert seen_pids == [os.getpid()]


def test_custom_thread_name_prefix_is_honoured():
    """``thread_name_prefix`` filters out unrelated threads."""
    not_matching = _make_thread("synaflow-worker_0")
    logs: list[tuple] = []

    polls = wait_for_workers_after_shutdown(
        thread_name_prefix="custom-pool-",
        _enumerate_threads=lambda: [not_matching],
        _is_alive=lambda _t: True,
        _sleep=lambda _s: None,
        _monotonic=lambda: 0.0,
        _log=lambda *a, **kw: logs.append((a, kw)),
        _process_pid=1,
    )
    assert polls == 1
    assert logs == []


def test_poll_seconds_passed_through_to_sleep():
    """Sleep is called with the configured ``poll_seconds`` value."""
    sleeps: list[float] = []
    t = _make_thread("synaflow-worker_0")
    mono = {"now": 0.0}

    polls = wait_for_workers_after_shutdown(
        poll_seconds=0.25,
        log_every_seconds=10.0,  # suppress re-logging
        _enumerate_threads=lambda: [t],
        _is_alive=_alive_for_first_n_polls(2),
        _sleep=lambda s: (sleeps.append(s), mono.update({"now": mono["now"] + s})),
        _monotonic=lambda: mono["now"],
        _log=lambda *_a, **_kw: None,
        _process_pid=1,
    )
    assert polls == 3
    assert sleeps == [0.25, 0.25]
