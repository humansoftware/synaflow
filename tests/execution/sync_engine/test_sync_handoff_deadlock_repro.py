"""Regression tests for SyncFanout.join() (Issue #120).

``threading.Thread.join(timeout)`` returns ``None`` unconditionally in
Python 3; ``SyncFanout.join()`` was using that return value directly,
which was always falsy.  The fix uses ``is_alive()`` after the raw
``join``, making the return value meaningful.
"""

import threading
import time

from synaflow.execution.sync_handoff import SyncFanout


def test_join_returns_true_when_pump_exits():
    fanout = SyncFanout(iter((1,)), max_in_flight=1, branches=["a"])

    def drain():
        it = fanout.lazy_iterator("a")
        try:
            for _ in it:
                pass
        finally:
            it.close()

    t = threading.Thread(target=drain, daemon=True)
    t.start()
    t.join(timeout=5.0)

    assert fanout.join(timeout=5.0) is True


def test_join_returns_true_for_multiple_branches_under_abort_contention():
    """Stress test: pump + abort racing across 2 branches.  With a
    correct lock protocol the pump always exits; verify join() reports
    that.  This is a regression test for the false-positive deadlock
    reported in Issue #120 — the original ``join()`` returned ``None``
    (falsy) in every case, making ``cleanup()`` log a misleading
    warning."""
    for _ in range(200):
        fanout = SyncFanout(iter(range(10)), max_in_flight=1, branches=["a", "b"])

        def drain(b):
            it = fanout.lazy_iterator(b)
            try:
                for _ in it:
                    pass
            finally:
                it.close()

        consumers = [
            threading.Thread(target=drain, args=(b,), daemon=True) for b in ("a", "b")
        ]
        for c in consumers:
            c.start()

        abort_called = threading.Event()

        def do_abort():
            abort_called.wait()
            fanout.abort()

        aborter = threading.Thread(target=do_abort, daemon=True)
        aborter.start()
        time.sleep(0.005)
        abort_called.set()

        assert fanout.join(timeout=5.0), (
            "pump did not exit within 5 s — real deadlock or join() still broken"
        )

        for c in consumers:
            c.join(timeout=1)
        aborter.join(timeout=1)


def test_join_returns_true_when_pump_not_started():
    """join() on a SyncFanout whose pump was never started returns True."""
    fanout = SyncFanout(iter((1,)), max_in_flight=1, branches=["a"])
    assert fanout.join(timeout=0.1) is True


def test_join_returns_false_when_pump_is_stuck():
    """A pump blocked on a never-firing source should not exit, so join()
    with a short timeout must return False."""
    blocked = threading.Event()

    def _blocked_source():
        blocked.wait()
        yield  # never reached until blocked.set()

    fanout = SyncFanout(
        _blocked_source(),
        max_in_flight=1,
        branches=["a"],
    )

    # Start a consumer so ensure_started triggers the pump.
    def drain():
        it = fanout.lazy_iterator("a")
        try:
            for _ in it:
                pass
        finally:
            it.close()

    t = threading.Thread(target=drain, daemon=True)
    t.start()
    time.sleep(0.05)  # let consumer trigger ensure_started
    assert not fanout.join(timeout=0.3), (
        "pump should NOT exit when blocked on a never-firing source"
    )

    blocked.set()
    t.join(timeout=5.0)
