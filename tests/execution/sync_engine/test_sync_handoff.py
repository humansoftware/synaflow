import queue
import threading
from synaflow.execution.sync_handoff import SyncFanout, EOF_MARKER


def test_given_full_branch_queue_when_stream_finishes_then_last_item_is_not_dropped():
    eof_put_attempted = threading.Event()

    class TestQueue(queue.Queue):
        def put(self, item, block=True, timeout=None):
            if self.full() and item is EOF_MARKER:
                eof_put_attempted.set()
            super().put(item, block=block, timeout=timeout)

    fanout = SyncFanout(
        iter([1]), max_in_flight=1, branches=["a"], queue_factory=TestQueue
    )
    fanout.start()

    eof_put_attempted.wait(timeout=5.0)

    # Double check that the thread is actually alive and blocked, not dead
    fanout._thread.join(timeout=0.1)
    assert fanout._thread.is_alive(), "Thread should be blocked waiting for queue space"

    it = fanout.lazy_iterator("a")
    items = list(it)
    assert items == [1]


def test_given_full_branch_queue_when_aborted_then_exception_is_raised_and_unconsumed_dropped():
    put_blocked_event = threading.Event()

    class TestQueue(queue.Queue):
        def put(self, item, block=True, timeout=None):
            # This triggers when the second item is put and the queue is full
            if self.full() and item == 2:
                put_blocked_event.set()
            super().put(item, block=block, timeout=timeout)

    fanout = SyncFanout(
        iter([1, 2]), max_in_flight=1, branches=["a"], queue_factory=TestQueue
    )
    fanout.start()

    put_blocked_event.wait(timeout=5.0)

    it = fanout.lazy_iterator("a")
    fanout.abort(ValueError("Boom"))

    items = []
    try:
        for i in it:
            items.append(i)
        assert False, "Should have raised"
    except ValueError as e:
        assert str(e) == "Boom"


def test_given_fanout_with_normal_exhaustion_when_pump_finishes_then_join_reports_true():
    """SyncFanout cleanup contract: after normal source exhaustion and
    consumer drain, ``join()`` must report that the pump exited.

    Regression test for Issue #120 — the previous ``join()`` returned
    ``None`` (falsy) unconditionally because ``Thread.join(timeout)``
    returns ``None`` in Python 3."""
    fanout = SyncFanout(iter(range(5)), max_in_flight=2, branches=["a"])

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
    assert fanout.join(timeout=1.0), (
        "join() must return True when the pump has exited normally"
    )


def test_given_fanout_under_external_abort_when_source_exhausts_then_join_reports_true():
    """SyncFanout cleanup contract: when an external abort races with
    the pump during normal source exhaustion, ``join()`` must still
    report that the pump exited — no false positives under contention."""
    fanout = SyncFanout(iter(range(10)), max_in_flight=2, branches=["a", "b"])

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
    abort_called.set()

    for c in consumers:
        c.join(timeout=5.0)
    aborter.join(timeout=5.0)
    assert fanout.join(timeout=1.0), (
        "join() must return True even when abort races with the pump"
    )


def test_given_fanout_with_blocked_source_when_source_never_yields_then_join_reports_false():
    """SyncFanout cleanup contract: when the source blocks forever,
    ``join()`` must report that the pump did NOT exit within the
    timeout so the caller can take action (log a warning, abandon
    the worker)."""
    blocked = threading.Event()

    def _blocked_source():
        blocked.wait()
        yield  # never reached until blocked.set()

    fanout = SyncFanout(_blocked_source(), max_in_flight=2, branches=["a"])
    # Start the pump explicitly so ``join()`` does not race with the
    # lazy ``ensure_started`` call inside the drain thread (Issue #120).
    fanout.start()

    def drain():
        it = fanout.lazy_iterator("a")
        try:
            for _ in it:
                pass
        finally:
            it.close()

    t = threading.Thread(target=drain, daemon=True)
    t.start()
    assert not fanout.join(timeout=0.3), (
        "join() must return False when the pump is stuck in a blocked source"
    )
    blocked.set()
    t.join(timeout=5.0)


def test_given_full_queue_when_pump_pushes_terminal_and_abort_called_then_pump_exits():
    """SyncFanout contract: when the pump is stuck in ``_put_terminal``
    trying to push ``EOF_MARKER`` to a full queue and ``abort()`` is
    called concurrently, the pump must detect ``_stop``, drop the
    unread item, and exit (regression: the old code looped forever
    leaking a daemon thread)."""
    terminal_attempted = threading.Event()

    class TestQueue(queue.Queue):
        def put(self, item, block=True, timeout=None):
            if self.full() and item is EOF_MARKER:
                terminal_attempted.set()
            super().put(item, block=block, timeout=timeout)

    fanout = SyncFanout(
        iter(range(1)), max_in_flight=1, branches=["a"], queue_factory=TestQueue
    )
    fanout.start()

    # pump pushes item 0 → queue full.  Source exhausts → pump enters
    # _put_terminal.  On the first EOF_MARKER attempt the TestQueue
    # signals via terminal_attempted.
    assert terminal_attempted.wait(timeout=5.0), (
        "pump must reach _put_terminal within timeout"
    )

    # Now abort from a different thread.  This sets _stop and runs
    # its own _put_terminal (which races with the pump's).  Both
    # threads must detect _stop.is_set() and drop items to exit.
    fanout.abort()
    assert fanout.join(timeout=5.0), (
        "pump must exit when abort is called while waiting to push terminal"
    )
