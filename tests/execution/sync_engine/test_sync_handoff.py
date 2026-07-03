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
