import time
from synaflow.execution.sync_handoff import SyncFanout


def test_given_full_branch_queue_when_stream_finishes_then_last_item_is_not_dropped():
    fanout = SyncFanout(iter([1]), max_in_flight=1, branches=["a"])
    fanout.start()

    it = fanout.lazy_iterator("a")
    time.sleep(0.2)

    items = list(it)
    assert items == [1], f"Expected [1], got {items}"


if __name__ == "__main__":
    test_given_full_branch_queue_when_stream_finishes_then_last_item_is_not_dropped()
    print("Success!")
