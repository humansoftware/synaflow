"""Regression tests for Issue #103: PipelineExecutor hangs on step failure.

Covers the SyncFanout cleanup hang (Test A) and the production scenario
where build_arguments() leaks a SyncQueueIterator branch (Test C and
baselines D, E, F).

The framework's contract for stuck workers is now: the worker is allowed
to remain alive, ``run()`` blocks inside ``wait_for_workers_after_shutdown``
with a per-minute warning log so the user can identify which step is
blocked; the user owns step progress.  Tests that depended on the old
"abandon and daemonise" path (B and G) were removed — that contract is
no longer supported.

Each test runs the pipeline in a daemon watchdog thread with a 5 s
timeout.  The assertions expect the pipeline to EXIT within the timeout —
the framework bug is fixed.
"""

from synaflow.core.dag_builder import build_dag
import threading
from collections.abc import Iterator
from typing import NamedTuple
from synaflow import OnError, pipeline, run, step
from synaflow.core.exceptions import PipelineStopException
from synaflow.execution.sync_engine import PipelineExecutor


class EmptyParams(NamedTuple):
    pass


def test_given_fanout_pump_blocked_when_consumer_raises_then_cleanup_hangs():
    """Cause 2 (cleanup hang): _pump thread is stuck in next(blocked_source).

    Pipeline topology:

        blocked_producer (max_in_flight=3, Iterator[int])
            ├── consumer_a  (Iterator[int] -> None)
            └── consumer_b  (Iterator[int] -> None)  ← raises ValueError

    The _pump thread blocks on source_blocked.wait() inside the producer's
    generator.  consumer_b raises after confirming the pump is running.
    abort() sets _stop and pushes ExceptionMarker, but the pump never sees
    _stop because it is parked inside next().  cleanup() → fanout.join()
    blocks the main thread indefinitely.
    """
    source_blocked = threading.Event()
    pump_started = threading.Event()

    def blocked_producer() -> Iterator[int]:
        source_blocked.wait()
        yield 1

    def consumer_a(blocked_producer: Iterator[int]) -> None:
        it = iter(blocked_producer)
        pump_started.set()
        for _item in it:
            pass

    def consumer_b(blocked_producer: Iterator[int]) -> None:
        pump_started.wait()
        raise ValueError("consumer_b fails early")

    pipeline_def = pipeline(
        name="test_cleanup_hang",
        params=EmptyParams,
        steps=[
            step("blocked_producer", fn=blocked_producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a),
            step("consumer_b", fn=consumer_b, on_error=OnError.STOP),
        ],
    )
    completed = threading.Event()

    def target() -> None:
        try:
            run(build_dag(pipeline_def), EmptyParams())
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()
    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline should not hang: cleanup() must not block on fanout.join() indefinitely.  See Issue #103."
    )


def test_given_build_arguments_raises_when_max_in_flight_active_then_pump_hangs_on_eof():
    """Production hang: build_arguments() raises before StepRunner is built.

    Pipeline topology (matches the master → all_mkt_data → ... → fetched_file
    chain in the issue comment):

        producer (max_in_flight=3, Iterator[int])  ← SyncFanout
            ├── consumer_a  (int -> None)
            └── consumer_b  (int, downloader)  ← build_arguments() raises

    The producer publishes via SyncFanout (two consumers).  The pump pushes
    items into both per-branch queues.  consumer_b's build_arguments()
    resolves the "downloader" resource and raises ValueError because the
    runtime resource_factories (passed to PipelineExecutor) lacks it
    (production case: include() didn't propagate the resource).  Before
    Fix #1, the SyncQueueIterator for "producer" was held in args and not
    closed on exception — its branch stayed in _active_branches and the
    pump's final EOF_MARKER push deadlocked.
    """

    class Downloader:
        pass

    def make_downloader() -> Downloader:
        return Downloader()

    def producer() -> Iterator[int]:
        for i in range(20):
            yield i

    consumer_a_results: list[int] = []

    def consumer_a(producer: int) -> None:
        consumer_a_results.append(producer)

    def consumer_b(producer: int, downloader: Downloader) -> None:
        raise AssertionError("consumer_b must not run")

    pipeline_def = pipeline(
        name="test_build_args_hang",
        params=EmptyParams,
        resources={"downloader": make_downloader},
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a),
            step("consumer_b", fn=consumer_b),
        ],
    )
    completed = threading.Event()

    def target() -> None:
        try:
            PipelineExecutor(build_dag(pipeline_def), resource_factories={}).execute(
                EmptyParams()
            )
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()
    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline should not hang: build_arguments() failures must not leak SyncQueueIterator branches that block the pump's EOF push.  See Issue #103."
    )


def test_given_build_arguments_raises_without_bounded_handoff_then_no_hang():
    """Same shape as Test C but with max_in_flight=1 → no SyncFanout.

    Without a fanout, there is no pump thread waiting on a leaked branch.
    The pipeline should fail fast.  This is the baseline confirming Test C's
    hang is specifically about the leaked SyncQueueIterator interacting
    with SyncFanout's EOF push.
    """

    class Downloader:
        pass

    def make_downloader() -> Downloader:
        return Downloader()

    def producer() -> Iterator[int]:
        for i in range(10):
            yield i

    def consumer_b(producer: int, downloader: Downloader) -> None:
        raise AssertionError("consumer_b must not run")

    pipeline_def = pipeline(
        name="test_build_args_no_fanout",
        params=EmptyParams,
        resources={"downloader": make_downloader},
        steps=[
            step("producer", fn=producer, max_in_flight=1),
            step("consumer_b", fn=consumer_b),
        ],
    )
    completed = threading.Event()

    def target() -> None:
        try:
            PipelineExecutor(build_dag(pipeline_def), resource_factories={}).execute(
                EmptyParams()
            )
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()
    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline with max_in_flight=1 should fail fast on build_arguments error — no SyncFanout, no leaked branch, no pump hang.  If this fails, something else is blocking."
    )


def test_given_consumer_raises_with_on_error_continue_then_pump_drains():
    """Consumer raises with OnError.CONTINUE (default) → no abort(), but
    _close_managed_streams() still fires and the branch is removed.  The
    pump should exit cleanly because its loop checks _active_branches.

    This validates the matrix entry "OnError.CONTINUE + consumer raises":
    no hang expected, pipeline completes.
    """

    def producer() -> Iterator[int]:
        for i in range(100):
            yield i

    def consumer(producer: Iterator[int]) -> None:
        for i, x in enumerate(producer):
            if i >= 3:
                raise ValueError("consumer fails early")

    pipeline_def = pipeline(
        name="test_continue_fanout",
        params=EmptyParams,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer", fn=consumer, on_error=OnError.CONTINUE),
        ],
    )
    completed = threading.Event()

    def target() -> None:
        try:
            run(build_dag(pipeline_def), EmptyParams())
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()
    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline with OnError.CONTINUE should drain cleanly — branch is closed by _close_managed_streams() even when on_error swallows the exception.  Hang here means the pump's EOF push is broken."
    )


def test_given_consumer_raises_with_on_error_stop_and_fanout_then_pump_drains():
    """Consumer raises with OnError.STOP → PipelineStopException →
    abort() sets _stop and pushes ExceptionMarker.  Pump should exit
    cleanly via the _stop check in its main loop.

    This validates the matrix entry "OnError.STOP + consumer raises +
    fanout": no hang expected, PipelineStopException propagates.
    """

    def producer() -> Iterator[int]:
        for i in range(100):
            yield i

    def consumer_b(producer: Iterator[int]) -> None:
        for i, _x in enumerate(producer):
            if i >= 3:
                raise ValueError("consumer_b fails early")

    def consumer_a(producer: Iterator[int]) -> None:
        for _x in producer:
            pass

    pipeline_def = pipeline(
        name="test_stop_fanout",
        params=EmptyParams,
        steps=[
            step("producer", fn=producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a, on_error=OnError.STOP),
            step("consumer_b", fn=consumer_b, on_error=OnError.STOP),
        ],
    )
    completed = threading.Event()
    raised: list[BaseException] = []

    def target() -> None:
        try:
            run(build_dag(pipeline_def), EmptyParams())
        except BaseException as exc:
            raised.append(exc)
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()
    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline with OnError.STOP should propagate PipelineStopException and exit — pump's _stop check breaks the loop.  Hang here means abort() is not setting _stop or the pump is ignoring it."
    )
    assert len(raised) >= 1
    assert isinstance(raised[0], PipelineStopException)
