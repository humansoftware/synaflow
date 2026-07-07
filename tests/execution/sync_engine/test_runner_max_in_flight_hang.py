"""Regression tests for Issue #103: PipelineExecutor hangs on step failure.

Three independent hang mechanisms are covered:

  1. Cause 2 (cleanup hang): The SyncFanout _pump thread blocks on a stuck
     source iterator.  cleanup() calls fanout.join() which never returns.

  2. Cause 1 (_run_graph hang): An in-flight future never completes (blocked
     on I/O).  _run_graph() waits on cond.wait() forever because the
     blocking step stays in running_tasks.

  3. Production scenario (Test C): build_arguments() raises before the
     StepRunner is constructed.  The leaked SyncQueueIterator keeps its
     branch in _active_branches, so the pump's final EOF push deadlocks.

Each test runs the pipeline in a daemon watchdog thread with a 5 s timeout.
The assertions expect the pipeline to EXIT within the timeout — the bug is
fixed, the framework no longer hangs.
"""

import threading
import time
from collections.abc import Iterator
from typing import NamedTuple

from synaflow import OnError, pipeline, run, step
from synaflow.core.exceptions import PipelineStopException
from synaflow.execution.sync_engine import PipelineExecutor


class EmptyParams(NamedTuple):
    pass


# ---------------------------------------------------------------------------
# Test A — cleanup() hang via stuck SyncFanout _pump thread
# ---------------------------------------------------------------------------


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
    source_blocked = threading.Event()  # never set → source blocks forever
    pump_started = threading.Event()

    def blocked_producer() -> Iterator[int]:
        source_blocked.wait()  # blocks forever (simulating stuck I/O)
        yield 1  # unreachable

    def consumer_a(blocked_producer: Iterator[int]) -> None:
        # Access the iterator — this lazily starts the _pump thread.
        it = iter(blocked_producer)
        pump_started.set()
        # Now try to consume.  _queue.get() will block until an item or
        # ExceptionMarker arrives.
        for _item in it:
            pass

    def consumer_b(blocked_producer: Iterator[int]) -> None:
        pump_started.wait()  # guarantee the pump thread is alive
        raise ValueError("consumer_b fails early")

    pipeline_def = pipeline(
        name="test_cleanup_hang",
        params=EmptyParams,
        steps=[
            step("blocked_producer", fn=blocked_producer, max_in_flight=3),
            step("consumer_a", fn=consumer_a),
            # on_error=STOP ensures the failure propagates to step_done
            # so fatal_error is set and the executor attempts to abort the
            # pipeline (which calls fanout.abort() under the hood).  Default
            # CONTINUE would swallow the exception.
            step("consumer_b", fn=consumer_b, on_error=OnError.STOP),
        ],
    )

    completed = threading.Event()

    def target() -> None:
        try:
            run(pipeline_def, EmptyParams())
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()

    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline should not hang: cleanup() must not block on "
        "fanout.join() indefinitely.  See Issue #103."
    )


# ---------------------------------------------------------------------------
# Test B — _run_graph() hang via in-flight future that never completes
# ---------------------------------------------------------------------------


def test_given_blocking_step_when_another_step_raises_then_run_graph_hangs():
    """Cause 1 (_run_graph hang): a blocking step keeps running_tasks non-empty.

    Pipeline topology (both steps are independent, same level):

        blocking_step  (ALL mode)  ← blocks on Event().wait() forever
        failing_step   (ALL mode)  ← raises ValueError (on_error=STOP)

    When failing_step raises (with on_error=STOP), step_done sets fatal_error
    but does NOT cancel the blocking step's future.  running_tasks still
    contains "blocking_step", so the main loop spins on cond.wait() forever
    unless _run_graph has a non-blocking shutdown path.  No SyncFanout or
    max_in_flight is required — any I/O-bound step at the same topological
    level is vulnerable.
    """
    step_blocked = threading.Event()  # never set
    blocking_started = threading.Event()

    def blocking_step() -> None:
        blocking_started.set()  # signal we are now in running_tasks
        step_blocked.wait()  # blocks forever (simulating stuck I/O)

    def failing_step() -> None:
        blocking_started.wait()  # ensure blocking_step is in-flight first
        raise ValueError("failing_step raises")

    pipeline_def = pipeline(
        name="test_run_graph_hang",
        params=EmptyParams,
        steps=[
            step("blocking_step", fn=blocking_step),
            # on_error=STOP ensures the failure propagates to step_done and
            # fatal_error is set.  In default (CONTINUE) mode the StepRunner
            # intentionally swallows exceptions, so this test would not
            # exercise the abort path.
            step("failing_step", fn=failing_step, on_error=OnError.STOP),
        ],
    )

    completed = threading.Event()

    def target() -> None:
        try:
            run(pipeline_def, EmptyParams())
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()

    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline should not hang: _run_graph() must not block on "
        "cond.wait() indefinitely when one step fails.  See Issue #103."
    )


# ---------------------------------------------------------------------------
# Test C — production scenario: build_arguments() leaks a SyncQueueIterator
# ---------------------------------------------------------------------------


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
        # Never reached — build_arguments raises on downloader first.
        raise AssertionError("consumer_b must not run")

    # DAG declares downloader (so DAG build succeeds and consumer_b's
    # inputs_available() returns True via the dag.resource_factories
    # shortcut).  We then run the executor with empty runtime
    # resource_factories to simulate include()'s broken propagation.
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
            PipelineExecutor(
                pipeline_def.dag,
                resource_factories={},  # production bug: downloader missing
            ).execute(EmptyParams())
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()

    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline should not hang: build_arguments() failures must not leak "
        "SyncQueueIterator branches that block the pump's EOF push.  "
        "See Issue #103."
    )


# ---------------------------------------------------------------------------
# Test D — build_arguments() failure WITHOUT bounded handoff completes fast
# ---------------------------------------------------------------------------


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
            step("producer", fn=producer, max_in_flight=1),  # no fanout
            step("consumer_b", fn=consumer_b),
        ],
    )

    completed = threading.Event()

    def target() -> None:
        try:
            PipelineExecutor(
                pipeline_def.dag,
                resource_factories={},  # downloader missing at runtime
            ).execute(EmptyParams())
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()

    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline with max_in_flight=1 should fail fast on build_arguments "
        "error — no SyncFanout, no leaked branch, no pump hang.  If this "
        "fails, something else is blocking."
    )


# ---------------------------------------------------------------------------
# Test E — OnError.CONTINUE, consumer raises mid-iteration
# ---------------------------------------------------------------------------


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
            # drain a few items then bail

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
            run(pipeline_def, EmptyParams())
        except Exception:
            pass
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()

    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline with OnError.CONTINUE should drain cleanly — branch is "
        "closed by _close_managed_streams() even when on_error swallows "
        "the exception.  Hang here means the pump's EOF push is broken."
    )


# ---------------------------------------------------------------------------
# Test F — OnError.STOP, consumer raises mid-iteration with fanout
# ---------------------------------------------------------------------------


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
            run(pipeline_def, EmptyParams())
        except BaseException as exc:
            raised.append(exc)
        completed.set()

    watchdog = threading.Thread(target=target, daemon=True)
    watchdog.start()

    hang_detected = not completed.wait(timeout=5.0)
    assert not hang_detected, (
        "Pipeline with OnError.STOP should propagate PipelineStopException "
        "and exit — pump's _stop check breaks the loop.  Hang here means "
        "abort() is not setting _stop or the pump is ignoring it."
    )
    assert len(raised) >= 1
    assert isinstance(raised[0], PipelineStopException)


# ---------------------------------------------------------------------------
# Test G — Thread leak regression (Issue #103, CI hang after #103 fix)
# ---------------------------------------------------------------------------


def test_no_non_daemon_worker_leak_after_executor_shutdown():
    """``synaflow`` is responsible for the full lifecycle of every thread
    it creates; the pipeline author must never need to clean up after us.
    After a ``PipelineExecutor`` cleans up, no ``synaflow-worker_*`` thread
    spawned by ``_DaemonThreadPoolExecutor`` may remain alive as a
    non-daemon thread.

    CI hangs were traced to two layers, both framework bugs: (1) the
    production hang in Issue #103 (unbounded ``cond.wait()`` /
    ``fanout.join()``); (2) once that was fixed, the leaked
    ``ThreadPoolExecutor`` workers stayed alive as non-daemon threads and
    ``python -m pytest`` then waited for them forever at session
    teardown, printing ``673 passed in 4.70s`` and never returning.

    The fix is to spawn daemon workers via ``_DaemonThreadPoolExecutor``;
    this test is the contract: any regression that puts a worker back on
    the ``daemon=False`` path will turn into a CI hang in production,
    and this test will catch it locally in 0.1 s.
    """
    step_blocked = threading.Event()  # never set - keeps the worker stuck
    blocking_started = threading.Event()

    def blocking_step() -> None:
        blocking_started.set()
        step_blocked.wait()  # blocks forever

    def failing_step() -> None:
        blocking_started.wait()
        raise ValueError("failing_step raises")

    pipeline_def = pipeline(
        name="test_thread_leak_regression",
        params=EmptyParams,
        steps=[
            step("blocking_step", fn=blocking_step),
            step("failing_step", fn=failing_step, on_error=OnError.STOP),
        ],
    )

    executor = PipelineExecutor(pipeline_def.dag)
    try:
        executor.execute(EmptyParams())
    except BaseException:
        pass  # expected: PipelineStopException / ValueError

    # Yield to let the executor complete its cleanup.
    time.sleep(0.1)

    leaked = [
        t
        for t in threading.enumerate()
        if t.name.startswith("synaflow-worker_") and t.is_alive() and not t.daemon
    ]
    assert not leaked, (
        f"Non-daemon synaflow-worker thread(s) leaked after shutdown: "
        f"{[t.name for t in leaked]}.  See Issue #103 — these threads "
        f"block pytest session teardown and make CI runs hang for hours."
    )
