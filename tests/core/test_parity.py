import ast
import os
from pathlib import Path


def get_test_functions_in_dir(directory: Path) -> set[str]:
    test_funcs = set()
    for root, _, files in os.walk(directory):
        for f in files:
            if f.startswith("test_") and f.endswith(".py"):
                path = Path(root) / f
                with open(path, "r", encoding="utf-8") as file:
                    try:
                        tree = ast.parse(file.read())
                        for node in ast.walk(tree):
                            if isinstance(
                                node, ast.FunctionDef
                            ) and node.name.startswith("test_"):
                                test_funcs.add(node.name)
                            elif isinstance(
                                node, ast.AsyncFunctionDef
                            ) and node.name.startswith("test_"):
                                test_funcs.add(node.name)
                    except SyntaxError:
                        continue
    return test_funcs


def test_sync_async_test_parity():
    sync_dir = Path(__file__).parent.parent / "execution" / "sync_engine"
    async_dir = Path(__file__).parent.parent / "execution" / "async_engine"

    assert sync_dir.exists() and sync_dir.is_dir(), (
        f"Sync test directory not found: {sync_dir}"
    )
    assert async_dir.exists() and async_dir.is_dir(), (
        f"Async test directory not found: {async_dir}"
    )

    sync_tests = get_test_functions_in_dir(sync_dir)
    async_tests = get_test_functions_in_dir(async_dir)

    # Some tests only make sense in their specific contexts
    expected_sync_only = [
        (
            "test_given_threadpool_start_and_await_when_max_in_flight_5_then_only_five_tasks_start",
            "Tests ThreadPoolExecutor sync concurrency behavior. The async engine concurrency is managed natively by asyncio Tasks/Semaphores, tested under different async-specific names.",
        ),
        (
            "test_itertools_tee_concurrent_reentry_crash",
            "Specifically tests concurrency crash protection using the standard library's itertools.tee which is sync-only. The async engine does not use itertools.tee.",
        ),
        (
            "test_given_stream_with_no_consumers_but_has_observers_then_stream_is_consumed",
            "Tests sync-specific observer drain for unconsumed sync generator outputs. The async engine handles observers natively during async generator iteration.",
        ),
        (
            "test_given_max_in_flight_fanout_when_terminal_consumers_do_not_iterate_then_run_completes",
            "Tests the sync-only SyncFanout lazy-start/queue handoff path when terminal consumers never iterate. The async engine uses a different queue/task handoff mechanism.",
        ),
        (
            "test_given_fanout_to_submit_and_await_barrier_when_max_in_flight_then_await_steps_drain",
            "Tests the sync-only SyncFanout plus barrier-only done-step topology that can deadlock when EACH await steps are left lazy. The async engine uses a different handoff mechanism.",
        ),
        # Unit tests for ``wait_for_workers_after_shutdown`` — sync engine only.
        # The async engine does not use a ThreadPoolExecutor; worker lifecycle
        # is governed by the asyncio event loop.
        (
            "test_returns_when_no_threads",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_filters_threads_outside_prefix",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_logs_once_then_returns_when_workers_clear_first_poll",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_logs_each_log_window_until_workers_clear",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_logs_at_most_once_per_window_even_with_many_short_polls",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_logs_multiple_workers_in_single_line",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_process_pid_defaults_to_os_getpid",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_custom_thread_name_prefix_is_honoured",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_poll_seconds_passed_through_to_sleep",
            "Unit test for the sync-only ``wait_for_workers_after_shutdown`` helper.",
        ),
        (
            "test_given_async_dag_passed_to_sync_run_then_raises_engine_mismatch",
            "Engine-mismatch test is per-engine by nature: sync engine rejects "
            "an async Dag. The async equivalent is its own test in the async dir.",
        ),
        # Unit tests for ``SyncFanout.join()`` — sync engine only.
        # The async engine does not use ``Thread.join()``; its equivalent
        # is ``asyncio.wait_for(..., timeout)``.
        (
            "test_join_returns_true_when_pump_exits",
            "Unit test for the sync-only ``SyncFanout.join()`` method.",
        ),
        (
            "test_join_returns_true_for_multiple_branches_under_abort_contention",
            "Unit test for the sync-only ``SyncFanout.join()`` method.",
        ),
        (
            "test_join_returns_true_when_pump_not_started",
            "Unit test for the sync-only ``SyncFanout.join()`` method.",
        ),
        (
            "test_join_returns_false_when_pump_is_stuck",
            "Unit test for the sync-only ``SyncFanout.join()`` method.",
        ),
    ]

    expected_async_only = [
        (
            "test_given_async_generator_and_each_consumer_when_run_then_processed_concurrently",
            "Tests concurrent execution of async generator outputs across EACH consumers. Sync engine only executes sequentially since threads/concurrency in sync pipelines doesn't apply to async generators.",
        ),
        (
            "test_given_async_generator_and_list_consumer_when_run_then_materialized",
            "Tests materialization of async generators, which are invalid in the sync engine.",
        ),
        (
            "test_given_async_generator_and_two_async_iterator_consumers_when_run_then_both_receive_items",
            "Tests async engine multi-consumer async generator duplication. Sync engine uses standard iterators and handles duplication via sync queues.",
        ),
        # Async-only observer handler tests
        (
            "test_given_async_def_handler_when_dispatched_then_awaited",
            "Tests awaiting an async def observer handler. The sync engine does not support async observer handlers.",
        ),
        (
            "test_given_partial_async_handler_when_dispatched_then_awaited",
            "Tests awaiting a partial function wrapping an async observer handler. The sync engine does not support async observer handlers.",
        ),
        (
            "test_given_callable_object_with_async_call_when_dispatched_then_awaited",
            "Tests awaiting a callable object with an async def __call__. The sync engine does not support async observer handlers.",
        ),
        (
            "test_given_terminal_stream_with_no_observers_bypass_validation",
            "Tests async-only validation bypass for unobserved terminal async streams. Sync engine doesn't have an equivalent bypass because all sync generator outputs are consumed or validated under sync rules.",
        ),
        (
            "test_given_sync_dag_passed_to_async_run_then_raises_engine_mismatch",
            "Engine-mismatch test is per-engine by nature: async engine rejects "
            "a sync Dag. The sync equivalent is its own test in the sync dir.",
        ),
        (
            "test_given_blocking_step_when_another_step_raises_then_run_graph_hangs",
            "Tests the async-only `_run_graph()` recovery path: asyncio task cancellation interrupts blocked awaits natively, while the sync engine relies on the user to ensure steps make progress (no equivalent guard test).",
        ),
    ]

    # Remove expected differences
    sync_normalized = sync_tests - {t[0] for t in expected_sync_only}
    async_normalized = async_tests - {t[0] for t in expected_async_only}

    missing_in_async = sync_normalized - async_normalized
    missing_in_sync = async_normalized - sync_normalized

    error_msg = []
    if missing_in_async:
        error_msg.append(
            f"Tests found in sync but missing in async: {missing_in_async}"
        )
    if missing_in_sync:
        error_msg.append(f"Tests found in async but missing in sync: {missing_in_sync}")

    assert not error_msg, "\n".join(error_msg)
