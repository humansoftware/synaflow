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
            "test_given_async_pipeline_when_run_synchronously_then_raises",
            "Verifies sync runner rejects async pipelines. The async runner has no equivalent since it natively executes async pipelines, and instead tests the inverse rejection (sync stream rejection).",
        ),
        (
            "test_given_threadpool_start_and_await_when_max_in_flight_5_then_only_five_tasks_start",
            "Tests ThreadPoolExecutor sync concurrency behavior. The async engine concurrency is managed natively by asyncio Tasks/Semaphores, tested under different async-specific names.",
        ),
        (
            "test_given_user_resource_with_close_when_used_as_param_then_executor_does_not_close_it",
            "Tests sync resource __exit__/close cleanup. Async resources use __aexit__ which are tested in different async-only test cases.",
        ),
        (
            "test_itertools_tee_concurrent_reentry_crash",
            "Specifically tests concurrency crash protection using the standard library's itertools.tee which is sync-only. The async engine does not use itertools.tee.",
        ),
        (
            "test_given_stream_with_no_consumers_but_has_observers_then_stream_is_consumed",
            "Tests sync-specific observer drain for unconsumed sync generator outputs. The async engine handles observers natively during async generator iteration.",
        ),
        # LifecycleStream tests
        (
            "test_sync_lifecycle_stream",
            "Tests the synchronous LifecycleStream class. The async equivalent tests AsyncLifecycleStream instead.",
        ),
        (
            "test_sync_lifecycle_stream_multiple_calls_after_terminal_state",
            "Tests sync-only terminal state protection on LifecycleStream. The async engine equivalent tests AsyncLifecycleStream.",
        ),
        (
            "test_sync_lifecycle_stream_on_start_fails",
            "Tests sync LifecycleStream start hook failure. The async engine equivalent tests separate async-callback and sync-callback failure variants.",
        ),
        (
            "test_sync_lifecycle_stream_empty",
            "Tests sync LifecycleStream empty stream iteration. The async engine tests empty async/sync iterator variants.",
        ),
        (
            "test_sync_lifecycle_stream_immediate_error",
            "Tests sync LifecycleStream immediate iterator error propagation. The async engine tests immediate error async/sync iterator variants.",
        ),
    ]

    expected_async_only = [
        (
            "test_given_sync_stream_pipeline_when_run_asynchronously_then_raises",
            "Verifies async runner rejects sync stream pipelines. The sync runner has no equivalent since it natively executes sync pipelines, and instead tests the inverse rejection (async pipeline rejection).",
        ),
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
        # AsyncLifecycleStream tests
        (
            "test_async_lifecycle_stream",
            "Tests the async AsyncLifecycleStream class. The sync equivalent tests LifecycleStream instead.",
        ),
        (
            "test_async_lifecycle_stream_multiple_calls_after_terminal_state",
            "Tests async terminal state protection on AsyncLifecycleStream. The sync engine equivalent tests LifecycleStream.",
        ),
        (
            "test_async_lifecycle_stream_on_start_fails_async_callback",
            "Tests async start hook failure on AsyncLifecycleStream. Sync engine doesn't support async callbacks.",
        ),
        (
            "test_async_lifecycle_stream_on_start_fails_sync_callback",
            "Tests sync start hook failure on AsyncLifecycleStream. Sync engine doesn't support async streams.",
        ),
        (
            "test_async_lifecycle_stream_empty_async",
            "Tests AsyncLifecycleStream with an empty async generator. Sync engine doesn't support async generators.",
        ),
        (
            "test_async_lifecycle_stream_immediate_error_async",
            "Tests AsyncLifecycleStream with an immediate async generator error. Sync engine doesn't support async generators.",
        ),
        (
            "test_async_lifecycle_stream_empty_sync",
            "Tests AsyncLifecycleStream with an empty sync iterator in an async context. Sync engine doesn't consume sync iterators asynchronously.",
        ),
        (
            "test_async_lifecycle_stream_immediate_error_sync",
            "Tests AsyncLifecycleStream with an immediate sync iterator error in an async context. Sync engine doesn't consume sync iterators asynchronously.",
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
