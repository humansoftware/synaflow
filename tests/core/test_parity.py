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
            "Verifies sync runner rejects async pipelines (sync-only validation).",
        ),
        (
            "test_given_threadpool_start_and_await_when_max_in_flight_5_then_only_five_tasks_start",
            "Tests ThreadPoolExecutor sync concurrency behavior.",
        ),
        (
            "test_given_user_resource_with_close_when_used_as_param_then_executor_does_not_close_it",
            "Tests sync resource cleanup behavior.",
        ),
        (
            "test_itertools_tee_concurrent_reentry_crash",
            "Tests itertools.tee sync reentry concurrency crash.",
        ),
        (
            "test_given_stream_with_no_consumers_but_has_observers_then_stream_is_consumed",
            "Sync-specific test for observer consumption on unconsumed streams.",
        ),
        # LifecycleStream tests
        ("test_sync_lifecycle_stream", "Tests sync-only LifecycleStream class."),
        (
            "test_sync_lifecycle_stream_multiple_calls_after_terminal_state",
            "Tests sync-only LifecycleStream post-exhaustion behavior.",
        ),
        (
            "test_sync_lifecycle_stream_on_start_fails",
            "Tests sync-only LifecycleStream start callback failure.",
        ),
        (
            "test_sync_lifecycle_stream_empty",
            "Tests sync-only LifecycleStream empty stream handling.",
        ),
        (
            "test_sync_lifecycle_stream_immediate_error",
            "Tests sync-only LifecycleStream immediate error handling.",
        ),
    ]

    expected_async_only = [
        (
            "test_given_sync_stream_pipeline_when_run_asynchronously_then_raises",
            "Verifies async runner rejects sync stream pipelines.",
        ),
        (
            "test_given_async_generator_and_each_consumer_when_run_then_processed_concurrently",
            "Tests async engine concurrent generator handling (async-only).",
        ),
        (
            "test_given_async_generator_and_list_consumer_when_run_then_materialized",
            "Tests async engine list materialization of async generators.",
        ),
        (
            "test_given_async_generator_and_two_async_iterator_consumers_when_run_then_both_receive_items",
            "Tests async engine multi-consumer async iterator dispatch.",
        ),
        # Async-only observer handler tests
        (
            "test_given_async_def_handler_when_dispatched_then_awaited",
            "Tests async-only def observer handler dispatch.",
        ),
        (
            "test_given_partial_async_handler_when_dispatched_then_awaited",
            "Tests async-only partial observer handler dispatch.",
        ),
        (
            "test_given_callable_object_with_async_call_when_dispatched_then_awaited",
            "Tests async-only callable object observer handler.",
        ),
        (
            "test_given_terminal_stream_with_no_observers_bypass_validation",
            "Tests async-only validation bypass for unobserved terminal streams.",
        ),
        # AsyncLifecycleStream tests
        ("test_async_lifecycle_stream", "Tests async-only AsyncLifecycleStream class."),
        (
            "test_async_lifecycle_stream_multiple_calls_after_terminal_state",
            "Tests async-only AsyncLifecycleStream post-exhaustion behavior.",
        ),
        (
            "test_async_lifecycle_stream_on_start_fails_async_callback",
            "Tests async-only AsyncLifecycleStream async start callback failure.",
        ),
        (
            "test_async_lifecycle_stream_on_start_fails_sync_callback",
            "Tests async-only AsyncLifecycleStream sync start callback failure.",
        ),
        (
            "test_async_lifecycle_stream_empty_async",
            "Tests AsyncLifecycleStream with empty async generator.",
        ),
        (
            "test_async_lifecycle_stream_immediate_error_async",
            "Tests AsyncLifecycleStream with immediate async generator error.",
        ),
        (
            "test_async_lifecycle_stream_empty_sync",
            "Tests AsyncLifecycleStream with empty sync iterator.",
        ),
        (
            "test_async_lifecycle_stream_immediate_error_sync",
            "Tests AsyncLifecycleStream with immediate sync iterator error.",
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
