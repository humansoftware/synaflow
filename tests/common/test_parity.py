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
                            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                                test_funcs.add(node.name)
                            elif isinstance(node, ast.AsyncFunctionDef) and node.name.startswith("test_"):
                                test_funcs.add(node.name)
                    except SyntaxError:
                        continue
    return test_funcs


def test_sync_async_test_parity():
    sync_dir = Path(__file__).parent.parent / "test_sync"
    async_dir = Path(__file__).parent.parent / "test_async"

    sync_tests = get_test_functions_in_dir(sync_dir)
    async_tests = get_test_functions_in_dir(async_dir)

    # Some tests only make sense in their specific contexts
    expected_sync_only = {
        "test_given_async_pipeline_when_run_synchronously_then_raises"
    }
    
    expected_async_only = {
        "test_given_sync_stream_pipeline_when_run_asynchronously_then_raises",
        "test_given_async_generator_and_each_consumer_when_run_then_processed_concurrently",
        "test_given_async_generator_and_list_consumer_when_run_then_materialized",
        "test_given_async_generator_and_two_async_iterator_consumers_when_run_then_both_receive_items",
    }

    # Remove expected differences
    sync_normalized = sync_tests - expected_sync_only
    async_normalized = async_tests - expected_async_only

    missing_in_async = sync_normalized - async_normalized
    missing_in_sync = async_normalized - sync_normalized

    error_msg = []
    if missing_in_async:
        error_msg.append(f"Tests found in sync but missing in async: {missing_in_async}")
    if missing_in_sync:
        error_msg.append(f"Tests found in async but missing in sync: {missing_in_sync}")

    assert not error_msg, "\n".join(error_msg)
