"""Helpers for detecting real sync/async context manager implementations."""

from __future__ import annotations

import inspect
from typing import Any


def _implements_method_without_mock_fallback(value: Any, method_name: str) -> bool:
    try:
        method = inspect.getattr_static(value, method_name)
    except AttributeError:
        return False
    return callable(method)


def is_async_context_manager_instance(value: Any) -> bool:
    return _implements_method_without_mock_fallback(
        value, "__aenter__"
    ) and _implements_method_without_mock_fallback(value, "__aexit__")


def is_sync_context_manager_instance(value: Any) -> bool:
    return _implements_method_without_mock_fallback(
        value, "__enter__"
    ) and _implements_method_without_mock_fallback(value, "__exit__")
