"""Runtime validation helpers for compiled stream/value contracts."""

from __future__ import annotations

import inspect
from typing import Any


def _implements_callable_method(value: Any, method_name: str) -> bool:
    try:
        method = inspect.getattr_static(value, method_name)
    except AttributeError:
        return False
    return callable(method)


def is_real_sync_iterator_instance(value: Any) -> bool:
    return _implements_callable_method(
        value, "__iter__"
    ) and _implements_callable_method(value, "__next__")


def is_real_async_iterator_instance(value: Any) -> bool:
    return _implements_callable_method(
        value, "__aiter__"
    ) and _implements_callable_method(value, "__anext__")
