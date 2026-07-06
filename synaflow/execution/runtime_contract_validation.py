"""Runtime validation helpers for compiled execution contracts.

These helpers are intentionally narrow: the DAG builder decides publication
strategy up front, and executors only use runtime inspection to validate that a
returned object actually satisfies the already-compiled contract or can be
safely wrapped for lifecycle/cleanup.
"""

from __future__ import annotations

import inspect
from typing import Any


def _has_callable_protocol_method(value: Any, method_name: str) -> bool:
    try:
        method = inspect.getattr_static(value, method_name)
    except AttributeError:
        return False
    return callable(method)


def satisfies_sync_iterator_contract(value: Any) -> bool:
    return _has_callable_protocol_method(
        value, "__iter__"
    ) and _has_callable_protocol_method(value, "__next__")


def satisfies_async_iterator_contract(value: Any) -> bool:
    return _has_callable_protocol_method(
        value, "__aiter__"
    ) and _has_callable_protocol_method(value, "__anext__")
