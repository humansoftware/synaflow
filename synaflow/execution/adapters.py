import inspect
import functools
from typing import Any


def is_async_callable(handler: Any) -> bool:
    if inspect.iscoroutinefunction(handler):
        return True
    if hasattr(handler, "__call__") and inspect.iscoroutinefunction(handler.__call__):
        return True
    func = getattr(handler, "func", None)
    if func is not None and (
        inspect.iscoroutinefunction(func)
        or (hasattr(func, "__call__") and inspect.iscoroutinefunction(func.__call__))
    ):
        return True
    return False


def async_adapter(fn):
    """
    Wraps a synchronous function into an asynchronous one.

    This is useful for passing synchronous callables (like lambdas or regular functions)
    into asynchronous pipelines where components (like steps, materializers, or observers)
    are expected to be coroutine functions and will be unconditionally awaited.
    """
    if is_async_callable(fn):
        return fn

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    del wrapper.__wrapped__
    return wrapper
