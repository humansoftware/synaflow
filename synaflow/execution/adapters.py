import inspect
import functools


def async_adapter(fn):
    """
    Wraps a synchronous function into an asynchronous one.

    This is useful for passing synchronous callables (like lambdas or regular functions)
    into asynchronous pipelines where components (like steps, materializers, or observers)
    are expected to be coroutine functions and will be unconditionally awaited.
    """
    if (
        inspect.iscoroutinefunction(fn)
        or inspect.isgeneratorfunction(fn)
        or inspect.isasyncgenfunction(fn)
        or (hasattr(fn, "__call__") and inspect.iscoroutinefunction(fn.__call__))
    ):
        return fn

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapper
