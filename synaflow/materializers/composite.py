import inspect
from collections.abc import Iterator
from typing import Any


def _is_async_callable(func: Any) -> bool:
    if func is None:
        return False
    if inspect.iscoroutinefunction(func):
        return True
    call_method = getattr(func, "__call__", None)
    if call_method and inspect.iscoroutinefunction(call_method):
        return True
    return False


def composite_materializer(*materializers):
    def factory(ctx):
        resolved = [m(ctx) for m in materializers if m is not None]
        any_async = any(_is_async_callable(m) for m in resolved)

        if any_async:

            async def concrete_async(value: Any) -> Any:
                if isinstance(value, Iterator):
                    value = list(value)

                res = None
                for m in resolved:
                    if _is_async_callable(m):
                        res = await m(value)
                    else:
                        res = m(value)
                        if inspect.iscoroutine(res):
                            res = await res
                return res if res is not None else value

            return concrete_async
        else:

            def concrete_sync(value: Any) -> Any:
                if isinstance(value, Iterator):
                    value = list(value)

                res = None
                for m in resolved:
                    res = m(value)
                return res if res is not None else value

            return concrete_sync

    return factory


def composite_error_materializer(*error_materializers):
    def factory(ctx):
        resolved = [em(ctx) for em in error_materializers if em is not None]
        any_async = any(_is_async_callable(em) for em in resolved)

        if any_async:

            async def concrete_async(exc: BaseException) -> None:
                for em in resolved:
                    if _is_async_callable(em):
                        await em(exc)
                    else:
                        res = em(exc)
                        if inspect.iscoroutine(res):
                            await res

            return concrete_async
        else:

            def concrete_sync(exc: BaseException) -> None:
                for em in resolved:
                    em(exc)

            return concrete_sync

    return factory
