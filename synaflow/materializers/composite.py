import inspect
from collections.abc import Iterator
from typing import Any

from synaflow.core.type_compatibility import is_factory


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
        resolved = [
            m(ctx) if is_factory(m) else m for m in materializers if m is not None
        ]
        any_async = any(_is_async_callable(m) for m in resolved)

        if any_async:

            async def run_composite_materializers_async(value: Any) -> Any:
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

            return run_composite_materializers_async
        else:

            def run_composite_materializers_sync(value: Any) -> Any:
                if isinstance(value, Iterator):
                    value = list(value)

                res = None
                for m in resolved:
                    res = m(value)
                return res if res is not None else value

            return run_composite_materializers_sync

    return factory


def composite_error_materializer(*error_materializers):
    def factory(ctx):
        resolved = [
            em(ctx) if is_factory(em) else em
            for em in error_materializers
            if em is not None
        ]
        any_async = any(_is_async_callable(em) for em in resolved)

        if any_async:

            async def run_composite_error_materializers_async(error_ctx) -> None:
                for em in resolved:
                    if _is_async_callable(em):
                        await em(error_ctx)
                    else:
                        res = em(error_ctx)
                        if inspect.iscoroutine(res):
                            await res

            return run_composite_error_materializers_async
        else:

            def run_composite_error_materializers_sync(error_ctx) -> None:
                for em in resolved:
                    em(error_ctx)

            return run_composite_error_materializers_sync

    return factory
