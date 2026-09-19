"""Composite materializers: chain several materializers (or error
materializers) so each stage feeds the next.  Factories resolve their
stages against the usual MaterializeContext and pick a sync or async
runner based on the resolved stages."""

import inspect
from collections.abc import Iterator
from typing import Any, Callable

from synaflow.core.adapters import is_async_callable
from synaflow.core.type_compatibility import is_factory


def composite_materializer(
    *materializers: Callable[..., Any],
) -> Callable[[Any], Callable[..., Any]]:
    def factory(ctx):
        resolved = [
            m(ctx) if is_factory(m) else m for m in materializers if m is not None
        ]
        any_async = any(is_async_callable(m) for m in resolved)

        if any_async:

            async def run_composite_materializers_async(value: Any) -> Any:
                if isinstance(value, Iterator):
                    value = list(value)

                res = None
                for m in resolved:
                    if is_async_callable(m):
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


def composite_error_materializer(
    *error_materializers: Callable[..., Any],
) -> Callable[[Any], Callable[..., Any]]:
    def factory(ctx):
        resolved = [
            em(ctx) if is_factory(em) else em
            for em in error_materializers
            if em is not None
        ]
        any_async = any(is_async_callable(em) for em in resolved)

        if any_async:

            async def run_composite_error_materializers_async(error_ctx) -> None:
                for em in resolved:
                    if is_async_callable(em):
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
