from collections.abc import Iterator
from typing import Any


def composite_materializer(*materializers):
    def factory(ctx):
        resolved = [m(ctx) for m in materializers if m is not None]

        def concrete(value: Any) -> Any:
            if isinstance(value, Iterator):
                value = list(value)

            res = None
            for m in resolved:
                res = m(value)
            return res if res is not None else value

        return concrete

    return factory


def composite_error_materializer(*error_materializers):
    def factory(ctx):
        resolved = [em(ctx) for em in error_materializers if em is not None]

        def concrete(exc: BaseException) -> None:
            for em in resolved:
                em(exc)

        return concrete

    return factory
