from typing import Callable
from synaflow.core.types import MaterializeContext, ErrorMaterializeContext


def to_materializer(
    callable_or_type: Callable,
) -> Callable[[MaterializeContext], Callable]:
    """
    Wraps a simple, direct callable (like list, set, or custom function)
    to conform to the materializer factory protocol.
    """
    if not callable(callable_or_type):
        raise TypeError("to_materializer expects a callable argument")

    def factory(ctx: MaterializeContext) -> Callable:
        return callable_or_type

    return factory


def to_error_materializer(
    callable_or_type: Callable,
) -> Callable[[ErrorMaterializeContext], Callable]:
    """
    Wraps a simple, direct callable (like a logging/handler function)
    to conform to the error materializer factory protocol.
    """
    if not callable(callable_or_type):
        raise TypeError("to_error_materializer expects a callable argument")

    def factory(ctx: ErrorMaterializeContext) -> Callable:
        return callable_or_type

    return factory
