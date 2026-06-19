import types
import inspect
from collections.abc import AsyncGenerator, AsyncIterator, Generator, Iterable, Iterator
from typing import Any, Callable, Tuple, Union, get_args, get_origin


def is_factory(func: Callable) -> bool:
    if not callable(func):
        return False
    sig = inspect.signature(func)
    for param in sig.parameters.values():
        if param.name in ("ctx", "context") or "MaterializeContext" in str(
            param.annotation
        ):
            return True
    return False


SCALAR_TYPES = {int, float, str, bool, bytes, type(None)}
COLLECTION_ORIGINS = {
    list,
    set,
    tuple,
    dict,
    Generator,
    Iterator,
    Iterable,
    AsyncGenerator,
    AsyncIterator,
}


class ListType:
    """Wrapper to represent a runtime-resolved list of a specific type."""

    def __init__(self, inner_type: Any):
        self.inner_type = inner_type

    def __repr__(self):
        return f"ListType({self.inner_type})"


def is_type_compatible(producer_type: Any, consumer_type: Any) -> bool:
    """Checks if a producer output type satisfies a consumer input type."""
    if producer_type is None or consumer_type is None:
        return True

    if producer_type == consumer_type:
        return True

    producer_origin = get_origin(producer_type)
    consumer_origin = get_origin(consumer_type)

    if _is_union(producer_type, producer_origin) and _is_union(
        consumer_type, consumer_origin
    ):
        return _all_producer_types_match_any_consumer_type(producer_type, consumer_type)

    if _is_union(producer_type, producer_origin):
        return _all_producer_types_match_consumer(producer_type, consumer_type)

    is_producer_iterable = _is_iterable(producer_type, producer_origin)
    is_consumer_iterable = _is_iterable(consumer_type, consumer_origin)

    if is_producer_iterable:
        return _check_iterable_producer_compatibility(
            producer_type, consumer_type, is_consumer_iterable
        )

    if _is_union(consumer_type, consumer_origin):
        return _producer_matches_any_consumer_type(producer_type, consumer_type)

    if is_consumer_iterable:
        return False

    if is_scalar(consumer_type):
        return _check_scalar_compatibility(producer_type, consumer_type)

    return True


def _all_producer_types_match_any_consumer_type(
    producer_type: Any, consumer_type: Any
) -> bool:
    return all(
        any(is_type_compatible(p, c) for c in get_args(consumer_type))
        for p in get_args(producer_type)
    )


def _all_producer_types_match_consumer(producer_type: Any, consumer_type: Any) -> bool:
    return all(is_type_compatible(p, consumer_type) for p in get_args(producer_type))


def _producer_matches_any_consumer_type(producer_type: Any, consumer_type: Any) -> bool:
    return any(is_type_compatible(producer_type, c) for c in get_args(consumer_type))


def _get_consumer_build_type(tp: Any) -> Any:
    origin = get_origin(tp) or tp
    if origin is dict:
        return _get_dict_pair_type(tp)
    return get_inner_type(tp)


def _is_dict_type(tp: Any) -> bool:
    return get_origin(tp) is dict


def _get_dict_pair_type(tp: Any) -> Any:
    args = get_args(tp)
    if len(args) == 2:
        return Tuple[args[0], args[1]]
    return None


def _check_iterable_producer_compatibility(
    producer_type: Any, consumer_type: Any, is_consumer_iterable: bool
) -> bool:
    producer_inner = get_inner_type(producer_type)
    if producer_inner is None:
        if producer_type not in COLLECTION_ORIGINS:
            return False

    consumer_origin = get_origin(consumer_type)

    if _is_union(consumer_type, consumer_origin):
        return is_type_compatible(producer_inner, consumer_type)

    if is_consumer_iterable:
        consumer_inner = _get_consumer_build_type(consumer_type)
        if consumer_inner is not None:
            if is_type_compatible(producer_inner, consumer_inner):
                return True
            if _is_dict_type(producer_type):
                pair_inner = _get_dict_pair_type(producer_type)
                if pair_inner is not None and is_type_compatible(
                    pair_inner, consumer_inner
                ):
                    return True
            return False
        return True

    if is_scalar(consumer_type):
        return is_type_compatible(producer_inner, consumer_type)

    return False


def _check_scalar_producer_to_iterable_consumer(
    producer_type: Any, consumer_type: Any
) -> bool:
    producer_inner = get_inner_type(producer_type)
    consumer_inner = get_inner_type(consumer_type)

    if consumer_inner is None:
        return True

    if producer_inner is None:
        return is_type_compatible(producer_type, consumer_inner)

    return is_type_compatible(producer_inner, consumer_inner)


def _check_scalar_compatibility(producer_type: Any, consumer_type: Any) -> bool:
    producer_inner = get_inner_type(producer_type)
    if producer_inner is not None:
        return is_type_compatible(producer_inner, consumer_type)
    return is_scalar(producer_type) and producer_type == consumer_type


def _is_union(tp: Any, origin: Any) -> bool:
    return origin is types.UnionType or origin is __import__("typing").Union


def _is_iterable(tp: Any, origin: Any) -> bool:
    if isinstance(tp, ListType):
        return True
    if origin is not None:
        return origin in COLLECTION_ORIGINS
    return tp in COLLECTION_ORIGINS


def is_iterable_type(tp: Any) -> bool:
    if tp is None:
        return False
    if isinstance(tp, ListType):
        return True
    return _is_iterable(tp, get_origin(tp))


def is_scalar(tp: Any) -> bool:
    if tp is None:
        return False
    if tp in SCALAR_TYPES:
        return True
    origin = get_origin(tp)
    if origin is not None:
        if origin is types.UnionType or origin is Union:
            return all(is_scalar(a) for a in get_args(tp))
        return False
    return tp not in COLLECTION_ORIGINS


def get_inner_type(tp: Any) -> Any:
    if isinstance(tp, ListType):
        return tp.inner_type
    args = get_args(tp)
    if args:
        return args[0]
    return None


def get_type_name(tp: Any) -> str:
    if tp is None or tp is type(None):
        return "None"

    if tp in (Iterator, Generator, AsyncIterator, AsyncGenerator):
        return "Stream"

    origin = get_origin(tp)
    if origin is not None:
        arg_names = ", ".join(get_type_name(a) for a in get_args(tp))
        if origin in (Iterator, Generator, AsyncIterator, AsyncGenerator):
            origin_name = "Stream"
        else:
            origin_name = getattr(origin, "__name__", str(origin))
        return f"{origin_name}[{arg_names}]"
    return getattr(tp, "__name__", str(tp))


def is_materialized_consumer(tp: Any) -> bool:
    """Checks if a consumer type requires an eagerly materialized collection."""
    if tp is None:
        return False
    if tp in (list, set, tuple, dict):
        return True

    origin = get_origin(tp)
    if origin in (list, set, tuple, dict):
        return True
    if origin in (Iterator, Generator, Iterable):
        return False
    if is_scalar(tp):
        return False
    if _is_union(tp, origin):
        return any(is_materialized_consumer(a) for a in get_args(tp))

    return False


def is_sync_stream_type(tp: Any) -> bool:
    if tp is None:
        return False
    origin = get_origin(tp) or tp
    if origin in (Iterator, Generator):
        return True
    if _is_union(tp, get_origin(tp)):
        return any(is_sync_stream_type(a) for a in get_args(tp))
    return False


def is_async_stream_type(tp: Any) -> bool:
    if tp is None:
        return False
    origin = get_origin(tp) or tp
    if origin in (AsyncIterator, AsyncGenerator):
        return True
    if _is_union(tp, get_origin(tp)):
        return any(is_async_stream_type(a) for a in get_args(tp))
    return False
