from collections.abc import Iterator
from typing import NamedTuple


class IntParam(NamedTuple):
    x: int = 1


class KVParam(NamedTuple):
    pass


def _producer_scalar(x: int) -> int:
    return x * 2


def _consumer_scalar(producer: int) -> None:
    pass


def _producer_iter() -> Iterator[int]:
    yield from [1, 2, 3]


def _consumer_iter(producer: Iterator[int]) -> list[int]:
    return list(producer)


def _consumer_each(producer: int) -> int:
    return producer


def _consumer_list(producer: list[int]) -> int:
    return len(producer)


def _consumer_set(producer: set[int]) -> int:
    return len(producer)


def _consumer_tuple(producer: tuple[int, ...]) -> int:
    return len(producer)


def _producer_kv() -> Iterator[tuple[str, int]]:
    yield ("a", 1)
    yield ("b", 2)


def _consumer_dict(producer: dict[str, int]) -> int:
    return len(producer)


def _consumer_kv_list(producer: list[tuple[str, int]]) -> int:
    return len(producer)


COMPATIBILITY_TABLE = [
    dict(
        label="T -> T",
        producer_fn=_producer_scalar,
        consumer_fn=_consumer_scalar,
        params=IntParam,
    ),
    dict(
        label="Iterator[T] -> Iterator[T]",
        producer_fn=_producer_iter,
        consumer_fn=_consumer_iter,
    ),
    dict(
        label="Iterator[T] -> T (each mode)",
        producer_fn=_producer_iter,
        consumer_fn=_consumer_each,
    ),
    dict(
        label="Iterator[T] -> list[T]",
        producer_fn=_producer_iter,
        consumer_fn=_consumer_list,
        expected_materialized_deps=["producer"],
    ),
    dict(
        label="Iterator[T] -> set[T]",
        producer_fn=_producer_iter,
        consumer_fn=_consumer_set,
        expected_materialized_deps=["producer"],
    ),
    dict(
        label="Iterator[T] -> tuple[T, ...]",
        producer_fn=_producer_iter,
        consumer_fn=_consumer_tuple,
        expected_materialized_deps=["producer"],
    ),
    dict(
        label="Iterator[tuple[K,V]] -> list[tuple[K,V]]",
        producer_fn=_producer_kv,
        consumer_fn=_consumer_kv_list,
        expected_materialized_deps=["producer"],
    ),
    dict(
        label="Iterator[tuple[K,V]] -> dict[K,V]",
        producer_fn=_producer_kv,
        consumer_fn=_consumer_dict,
        expected_materialized_deps=["producer"],
    ),
]


def _consumer_iter_on_stop(producer: Iterator[int]) -> list[int]:
    return list(producer)


def _consumer_scalar_on_stop(producer: int) -> None:
    pass


COMPATIBILITY_TABLE_ON_ERROR_STOP = []


def _producer_iter_stop() -> Iterator[int]:
    yield from [1, 2, 3]


def _consumer_iter_stop(producer: Iterator[int]) -> list[int]:
    return list(producer)


VALIDATION_ERROR_CASES = [
    dict(
        label="T -> list[T] should raise",
        producer_fn=_producer_scalar,
        consumer_fn=_consumer_list,
        params=IntParam,
        expected_error="expects",
    ),
    dict(
        label="T -> Iterator[T] should raise",
        producer_fn=_producer_scalar,
        consumer_fn=_consumer_iter,
        params=IntParam,
        expected_error="expects",
    ),
    dict(
        label="T -> set[T] should raise",
        producer_fn=_producer_scalar,
        consumer_fn=_consumer_set,
        params=IntParam,
        expected_error="expects",
    ),
]
