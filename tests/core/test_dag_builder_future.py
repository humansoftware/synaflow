"\nTests for expected DagBuilder behavior not yet implemented.\nxfail = expected to fail until implemented.\n"

from synaflow.core.dag_builder import build_dag
from collections.abc import Iterator
from typing import NamedTuple
from .conftest import build_minimal_dag


class KVParam(NamedTuple):
    pass


def test_given_iterator_of_pairs_when_consumer_wants_dict_then_dag_builds():

    def producer() -> Iterator[tuple[str, int]]:
        yield ("a", 1)

    def consumer(producer: dict[str, int]) -> int:
        return len(producer)

    p = build_minimal_dag(producer_fn=producer, consumer_fn=consumer, params=KVParam)
    assert build_dag(p).needs_materialize("producer") is True


def test_given_dict_producer_when_consumer_wants_iterator_of_items_then_producer_stays_lazy():

    class DictParam(NamedTuple):
        data: dict[str, int] = {"a": 1}

    def producer(data: dict[str, int]) -> dict[str, int]:
        return data

    def consumer(producer: Iterator[tuple[str, int]]) -> list[tuple[str, int]]:
        return list(producer)

    p = build_minimal_dag(producer_fn=producer, consumer_fn=consumer, params=DictParam)
    assert build_dag(p).needs_materialize("producer") is False
