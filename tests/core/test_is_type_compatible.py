from collections.abc import Generator, Iterator
from dataclasses import dataclass
from concurrent.futures import Future
from typing import Any
import pytest
from synaflow.core.type_compatibility import is_type_compatible


@dataclass
class MyDataclass:
    x: int


@dataclass
class OtherDataclass:
    x: int


@pytest.mark.parametrize(
    "producer, consumer, expected",
    [
        (list[dict], list[dict], True),
        (Iterator[dict], Iterator[dict], True),
        (Generator[dict, None, None], Generator[dict, None, None], True),
        (list[dict], Iterator[dict], True),
        (dict, dict, True),
        (list, list, True),
        (list[int], list[int], True),
        (dict[str, int], dict[str, int], True),
        # Bare container compatibility with parameterized ones
        (list, list[int], True),
        (list[int], list, True),
        (dict, dict[str, int], True),
        (dict[str, int], dict, True),
        # Mismatched types should still fail
        (list[int], list[str], False),
        (list[dict], list[int], True),
        # Custom/specific type compatibility tests
        (MyDataclass, MyDataclass, True),
        (MyDataclass, OtherDataclass, False),
        (list[MyDataclass], list[MyDataclass], True),
        (list[MyDataclass], list[OtherDataclass], False),
        (tuple[int, str], tuple[int, str], True),
        (tuple[int, str], tuple[str, int], False),
        (Future, Future, True),
        (list[Future], list[Future], True),
    ],
)
def test_given_bare_containers_when_checking_compatibility_then_returns_expected(
    producer: Any, consumer: Any, expected: bool
):
    assert is_type_compatible(producer, consumer) is expected


def test_given_list_type_with_none_when_checking_compatibility_then_returns_false():
    from synaflow.core.type_compatibility import ListType

    assert is_type_compatible(ListType(None), list) is False
