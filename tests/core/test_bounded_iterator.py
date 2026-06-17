import pytest

from synaflow.execution.bounded_iterator import BoundedIterator


def test_given_empty_source_when_iterated_then_returns_nothing():
    it = BoundedIterator(iter([]), maxsize=5)
    assert list(it) == []


def test_given_source_smaller_than_maxsize_when_iterated_then_returns_all():
    it = BoundedIterator(iter([1, 2, 3]), maxsize=10)
    assert list(it) == [1, 2, 3]


def test_given_source_larger_than_maxsize_when_iterated_then_returns_all():
    it = BoundedIterator(iter(range(100)), maxsize=5)
    assert list(it) == list(range(100))


def test_given_maxsize_1_when_iterated_then_lockstep():
    it = BoundedIterator(iter(range(5)), maxsize=1)
    assert list(it) == [0, 1, 2, 3, 4]


def test_given_maxsize_lt_1_when_constructed_then_raises():
    with pytest.raises(ValueError, match="maxsize must be >= 1"):
        BoundedIterator(iter([]), maxsize=0)


def test_given_source_raises_when_iterated_then_exception_propagates():
    def bad_source():
        yield 1
        yield 2
        raise ValueError("explode")

    it = BoundedIterator(bad_source(), maxsize=5)
    results = []
    with pytest.raises(ValueError, match="explode"):
        for x in it:
            results.append(x)
    assert results == [1, 2]


def test_given_source_raises_stopiteration_when_iterated_then_stops():
    def source():
        yield 1
        yield 2
        # implicit StopIteration

    it = BoundedIterator(source(), maxsize=5)
    assert list(it) == [1, 2]


def test_given_zero_item_source_when_iterated_then_empty():
    it = BoundedIterator(iter([]), maxsize=1)
    assert list(it) == []


def test_given_large_maxsize_when_iterated_then_correct_order():
    it = BoundedIterator(iter([10, 20, 30]), maxsize=1000)
    result = []
    for x in it:
        result.append(x)
    assert result == [10, 20, 30]


def test_given_maxsize_2_when_partial_iteration_then_partial_consumption():
    src = iter([1, 2, 3, 4, 5])
    it = BoundedIterator(src, maxsize=2)
    assert next(it) == 1
    assert next(it) == 2
    # remaining items still in source
    assert list(it) == [3, 4, 5]
