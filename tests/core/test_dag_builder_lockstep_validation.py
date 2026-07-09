from synaflow.core.dag_builder import build_dag
import pytest
from typing import NamedTuple, Iterator
from synaflow import pipeline, step
from synaflow.core.types import StepMode


class P(NamedTuple):
    pass


def test_original_case_1_all_stream_validates_successfully():

    def p1() -> Iterator[int]:
        yield 1

    def s2(p1: int) -> int:
        return p1

    def s2b(p1: int) -> int:
        return p1

    def s3(s2: int) -> int:
        return s2

    def s3b(s2b: int) -> int:
        return s2b

    def s4(s3: int, s3b: int) -> int:
        return s3 + s3b

    my_pipe = pipeline(
        name="test",
        params=P,
        steps=[
            step("p1", p1, mode=StepMode.ALL),
            step("s2", s2, mode=StepMode.EACH),
            step("s2b", s2b, mode=StepMode.EACH),
            step("s3", s3, mode=StepMode.EACH),
            step("s3b", s3b, mode=StepMode.EACH),
            step("s4", s4, mode=StepMode.EACH),
        ],
    )
    assert build_dag(my_pipe) is not None


def test_original_case_2_only_s4_materializes_validates_successfully():

    def p1() -> Iterator[int]:
        yield 1

    def s2(p1: int) -> int:
        return p1

    def s2b(p1: int) -> int:
        return p1

    def s3(s2: int) -> int:
        return s2

    def s3b(s2b: int) -> int:
        return s2b

    def s4(s3: Iterator[int], s3b: Iterator[int]) -> list[int]:
        return list(s3)

    my_pipe = pipeline(
        name="test",
        params=P,
        steps=[
            step("p1", p1, mode=StepMode.ALL),
            step("s2", s2, mode=StepMode.EACH),
            step("s2b", s2b, mode=StepMode.EACH),
            step("s3", s3, mode=StepMode.EACH),
            step("s3b", s3b, mode=StepMode.EACH),
            step("s4", s4, mode=StepMode.ALL),
        ],
    )
    assert build_dag(my_pipe) is not None


def test_original_case_4_only_s2_materializes_throws_design_time_error():

    def p1() -> Iterator[int]:
        yield 1

    def s2(p1: Iterator[int]) -> list[int]:
        return list(p1)

    def s2b(p1: int) -> int:
        return p1

    def s3(s2: list[int]) -> Iterator[int]:
        yield len(s2)

    def s3b(s2b: int) -> int:
        return s2b

    def s4(s3: int, s3b: int) -> int:
        return s3 + s3b

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("p1", p1, mode=StepMode.ALL),
            step("s2", s2, mode=StepMode.ALL),
            step("s2b", s2b, mode=StepMode.EACH),
            step("s3", s3, mode=StepMode.ALL),
            step("s3b", s3b, mode=StepMode.EACH),
            step("s4", s4, mode=StepMode.EACH),
        ],
    )
    with pytest.raises(ValueError, match="Asymmetric lockstep materialization"):
        build_dag(p)


def test_original_case_5_s2_and_s3b_materialize_validates_successfully():

    def p1() -> Iterator[int]:
        yield 1

    def s2(p1: Iterator[int]) -> list[int]:
        return list(p1)

    def s2b(p1: int) -> int:
        return p1

    def s3(s2: list[int]) -> Iterator[int]:
        yield len(s2)

    def s3b(s2b: Iterator[int]) -> list[int]:
        return list(s2b)

    def s4(s3: int, s3b: list[int]) -> int:
        return s3 + len(s3b)

    my_pipe = pipeline(
        name="test",
        params=P,
        steps=[
            step("p1", p1, mode=StepMode.ALL),
            step("s2", s2, mode=StepMode.ALL),
            step("s2b", s2b, mode=StepMode.EACH),
            step("s3", s3, mode=StepMode.ALL),
            step("s3b", s3b, mode=StepMode.ALL),
            step("s4", s4, mode=StepMode.EACH),
        ],
    )
    assert build_dag(my_pipe) is not None


def test_nested_diamonds_with_bypass_throws_design_time_error():

    def s1() -> Iterator[int]:
        yield 1

    def s2(s1: int) -> int:
        return s1

    def s2b(s1: int) -> int:
        return s1

    def s3(s2: int) -> int:
        return s2

    def s3c(s2b: int) -> int:
        return s2b

    def s3c1(s3c: Iterator[int]) -> list[int]:
        return list(s3c)

    def s3c2(s3c: int) -> int:
        return s3c

    def s3c4(s3c1: list[int], s3c2: int) -> int:
        return len(s3c1) + s3c2

    def s4(s3: int, s3c: int, s3c4: Iterator[int]) -> int:
        return s3 + s3c

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", s1, mode=StepMode.ALL),
            step("s2", s2, mode=StepMode.EACH),
            step("s2b", s2b, mode=StepMode.EACH),
            step("s3", s3, mode=StepMode.EACH),
            step("s3c", s3c, mode=StepMode.EACH),
            step("s3c1", s3c1, mode=StepMode.ALL),
            step("s3c2", s3c2, mode=StepMode.EACH),
            step("s3c4", s3c4, mode=StepMode.EACH),
            step("s4", s4, mode=StepMode.EACH),
        ],
    )
    with pytest.raises(ValueError, match="Asymmetric lockstep materialization"):
        build_dag(p)


def test_ultra_complex_diamond_staggered_joins_throws_design_time_error():

    def s1() -> Iterator[int]:
        yield 1

    def s2(s1: int) -> int:
        return s1

    def s3(s1: int) -> int:
        return s1

    def s4(s1: int) -> int:
        return s1

    def s5(s2: Iterator[int]) -> list[int]:
        return list(s2)

    def s6(s3: Iterator[int]) -> list[int]:
        return list(s3)

    def s7(s4: int) -> int:
        return s4

    def s8(s5: list[int], s6: list[int]) -> Iterator[int]:
        yield (len(s5) + len(s6))

    def s9(s8: int, s7: int) -> int:
        return s8 + s7

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("s1", s1, mode=StepMode.ALL),
            step("s2", s2, mode=StepMode.EACH),
            step("s3", s3, mode=StepMode.EACH),
            step("s4", s4, mode=StepMode.EACH),
            step("s5", s5, mode=StepMode.ALL),
            step("s6", s6, mode=StepMode.ALL),
            step("s7", s7, mode=StepMode.EACH),
            step("s8", s8, mode=StepMode.ALL),
            step("s9", s9, mode=StepMode.EACH),
        ],
    )
    with pytest.raises(ValueError, match="Asymmetric lockstep materialization"):
        build_dag(p)


def test_given_cross_level_stream_bypass_throws_design_time_error():
    from typing import Generator

    def producer() -> Generator[int, None, None]:
        for i in range(10):
            yield i

    def first_consumer(producer: Iterator[int]) -> int:
        total = 0
        for item in producer:
            total += item
        return total

    def second_consumer(first_consumer: int, producer: Iterator[int]) -> None:
        pass

    p = pipeline(
        name="test",
        params=P,
        steps=[
            step("producer", fn=producer, mode=StepMode.ALL),
            step("first_consumer", fn=first_consumer, mode=StepMode.ALL),
            step("second_consumer", fn=second_consumer, mode=StepMode.ALL),
        ],
    )
    with pytest.raises(ValueError, match="Asymmetric lockstep materialization"):
        build_dag(p)
