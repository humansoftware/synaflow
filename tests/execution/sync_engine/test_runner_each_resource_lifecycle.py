from collections.abc import Generator, Iterator
from contextlib import contextmanager
from typing import NamedTuple

from synaflow import PipelineRegistry, pipeline, run, step


class Params(NamedTuple):
    count: int = 3


def test_cm_resource_entered_and_exited_per_item_in_each_mode():
    events = []

    @contextmanager
    def tracking_cm(name: str) -> Iterator[str]:
        events.append(f"{name}: enter")
        try:
            yield name
        finally:
            events.append(f"{name}: exit")

    def cm_factory() -> str:
        return tracking_cm("write_db")  # type: ignore

    def numbers(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def each_step(numbers: int, cm: str) -> str:
        events.append(f"each_step: processing {numbers} with cm={cm}")
        return f"processed-{numbers}"

    p = pipeline(
        name="test_each_resource",
        params=Params,
        resources={"cm": cm_factory},
        steps=[
            step("numbers", fn=numbers),
            step("each_step", fn=each_step),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    run(catalog.get_dag("test_each_resource"), Params(count=3))

    expected = [
        "write_db: enter",
        "each_step: processing 0 with cm=write_db",
        "write_db: exit",
        "write_db: enter",
        "each_step: processing 1 with cm=write_db",
        "write_db: exit",
        "write_db: enter",
        "each_step: processing 2 with cm=write_db",
        "write_db: exit",
    ]
    assert events == expected


def test_plain_resource_resolved_once_in_each_mode():
    factory_calls = 0

    def plain_factory() -> str:
        nonlocal factory_calls
        factory_calls += 1
        return "plain_res"

    def numbers(count: int) -> Generator[int, None, None]:
        yield from range(count)

    def each_step(numbers: int, res: str) -> str:
        return f"{res}-{numbers}"

    p = pipeline(
        name="test_each_plain_resource",
        params=Params,
        resources={"res": plain_factory},
        steps=[
            step("numbers", fn=numbers),
            step("each_step", fn=each_step),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    run(catalog.get_dag("test_each_plain_resource"), Params(count=3))

    assert factory_calls == 1


def test_cm_resource_in_all_mode_entered_once():
    events = []

    @contextmanager
    def tracking_cm(name: str) -> Iterator[str]:
        events.append(f"{name}: enter")
        try:
            yield name
        finally:
            events.append(f"{name}: exit")

    def cm_factory() -> str:
        return tracking_cm("db")  # type: ignore

    def all_step(cm: str, count: int) -> str:
        events.append(f"all_step: cm={cm}")
        return f"done-{count}"

    p = pipeline(
        name="test_all_resource",
        params=Params,
        resources={"cm": cm_factory},
        steps=[
            step("all_step", fn=all_step),
        ],
    )

    catalog = PipelineRegistry()
    catalog.add(p)
    run(catalog.get_dag("test_all_resource"), Params(count=3))

    expected = [
        "db: enter",
        "all_step: cm=db",
        "db: exit",
    ]
    assert events == expected
