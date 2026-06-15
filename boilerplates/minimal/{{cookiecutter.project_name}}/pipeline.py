from collections.abc import Generator, Iterator
from typing import NamedTuple

from synaflow import pipeline, step, run


class Params(NamedTuple):
    count: int = 10


def producer(count: int) -> Generator[int, None, None]:
    """Produces a stream of integers."""
    yield from range(count)


def transformer(producer: int) -> int:
    """Transforms each item (EACH mode)."""
    return producer * 2


def consumer(transformer: Iterator[int]) -> None:
    """Consumes the transformed stream."""
    for x in transformer:
        print(x)


def main():
    p = pipeline(
        name="{{ cookiecutter.project_name }}",
        params=Params,
        steps=[
            step("producer", fn=producer),
            step("transformer", fn=transformer),
            step("consumer", fn=consumer),
        ],
    )

    run(p, Params(count=5))


if __name__ == "__main__":
    main()
