from typing import AsyncGenerator, Generator, NamedTuple

import pytest

from synaflow import pipeline, step


class P(NamedTuple):
    count: int = 3


def test_sync_pipeline_rejects_async_materializer():
    async def async_mat(g):
        pass

    def factory(ctx):
        return async_mat

    def gen() -> Generator[int, None, None]:
        yield 1

    with pytest.raises(ValueError, match="UNRUNNABLE"):
        pipeline(
            name="test",
            params=P,
            default_materializer_factory=factory,
            steps=[step("items", fn=gen)],
        )


def test_async_pipeline_rejects_sync_materializer():
    def sync_mat(g):
        pass

    def factory(ctx):
        return sync_mat

    async def async_gen() -> AsyncGenerator[int, None]:
        yield 1

    with pytest.raises(ValueError, match="UNRUNNABLE"):
        pipeline(
            name="test",
            params=P,
            default_materializer_factory=factory,
            steps=[step("items", fn=async_gen)],
        )


def test_step_materializer_rejects_incompatible():
    def sync_mat(g):
        pass

    async def async_gen() -> AsyncGenerator[int, None]:
        yield 1

    with pytest.raises(ValueError, match="UNRUNNABLE"):
        pipeline(
            name="test",
            params=P,
            steps=[step("items", fn=async_gen, materializer=sync_mat)],
        )
