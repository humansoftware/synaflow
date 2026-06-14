from typing import NamedTuple

from synaflow import pipeline, step
from synaflow.core.types import OnError


class EmptyParams(NamedTuple):
    pass


class IntParam(NamedTuple):
    x: int = 1


class KVParam(NamedTuple):
    pass


def build_minimal_dag(
    producer_fn,
    consumer_fn,
    params=None,
    producer_on_error=OnError.CONTINUE,
    producer_materializer=None,
    pipeline_materializer=None,
    consumer_name="consumer",
):
    if params is None:
        params = EmptyParams

    kwargs = dict(
        name="test_minimal",
        params=params,
        steps=[
            step(
                name="producer",
                fn=producer_fn,
                on_error=producer_on_error,
                materializer=producer_materializer,
            ),
            step(name=consumer_name, fn=consumer_fn),
        ],
    )
    if pipeline_materializer is not None:
        kwargs["materializer"] = pipeline_materializer

    return pipeline(**kwargs)
