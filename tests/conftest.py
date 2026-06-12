import asyncio

import pytest

from synaflow.execution.sync_engine.pipeline import run as sync_run


@pytest.fixture(params=["sync"])
def run_pipeline(request):
    if request.param == "sync":

        def wrapper(pipeline, params, **kwargs):
            return sync_run(pipeline, params, **kwargs)

        return wrapper
    elif request.param == "async":

        def wrapper(pipeline, params, **kwargs):
            from synaflow.execution.async_engine.pipeline import async_run

            return asyncio.run(async_run(pipeline, params, **kwargs))

        return wrapper
