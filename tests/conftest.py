import asyncio

import pytest

from synaflow.core.dag_builder import build_dag
from synaflow.execution.sync_engine.executor import run as sync_run
from synaflow.execution.async_engine.executor import async_run


@pytest.fixture(params=["sync"])
def run_pipeline(request):
    if request.param == "sync":

        def wrapper(pipeline, params, **kwargs):
            return sync_run(build_dag(pipeline), params, **kwargs)

        return wrapper
    elif request.param == "async":

        def wrapper(pipeline, params, **kwargs):
            return asyncio.run(async_run(build_dag(pipeline), params, **kwargs))

        return wrapper
