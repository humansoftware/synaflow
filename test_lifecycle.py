import asyncio
from synaflow.core.exceptions import PipelineStopException
from synaflow.execution.async_engine.step_lifecycle import AsyncStepLifecycle

class MockEvents:
    async def step_started(self, node, name): pass
    async def step_completed(self, node, name, **kwargs): pass
    async def step_failed(self, node, name, **kwargs):
        print("step_failed called with exception:", repr(kwargs["exception"]))

async def main():
    lifecycle = AsyncStepLifecycle(None, "test", MockEvents())
    exc = PipelineStopException("test", cause=ValueError("stop"))
    await lifecycle.finish(exception=exc)

asyncio.run(main())
