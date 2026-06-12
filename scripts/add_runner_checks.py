import os

with open("tests/test_sync/test_runner_basic.py", "a") as f:
    f.write("""

def test_given_async_pipeline_when_run_synchronously_then_raises():
    import pytest
    from synaflow.pipeline import pipeline
    from synaflow.step import step
    from synaflow.executor import run
    from typing import NamedTuple

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    async def s1(items: list[int]) -> int:
        return 1
        
    my_pipeline = pipeline(name="t", params=P, steps=[step("s1", fn=s1)])
    
    with pytest.raises(RuntimeError, match="must be executed with async_run"):
        run(my_pipeline, params=P())
""")

with open("tests/test_async/test_async_runner_basic.py", "a") as f:
    f.write("""

@pytest.mark.asyncio
async def test_given_sync_stream_pipeline_when_run_asynchronously_then_raises():
    import pytest
    from synaflow.pipeline import pipeline
    from synaflow.step import step
    from synaflow.async_executor import async_run
    from typing import NamedTuple, Iterator

    class P(NamedTuple):
        items: list[int] = [1, 2, 3]

    def s1(items: list[int]) -> Iterator[int]:
        for i in items: yield i
        
    my_pipeline = pipeline(name="t", params=P, steps=[step("s1", fn=s1)])
    
    with pytest.raises(RuntimeError, match="must be executed with run"):
        await async_run(my_pipeline, params=P())
""")
