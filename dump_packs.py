import json

from synaflow.execution.sync_engine.pipeline import PipelineExecutor
from tests.execution.sync_engine.corpus.complex_parallel import ComplexParallelParams
from tests.execution.sync_engine.corpus.complex_parallel import (
    pipeline_def as complex_parallel_pipeline,
)
from tests.execution.sync_engine.corpus.complex_parallel_mixed import (
    ComplexParallelMixedParams,
)
from tests.execution.sync_engine.corpus.complex_parallel_mixed import (
    pipeline_def as complex_parallel_mixed_pipeline,
)
from tests.execution.sync_engine.corpus.diamond import DiamondParams, diamond_pipeline
from tests.execution.sync_engine.corpus.fibonacci import FibonacciParams
from tests.execution.sync_engine.corpus.fibonacci import (
    pipeline_def as fibonacci_pipeline,
)
from tests.execution.sync_engine.corpus.linear import LinearParams, linear_pipeline
from tests.execution.sync_engine.corpus.sub_pipelines import AParams
from tests.execution.sync_engine.corpus.sub_pipelines import (
    pipe as sub_pipelines_pipeline,
)


def dump(pipeline, params):
    print(f"--- {pipeline.name} ---")
    executor = PipelineExecutor(pipeline)
    executor.execute(params)
    print("Results:", executor.context)
    print("Call Order:", executor.call_order)


if __name__ == "__main__":
    dump(linear_pipeline, LinearParams(count=3))
    dump(complex_parallel_pipeline, ComplexParallelParams(base=1))
    dump(diamond_pipeline, DiamondParams())
    dump(sub_pipelines_pipeline, AParams(raw_texts=["hi", "world"]))
    dump(fibonacci_pipeline, FibonacciParams(n=5))
    dump(complex_parallel_mixed_pipeline, ComplexParallelMixedParams(base=1))
