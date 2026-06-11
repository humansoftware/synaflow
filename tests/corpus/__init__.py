from .diamond import diamond_pipeline
from .linear import linear_pipeline
from .complex_parallel import pipeline_def as complex_parallel_pipeline
from .complex_parallel_mixed import pipeline_def as complex_parallel_mixed_pipeline

EXAMPLES = {
    "linear": linear_pipeline,
    "diamond": diamond_pipeline,
    "complex_parallel": complex_parallel_pipeline,
    "complex_parallel_mixed": complex_parallel_mixed_pipeline,
}

__all__ = ["EXAMPLES", "linear_pipeline", "diamond_pipeline", "complex_parallel_pipeline", "complex_parallel_mixed_pipeline"]
