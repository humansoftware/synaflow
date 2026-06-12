from .linear import linear_pipeline

EXAMPLES = {"sync_linear": linear_pipeline}
from .diamond import diamond_pipeline

EXAMPLES["sync_diamond"] = diamond_pipeline
from .complex_parallel import pipeline_def as complex_parallel_pipeline

EXAMPLES["sync_complex_parallel"] = complex_parallel_pipeline
from .fibonacci import pipeline_def as fibonacci_pipeline

EXAMPLES["sync_fibonacci"] = fibonacci_pipeline
from .complex_parallel_mixed import pipeline_def as complex_parallel_mixed_pipeline

EXAMPLES["sync_complex_parallel_mixed"] = complex_parallel_mixed_pipeline
