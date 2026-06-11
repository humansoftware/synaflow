from .diamond import diamond_pipeline
from .linear import linear_pipeline

EXAMPLES = {
    "linear": linear_pipeline,
    "diamond": diamond_pipeline,
}

__all__ = ["EXAMPLES", "linear_pipeline", "diamond_pipeline"]
