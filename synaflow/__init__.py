"""
Pipeline Engine

A lightweight, robust engine for defining and executing typed Directed Acyclic Graphs (DAGs).
This module defines the public interface for clients.
"""

from .executor import run
from .pipeline import PipelineDef, pipeline
from .step import Step, step
from .types import OnError, StepParams

__all__ = [
    "PipelineDef",
    "pipeline",
    "Step",
    "step",
    "OnError",
    "StepParams",
    "run",
]
