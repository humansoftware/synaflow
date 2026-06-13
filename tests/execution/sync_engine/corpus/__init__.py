from . import (
    complex_parallel,
    complex_parallel_mixed,
    deep_sub_pipelines,
    diamond,
    fibonacci,
    linear,
    sub_pipelines,
)

_MODULES = [
    linear,
    diamond,
    complex_parallel,
    fibonacci,
    complex_parallel_mixed,
    sub_pipelines,
    deep_sub_pipelines,
]

PACKS = {f"sync_{mod.__name__.split('.')[-1]}": mod.pack for mod in _MODULES}
