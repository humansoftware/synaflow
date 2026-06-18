from . import (
    complex_parallel,
    complex_parallel_mixed,
    deep_sub_pipelines,
    diamond,
    error_handling,
    explicit_modes,
    fibonacci,
    linear,
    max_in_flight_threadpool,
    mixed_fanout,
    sub_pipelines,
)

_MODULES = [
    linear,
    diamond,
    complex_parallel,
    fibonacci,
    complex_parallel_mixed,
    explicit_modes,
    mixed_fanout,
    max_in_flight_threadpool,
    sub_pipelines,
    deep_sub_pipelines,
    error_handling,
]

PACKS = {f"sync_{mod.__name__.split('.')[-1]}": mod.pack for mod in _MODULES}
