from . import (
    complex_parallel,
    complex_parallel_mixed,
    custom_types,
    deep_sub_pipelines,
    diamond,
    error_handling,
    explicit_modes,
    fibonacci,
    linear,
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
    sub_pipelines,
    deep_sub_pipelines,
    error_handling,
    custom_types,
]

PACKS = {f"async_{mod.__name__.split('.')[-1]}": mod.pack for mod in _MODULES}
