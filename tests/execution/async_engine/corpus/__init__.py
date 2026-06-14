from . import (
    complex_parallel,
    complex_parallel_mixed,
    deep_sub_pipelines,
    diamond,
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
]

PACKS = {f"async_{mod.__name__.split('.')[-1]}": mod.pack for mod in _MODULES}
