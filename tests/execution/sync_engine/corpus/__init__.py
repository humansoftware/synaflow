from .complex_parallel import pack as complex_parallel_pack
from .complex_parallel_mixed import pack as complex_parallel_mixed_pack
from .deep_sub_pipelines import pack as deep_sub_pipelines_pack
from .diamond import pack as diamond_pack
from .fibonacci import pack as fibonacci_pack
from .linear import pack as linear_pack
from .sub_pipelines import pack as sub_pipelines_pack

PACKS = {
    "sync_linear": linear_pack,
    "sync_diamond": diamond_pack,
    "sync_complex_parallel": complex_parallel_pack,
    "sync_fibonacci": fibonacci_pack,
    "sync_complex_parallel_mixed": complex_parallel_mixed_pack,
    "sync_sub_pipelines": sub_pipelines_pack,
    "sync_deep_sub_pipelines": deep_sub_pipelines_pack,
}
