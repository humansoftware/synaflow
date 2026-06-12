from .complex_parallel import pack as complex_parallel_pack
from .complex_parallel_mixed import pack as complex_parallel_mixed_pack
from .diamond import pack as diamond_pack
from .fibonacci import pack as fibonacci_pack
from .linear import pack as linear_pack
from .sub_pipelines import pack as sub_pipelines_pack

PACKS = {
    "async_linear": linear_pack,
    "async_diamond": diamond_pack,
    "async_complex_parallel": complex_parallel_pack,
    "async_fibonacci": fibonacci_pack,
    "async_complex_parallel_mixed": complex_parallel_mixed_pack,
    "async_sub_pipelines": sub_pipelines_pack,
}
