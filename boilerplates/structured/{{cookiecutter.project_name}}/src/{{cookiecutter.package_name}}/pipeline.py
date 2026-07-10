from synaflow import PipelineRegistry, pipeline, step

from {{ cookiecutter.package_name }}.steps import Params, consumer, producer, transformer


def build_catalog() -> PipelineRegistry:
    """Design-time: build the catalog at module load.

    ``catalog.get_dag(name)`` compiles the Dag on first call and
    caches it for subsequent runs.
    """
    catalog = PipelineRegistry()
    catalog["{{ cookiecutter.pipeline_name }}"] = pipeline(
        name="{{ cookiecutter.pipeline_name }}",
        params=Params,
        steps=[
            step("producer", fn=producer),
            step("transformer", fn=transformer),
            step("consumer", fn=consumer),
        ],
    )
    return catalog
