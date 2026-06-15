from synaflow import pipeline, step

from {{ cookiecutter.package_name }}.steps import Params, consumer, producer, transformer


def build_pipeline():
    return pipeline(
        name="{{ cookiecutter.project_name }}",
        params=Params,
        steps=[
            step("producer", fn=producer),
            step("transformer", fn=transformer),
            step("consumer", fn=consumer),
        ],
    )
