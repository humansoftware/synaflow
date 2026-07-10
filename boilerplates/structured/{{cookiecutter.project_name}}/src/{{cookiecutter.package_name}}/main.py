from synaflow import PipelineRegistry, run

from {{ cookiecutter.package_name }}.pipeline import build_catalog
from {{ cookiecutter.package_name }}.steps import Params


def main():
    # Design-time: build_catalog() returns a PipelineRegistry (loaded once
    # at module import). catalog.get_dag(name) compiles the Dag on first
    # call and caches it for subsequent runs.
    catalog: PipelineRegistry = build_catalog()
    # Runtime: consume the prebuilt Dag, never recompile.
    run(catalog.get_dag("{{ cookiecutter.pipeline_name }}"), Params(count=5))


if __name__ == "__main__":
    main()
