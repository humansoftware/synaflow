from synaflow import run

from {{ cookiecutter.package_name }}.pipeline import build_pipeline
from {{ cookiecutter.package_name }}.steps import Params


def main():
    p = build_pipeline()
    run(p, Params(count=5))


if __name__ == "__main__":
    main()
