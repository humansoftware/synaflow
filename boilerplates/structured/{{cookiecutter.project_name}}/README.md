# {{ cookiecutter.project_name }}

{{ cookiecutter.description }}

## Quickstart

```bash
uv run python src/{{ cookiecutter.package_name }}/main.py
```

## Structure

```
src/{{ cookiecutter.package_name }}/
├── __init__.py
├── steps.py       # all step functions + Params
├── pipeline.py    # pipeline definition
└── main.py        # entry point
```

## Customize

1. Add your functions to `steps.py`
2. Wire them in `pipeline.py`
3. Run from `main.py`

## Next concepts

If your pipeline needs a bounded ahead window between two streaming stages,
especially for I/O-bound work, read the `max_in_flight` docs:

- https://humansoftware.github.io/synaflow/core-concepts/max-in-flight/

Need a simpler single-file project? Use the `minimal` template:
```bash
uvx cookiecutter gh:humansoftware/synaflow --directory=boilerplates/minimal
```
