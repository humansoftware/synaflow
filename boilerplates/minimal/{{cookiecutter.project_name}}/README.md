# {{ cookiecutter.project_name }}

{{ cookiecutter.description }}

## Quickstart

```bash
uv run python pipeline.py
```

## Structure

- `pipeline.py` — your entire app: params, steps, pipeline definition, and entry point
- `pyproject.toml` — project metadata and dependencies

## Customize

Edit `pipeline.py`:
1. Modify `Params` to match your input data
2. Replace `producer`, `transformer`, `consumer` with your own functions
3. Add or remove `step()` calls in the `pipeline()` definition

## Next concepts

If your pipeline is I/O-bound and one step starts work while the next step
waits for the result, read the `max_in_flight` docs:

- https://humansoftware.github.io/synaflow/core-concepts/max-in-flight/

For more complex projects, see the `structured` template:
```bash
uvx cookiecutter gh:humansoftware/synaflow --directory=boilerplates/structured
```
