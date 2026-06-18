# Installation

SynaFlow requires Python 3.10 or later.

## Quickstart with Cookiecutter

Generate a complete project from a template — the fastest way to start:

```bash
# Single-file app (minimal)
uvx cookiecutter gh:humansoftware/synaflow --directory=boilerplates/minimal

# Structured project (steps, pipeline, main separated)
uvx cookiecutter gh:humansoftware/synaflow --directory=boilerplates/structured
```

Each template creates a ready-to-run project with `pyproject.toml`, example
steps, and a working pipeline. Answer a few prompts and you're done.

If you expect I/O-bound stages such as HTTP requests or RPC calls, the next
concept to read after generating a template is
[Max In Flight](../core-concepts/max-in-flight.md).

If you already have a project and just want to scaffold a new pipeline module:

```bash
uvx cookiecutter gh:humansoftware/synaflow --directory=boilerplates/scaffold
```

## Manual install

=== "pip"

    ```bash
    pip install synaflow
    ```

=== "uv"

    ```bash
    uv pip install synaflow
    ```

## Verify

```python
import synaflow
print(synaflow.__version__)
```

## Optional extras

For development and running the test suite:

=== "pip"

    ```bash
    pip install synaflow pytest pytest-asyncio pytest-cov
    ```

=== "uv"

    ```bash
    uv pip install synaflow pytest pytest-asyncio pytest-cov
    ```

## Next

Read the [Introduction](introduction.md) for a tour of the documentation, or jump straight into the [Tutorial](../tutorial/hello-world.md).
