# Installation

SynaFlow requires Python 3.10 or later.

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
