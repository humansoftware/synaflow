# Tutorial — Level 1: Hello World

Let's build the simplest possible SynaFlow pipeline: one step that receives a parameter and prints it.

=== "Sync"

    ```python
    from typing import NamedTuple
    from synaflow import pipeline, step, run

    class Params(NamedTuple):
        message: str

    def hello(message: str) -> None:
        print(f"Hello, {message}!")

    p = pipeline(
        name="hello_world",
        params=Params,
        steps=[step("hello", fn=hello)],
    )

    run(p, Params(message="SynaFlow"))
    ```

=== "Async"

    ```python
    from typing import NamedTuple
    from synaflow import pipeline, step, async_run

    class Params(NamedTuple):
        message: str

    async def hello(message: str) -> None:
        print(f"Hello, {message}!")

    p = pipeline(
        name="hello_world",
        params=Params,
        steps=[step("hello", fn=hello)],
    )

    async_run(p, Params(message="SynaFlow"))
    ```

**What happened?** SynaFlow read the type hint of `message: str` in `hello()`, found it in `Params`, and passed the value automatically. No manual wiring.

## The DAG JSON

Every pipeline can be exported to JSON:

```python
print(p.to_dict())
```

```json
{
  "name": "hello_world",
  "params": {"message": "str"},
  "steps": {
    "hello": {
      "deps": {"message": "str"},
      "output": "None",
      "fn": "hello",
      "on_error": "continue",
      "mode": "all"
    }
  }
}
```

## Next

Add more steps and let SynaFlow wire them automatically in [Level 2](multiple-steps.md).
