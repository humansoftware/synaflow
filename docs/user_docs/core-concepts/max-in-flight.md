# Max In Flight

`max_in_flight` controls how far a producing stream may get ahead of its next
consumer stage.

```python
step("start_request", fn=start_request, max_in_flight=30)
```

The contract is:

> `max_in_flight` = maximum number of items already emitted by a step and not
> yet delivered to the next consumption stage.

## Why it exists

SynaFlow is lazy by default. That is ideal for memory efficiency, but some
pipelines need a small window between two streaming stages.

Typical example:

- one step starts work and returns a handle, `Future`, or task
- the next step resolves that handle
- you want up to `N` handles in flight, not exactly one

`max_in_flight` adds that bounded handoff without changing your business logic
into manual queue management.

This is especially useful for I/O-bound work:

- HTTP requests
- database queries
- RPC calls
- object-store reads and writes

Those workloads spend most of their time waiting. `max_in_flight` lets one step
keep a small pipeline of pending work in motion while the next step resolves
responses, without turning your DAG into manual queue or semaphore code.

## Default behavior

Every step has `max_in_flight=1` unless you set a different value.

That means:

- default behavior stays lockstep
- one item is handed off at a time
- memory stays tightly bounded

## Basic Example

=== "Sync"

    ```python
    from collections.abc import Generator, Iterator
    from concurrent.futures import Future, ThreadPoolExecutor
    from typing import NamedTuple

    from synaflow import pipeline, run, step

    pool = ThreadPoolExecutor(max_workers=30)


    class Params(NamedTuple):
        urls: list[str]


    def urls(urls: list[str]) -> Generator[str, None, None]:
        yield from urls


    def start_request(url: str) -> Future:
        return pool.submit(fetch, url)


    def await_response(start_request: Iterator[Future]) -> None:
        for future in start_request:
            print(future.result())


    def fetch(url: str) -> str:
        return f"ok:{url}"


    p = pipeline(
        name="bounded_requests",
        params=Params,
        steps=[
            step("urls", fn=urls),
            step("start_request", fn=start_request, max_in_flight=30),
            step("await_response", fn=await_response),
        ],
    )

    run(p, Params(urls=["a", "b", "c"]))
    pool.shutdown(wait=True)
    ```

=== "Async"

    ```python
    from collections.abc import AsyncGenerator, AsyncIterator
    from typing import NamedTuple

    from synaflow import async_run, pipeline, step


    class Params(NamedTuple):
        urls: list[str]


    async def urls(urls: list[str]) -> AsyncGenerator[str, None]:
        for url in urls:
            yield url


    async def start_request(url: str):
        return fetch_async(url)


    async def await_response(start_request: AsyncIterator):
        async for task in start_request:
            print(await task)


    async def fetch_async(url: str) -> str:
        return f"ok:{url}"


    p = pipeline(
        name="bounded_requests_async",
        params=Params,
        steps=[
            step("urls", fn=urls),
            step("start_request", fn=start_request, max_in_flight=30),
            step("await_response", fn=await_response),
        ],
    )

    await async_run(p, Params(urls=["a", "b", "c"]))
    ```

The application still owns the real concurrency. SynaFlow only bounds the handoff between `start_request` and `await_response`.

## Real HTTP Example

If you use an HTTP client (like `requests` for sync or `httpx` for async), the clean pattern is:
* One step submits/creates the async task or submits to a thread pool
* The next step consumes/awaits the task or future
* `max_in_flight` bounds the number of pending requests ahead, preserving memory and scheduling control

=== "Sync"

    ```python
    from collections.abc import Generator, Iterator
    from concurrent.futures import Future, ThreadPoolExecutor
    from typing import NamedTuple

    import requests

    from synaflow import pipeline, run, step

    pool = ThreadPoolExecutor(max_workers=30)


    class Params(NamedTuple):
        urls: list[str]


    def urls(urls: list[str]) -> Generator[str, None, None]:
        yield from urls


    def fetch(url: str) -> dict:
        response = requests.get(url, timeout=10)
        return {
            "url": url,
            "status": response.status_code,
            "size": len(response.text),
        }


    def start_request(urls: str) -> Future:
        return pool.submit(fetch, urls)


    def await_response(start_request: Iterator[Future]) -> None:
        for future in start_request:
            data = future.result()
            print(data["url"], data["status"], data["size"])


    p = pipeline(
        name="bounded_http_sync",
        params=Params,
        steps=[
            step("urls", fn=urls),
            step("start_request", fn=start_request, max_in_flight=5),
            step("await_response", fn=await_response),
        ],
    )

    run(
        p,
        Params(
            urls=[
                "https://example.com",
                "https://example.com",
                "https://example.com",
            ]
        ),
    )
    pool.shutdown(wait=True)
    ```

=== "Async"

    ```python
    import asyncio
    from collections.abc import AsyncGenerator, AsyncIterator
    from typing import NamedTuple

    import httpx

    from synaflow import async_run, pipeline, step


    class Params(NamedTuple):
        urls: list[str]


    async def main() -> None:
        async with httpx.AsyncClient() as client:
            async def urls(urls: list[str]) -> AsyncGenerator[str, None]:
                for url in urls:
                    yield url

            async def fetch(url: str) -> dict:
                response = await client.get(url, timeout=10)
                return {
                    "url": url,
                    "status": response.status_code,
                    "size": len(response.text),
                }

            async def start_request(url: str) -> asyncio.Task[dict]:
                return asyncio.create_task(fetch(url))

            async def await_response(
                start_request: AsyncIterator[asyncio.Task[dict]],
            ) -> None:
                async for task in start_request:
                    data = await task
                    print(data["url"], data["status"], data["size"])

            p = pipeline(
                name="bounded_http_async",
                params=Params,
                steps=[
                    step("urls", fn=urls),
                    step("start_request", fn=start_request, max_in_flight=30),
                    step("await_response", fn=await_response),
                ],
            )

            await async_run(
                p,
                Params(
                    urls=[
                        "https://example.com",
                        "https://example.com",
                        "https://example.com",
                    ]
                ),
            )


    asyncio.run(main())
    ```

Why this is good:
* **Bounded Advancement:** Without `max_in_flight`, the pipeline stays in strict lockstep (1 item at a time). With `max_in_flight=5` (or 30), the producing step may get ahead without running away.
* **Natural Consumption:** The consumer still reads naturally using standard iteration (`for` or `async for`). No manual semaphores or queue logic are needed in application code.
* **Controlled Concurrency:** Concurrency is still managed by your own thread pool or event loop, keeping the code shaped as a normal SynaFlow DAG.
## What it does and does not mean

`max_in_flight` means:

- bounded buffered handoff between producer and next consumer stage
- configured on the producing step
- compiled into the DAG and used by the runner from DAG metadata

It does **not** mean:

- thread count
- task count
- downstream completion tracking
- guaranteed number of unresolved network requests in every topology

Delivery is counted when the next stage receives the item, not when it finishes
processing it.

In the sync HTTP example above, that means the `start_request` step can submit
up to `N` pending `Future`s ahead of `await_response`. In the async example, it
means up to `N` tasks can be handed off ahead. That is why this feature is so
useful for I/O-bound pipelines: you get a controlled window of outstanding
operations while keeping the code shaped as a normal SynaFlow DAG.

## Fan-out

When one producer feeds multiple downstream consumers:

- the limit is enforced per consumer branch
- lazy branches stay lazy
- eager branches keep normal materialization behavior

This means one branch can stream while another branch materializes.

## When it has no effect

`max_in_flight` is accepted on every step, but it only matters on progressive
stream handoff.

Examples where it becomes a no-op:

- the step returns a scalar
- the step is terminal
- every downstream path materializes eagerly before streaming can matter
- the topology puts the next real consumer stage behind another lazy barrier,
  so bounded receive-based handoff cannot be enforced safely

The value still exists in the compiled DAG and exported JSON.

## Related concepts

- [Lockstep Data Flow](lockstep-flow.md)
- [Materialization & Error Policies](materialization.md)
- [Build vs Run](build-vs-run.md)
