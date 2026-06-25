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

## How Bounded Handoff Works

To understand how `max_in_flight` bounds the producer progress, let's look at an interactive simulation of the basic example with `max_in_flight=3` and `count=5`:

<div id="mif-animation" style="font-family:monospace;margin:1.5em 0;padding:1em;background:#1a1b26;color:#a9b1d6;border-radius:8px;overflow:hidden">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5em">
  <div>
    <span style="font-size:0.85em;color:#565f89">Step:</span>
    <span id="mif-anim-step" style="color:#7dcfff">0/11</span>
  </div>
  <div>
    <button onclick="mifAnimFrame(-1)" style="background:#565f89;color:#fff;border:none;padding:4px 12px;border-radius:4px;cursor:pointer;font-size:0.9em;margin-right:6px" title="Previous frame">◀</button>
    <button id="mif-anim-play" onclick="mifAnimToggle()" style="background:#7dcfff;color:#1a1b26;border:none;padding:4px 12px;border-radius:4px;cursor:pointer;font-size:0.9em;margin-right:6px">▶ Play</button>
    <button onclick="mifAnimFrame(1)" style="background:#565f89;color:#fff;border:none;padding:4px 12px;border-radius:4px;cursor:pointer;font-size:0.9em;margin-right:6px" title="Next frame">▶</button>
    <button onclick="mifAnimReset()" style="background:#565f89;color:#fff;border:none;padding:4px 12px;border-radius:4px;cursor:pointer;font-size:0.9em">↺ Reset</button>
  </div>
</div>

<div style="text-align:center;margin-bottom:0.5em">
<svg id="mif-anim-dag" viewBox="0 0 500 120" style="max-width:500px;width:100%;height:auto;margin:0 auto">
  <defs>
    <marker id="mif-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">
      <path d="M0,0 L10,5 L0,10" fill="#565f89"/>
    </marker>
  </defs>
  <line x1="120" y1="60" x2="160" y2="60" stroke="#565f89" stroke-width="2" marker-end="url(#mif-arrow)"/>
  <line x1="330" y1="60" x2="370" y2="60" stroke="#565f89" stroke-width="2" marker-end="url(#mif-arrow)"/>
  <rect x="10" y="30" width="110" height="60" rx="6" fill="#7aa2f7" opacity="0.15" stroke="#7aa2f7" stroke-width="1.5"/>
  <text x="65" y="48" text-anchor="middle" fill="#7aa2f7" font-size="11" font-weight="bold">start_request</text>
  <text id="svg-prod-state" x="65" y="72" text-anchor="middle" fill="#a9b1d6" font-size="10" font-family="monospace">Idle</text>
  <rect x="170" y="20" width="160" height="80" rx="6" fill="none" stroke="#565f89" stroke-width="1.5" stroke-dasharray="3 3"/>
  <text x="250" y="38" text-anchor="middle" fill="#bb9af7" font-size="11" font-weight="bold">Buffer (max=3)</text>
  <rect id="slot-0" x="185" y="50" width="40" height="36" rx="4" fill="#3b4261" stroke="#565f89" stroke-width="1"/>
  <text id="slot-0-text" x="205" y="72" text-anchor="middle" fill="#a9b1d6" font-size="9" font-family="monospace">-</text>
  <rect id="slot-1" x="230" y="50" width="40" height="36" rx="4" fill="#3b4261" stroke="#565f89" stroke-width="1"/>
  <text id="slot-1-text" x="250" y="72" text-anchor="middle" fill="#a9b1d6" font-size="9" font-family="monospace">-</text>
  <rect id="slot-2" x="275" y="50" width="40" height="36" rx="4" fill="#3b4261" stroke="#565f89" stroke-width="1"/>
  <text id="slot-2-text" x="295" y="72" text-anchor="middle" fill="#a9b1d6" font-size="9" font-family="monospace">-</text>
  <rect x="380" y="30" width="110" height="60" rx="6" fill="#7aa2f7" opacity="0.15" stroke="#7aa2f7" stroke-width="1.5"/>
  <text x="435" y="48" text-anchor="middle" fill="#7aa2f7" font-size="11" font-weight="bold">await_response</text>
  <text id="svg-cons-state" x="435" y="72" text-anchor="middle" fill="#a9b1d6" font-size="10" font-family="monospace">Idle</text>
</svg>
</div>

<table style="width:100%;border-collapse:collapse;font-size:0.85em;text-align:center">
<thead>
  <tr style="color:#7aa2f7">
    <th style="padding:6px;border-bottom:1px solid #565f89;width:65px"></th>
    <th style="padding:6px;border-bottom:1px solid #565f89">start_request</th>
    <th style="padding:6px;border-bottom:1px solid #565f89">Buffer Items</th>
    <th style="padding:6px;border-bottom:1px solid #565f89">await_response</th>
  </tr>
</thead>
<tbody id="mif-anim-body">
</tbody>
</table>
<div id="mif-anim-insight" style="margin-top:0.8em;padding:8px 12px;background:#24283b;border-radius:4px;font-size:0.85em;min-height:2.8em;color:#9ece6a"></div>
</div>

<script>
(function() {
  var frames = [
    [0, "Idle", ["-", "-", "-"], "Idle", "Pipeline starts. Both steps are idle. Buffer is empty."],
    [1, "Submit url0", ["Fut0", "-", "-"], "Idle", "start_request submits url0 (returns Fut0) and places it in the buffer. Buffer size: 1/3."],
    [2, "Submit url1", ["Fut0", "Fut1", "-"], "Idle", "start_request submits url1 (returns Fut1). Buffer size: 2/3."],
    [3, "Submit url2", ["Fut0", "Fut1", "Fut2"], "Idle", "start_request submits url2 (returns Fut2). Buffer is now full (3/3) and start_request blocks!"],
    [4, "Blocked", ["Fut1", "Fut2", "-"], "Awaiting Fut0", "await_response pulls and resolves Fut0. Buffer drops to 2/3, unblocking producer."],
    [5, "Submit url3", ["Fut1", "Fut2", "Fut3"], "Idle", "start_request immediately submits url3 (returns Fut3). Buffer becomes full again (3/3) and producer blocks."],
    [6, "Blocked", ["Fut2", "Fut3", "-"], "Awaiting Fut1", "await_response resolves Fut1. Buffer drops to 2/3, unblocking producer."],
    [7, "Submit url4", ["Fut2", "Fut3", "Fut4"], "Idle", "start_request submits url4 (the final URL). Producer is finished. Buffer size: 3/3."],
    [8, "Finished", ["Fut3", "Fut4", "-"], "Awaiting Fut2", "await_response resolves Fut2. Buffer size: 2/3."],
    [9, "Finished", ["Fut4", "-", "-"], "Awaiting Fut3", "await_response resolves Fut3. Buffer size: 1/3."],
    [10, "Finished", ["-", "-", "-"], "Awaiting Fut4", "await_response pulls the final Fut4 from the buffer. Buffer is empty."],
    [11, "Finished", ["-", "-", "-"], "Finished", "await_response resolves Fut4. All futures resolved, both producer and consumer finished. Pipeline complete!"]
  ];

  var cur = 0, timer = null, playing = false;

  function renderTable() {
    var html = "";
    for (var i = 0; i < frames.length; i++) {
      var f = frames[i], isCur = i === cur;
      var label = i === 0 ? "start" : "&nbsp;&nbsp;t" + i;
      html += '<tr style="' + (isCur ? 'outline:2px solid #7dcfff;outline-offset:-2px' : '') + '">';
      html += '<td style="padding:4px 8px;color:#565f89;text-align:right;border-right:1px solid #565f89">' + label + '</td>';
      html += '<td style="padding:4px 8px;">' + f[1] + '</td>';
      html += '<td style="padding:4px 8px;font-family:monospace;">[' + f[2].join(', ') + ']</td>';
      html += '<td style="padding:4px 8px;">' + f[3] + '</td>';
      html += '</tr>';
    }
    document.getElementById("mif-anim-body").innerHTML = html;
  }

  function renderDag() {
    var f = frames[cur];

    var prodEl = document.getElementById("svg-prod-state");
    prodEl.textContent = f[1];
    if (f[1].indexOf("Blocked") !== -1) {
      prodEl.setAttribute("fill", "#f7768e");
    } else if (f[1].indexOf("Finished") !== -1) {
      prodEl.setAttribute("fill", "#565f89");
    } else if (f[1].indexOf("Submit") !== -1) {
      prodEl.setAttribute("fill", "#e0af68");
    } else {
      prodEl.setAttribute("fill", "#a9b1d6");
    }

    var consEl = document.getElementById("svg-cons-state");
    consEl.textContent = f[3];
    if (f[3].indexOf("Awaiting") !== -1) {
      consEl.setAttribute("fill", "#7dcfff");
    } else if (f[3].indexOf("Finished") !== -1) {
      consEl.setAttribute("fill", "#565f89");
    } else {
      consEl.setAttribute("fill", "#a9b1d6");
    }

    for (var j = 0; j < 3; j++) {
      var val = f[2][j];
      var slotEl = document.getElementById("slot-" + j);
      var textEl = document.getElementById("slot-" + j + "-text");
      textEl.textContent = val;
      if (val !== "-") {
        slotEl.setAttribute("fill", "#bb9af7");
        slotEl.setAttribute("opacity", "0.4");
        textEl.setAttribute("fill", "#ffffff");
      } else {
        slotEl.setAttribute("fill", "#3b4261");
        slotEl.setAttribute("opacity", "1");
        textEl.setAttribute("fill", "#a9b1d6");
      }
    }
  }

  window.mifAnimFrame = function(d) {
    var next = cur + d;
    if (next >= 0 && next < frames.length) cur = next;
    update();
  };

  window.mifAnimToggle = function() {
    if (playing) {
      clearInterval(timer);
      playing = false;
      document.getElementById("mif-anim-play").innerHTML = "▶ Play";
    } else {
      playing = true;
      document.getElementById("mif-anim-play").innerHTML = "⏸ Pause";
      timer = setInterval(function(){
        window.mifAnimFrame(1);
        if (cur >= frames.length - 1) window.mifAnimToggle();
      }, 1500);
    }
  };

  window.mifAnimReset = function() {
    cur = 0;
    update();
  };

  function update() {
    var f = frames[cur];
    document.getElementById("mif-anim-step").innerHTML = cur + "/" + (frames.length - 1);
    document.getElementById("mif-anim-insight").innerHTML = f[4];
    renderTable();
    renderDag();
  }

  update();
})();
</script>

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
