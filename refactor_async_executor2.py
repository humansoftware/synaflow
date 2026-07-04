import re

with open("synaflow/execution/async_engine/executor.py", "r") as f:
    content = f.read()

# Remove the leftover methods
content = re.sub(
    r"    async def _dispatch_step_event\([\s\S]*?await dispatch_observers_async\(registrations, ctx\)\n",
    "",
    content,
)

content = re.sub(
    r"    async def _dispatch_materialization_event\([\s\S]*?await dispatch_observers_async\(registrations, ctx\)\n",
    "",
    content,
)

# And I need to also remove `_resolve_step_observers` which might still be there if the first regex didn't match it?
# Let's just remove anything from `def _resolve_step_observers` up to `def _dispatch_pipeline_event`? Wait, I already removed `_dispatch_pipeline_event`?
# Let's just run a broad script.

with open("synaflow/execution/async_engine/executor.py", "w") as f:
    f.write(content)
