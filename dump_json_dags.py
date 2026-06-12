import json

from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS

for name, pack in SYNC_PACKS.items():
    print(f"--- SYNC {name} ---")
    print(json.dumps(pack.pipeline.to_dict(), indent=4))

for name, pack in ASYNC_PACKS.items():
    print(f"--- ASYNC {name} ---")
    print(json.dumps(pack.pipeline.to_dict(), indent=4))
