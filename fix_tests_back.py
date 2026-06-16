import re
from pathlib import Path

for engine in ["async_engine", "sync_engine"]:
    for f in ["complex_parallel.py", "complex_parallel_mixed.py", "fibonacci.py", "mixed_fanout.py"]:
        p = Path(f"tests/execution/{engine}/corpus/{f}")
        if not p.exists(): continue
        
        content = p.read_text()
        content = content.replace("max_in_flight=100", "")
        content = content.replace("max_in_flight: 100", "max_in_flight: 1")
        # Python code might have had `, max_in_flight=100` which I removed via `max_in_flight=100` to ``, so we need to clean up any trailing commas or `, )` to `)`
        content = content.replace(", )", ")")
        
        p.write_text(content)

# Also fix the materialization tests
for test_file in ["tests/execution/sync_engine/test_runner_materialization.py", "tests/execution/async_engine/test_async_runner_materialization.py"]:
    p = Path(test_file)
    content = p.read_text()
    content = content.replace(", max_in_flight=100", "")
    p.write_text(content)

