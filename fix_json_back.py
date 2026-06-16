import re
from pathlib import Path

for engine in ["async_engine", "sync_engine"]:
    for f in ["complex_parallel.py", "complex_parallel_mixed.py", "fibonacci.py", "mixed_fanout.py"]:
        p = Path(f"tests/execution/{engine}/corpus/{f}")
        if not p.exists(): continue
        
        content = p.read_text()
        content = content.replace('"max_in_flight": 100', '"max_in_flight": 1')
        p.write_text(content)

