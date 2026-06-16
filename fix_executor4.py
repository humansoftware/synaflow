import re
from pathlib import Path

p_sync = Path("synaflow/execution/sync_engine/executor.py")
content = p_sync.read_text()

content = content.replace(
    'raise RuntimeError(\n                                    f"max_in_flight bound of {max_in_flight} exceeded during sync fan-out."\n                                )',
    'raise PipelineStopException(step_name=step_name, cause=RuntimeError(f"max_in_flight bound of {max_in_flight} exceeded during sync fan-out."))'
)

p_sync.write_text(content)
