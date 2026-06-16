import re
from pathlib import Path

p = Path("tests/execution/async_engine/test_async_runner_max_in_flight.py")
content = p.read_text()

content = content.replace("for i in range(4):", "for i in range(5):")
content = content.replace('prod_3_index = log.index("prod 3")', 'prod_4_index = log.index("prod 4")')
content = content.replace('assert slow_0_index < prod_3_index', 'assert slow_0_index < prod_4_index')

p.write_text(content)
