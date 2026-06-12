import glob

f = "tests/async/test_async_runner_materialization.py"
with open(f, "r") as file:
    content = file.read()

old_spy = """    def spy_materialize(g):
        materialized.append("called")
        return list(g)"""

new_spy = """    async def spy_materialize(g):
        materialized.append("called")
        return [x async for x in g]"""

content = content.replace(old_spy, new_spy)

with open(f, "w") as file:
    file.write(content)
