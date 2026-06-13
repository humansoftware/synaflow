import glob

files = glob.glob("tests/async/test_async_runner_*.py")
for f in files:
    with open(f, "r") as file:
        content = file.read()
    if "from synaflow import async_run" not in content:
        content = "from synaflow import async_run\n" + content
    with open(f, "w") as file:
        file.write(content)
