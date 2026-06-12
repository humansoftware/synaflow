import os

def remove_decorator():
    for root, _, files in os.walk("tests"):
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(root, f)
                with open(path, "r", encoding="utf-8") as file:
                    content = file.read()
                if "@pytest.mark.asyncio\n" in content:
                    content = content.replace("@pytest.mark.asyncio\n", "")
                    with open(path, "w", encoding="utf-8") as file:
                        file.write(content)

if __name__ == "__main__":
    remove_decorator()
