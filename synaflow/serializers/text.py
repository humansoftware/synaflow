from collections.abc import Iterable
from typing import Any


class TextSerializer:
    extension: str = "txt"

    def serialize(self, file: Any, data: Any) -> None:
        if isinstance(data, (str, bytes)):
            if isinstance(data, bytes):
                file.write(data.decode("utf-8") if hasattr(file, "encoding") else data)
            else:
                file.write(data)
        elif isinstance(data, Iterable):
            for item in data:
                file.write(str(item) + "\n")
        else:
            file.write(str(data))


text_serializer = TextSerializer()
