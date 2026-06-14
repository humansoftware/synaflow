import json
from typing import Any


class JsonSerializer:
    extension: str = "json"

    def serialize(self, file: Any, data: Any) -> None:
        json.dump(data, file)


json_serializer = JsonSerializer()
