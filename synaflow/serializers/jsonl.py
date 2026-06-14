import json
import dataclasses
from collections.abc import Iterable
from typing import Any


class JsonlSerializer:
    extension: str = "jsonl"

    def serialize(self, file: Any, data: Any) -> None:
        def _to_dict_or_val(val: Any) -> Any:
            if dataclasses.is_dataclass(val):
                return dataclasses.asdict(val)
            return val

        if isinstance(data, (str, bytes)):
            file.write(json.dumps(_to_dict_or_val(data)) + "\n")
        elif isinstance(data, Iterable):
            for item in data:
                file.write(json.dumps(_to_dict_or_val(item)) + "\n")
        else:
            file.write(json.dumps(_to_dict_or_val(data)) + "\n")


jsonl_serializer = JsonlSerializer()
