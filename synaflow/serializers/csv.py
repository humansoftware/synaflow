import csv
import dataclasses
from collections.abc import Iterable, Mapping
from typing import Any


class CsvSerializer:
    extension: str = "csv"

    def __init__(self, fieldnames: list[str] | None = None, delimiter: str = ","):
        self.fieldnames = fieldnames
        self.delimiter = delimiter

    def serialize(self, file: Any, data: Any) -> None:
        def _to_dict_or_row(val: Any) -> Any:
            if dataclasses.is_dataclass(val):
                return dataclasses.asdict(val)
            return val

        if isinstance(data, (str, bytes)):
            rows = [data]
        elif isinstance(data, Iterable) and not isinstance(data, Mapping):
            rows = list(data)
        else:
            rows = [data]

        if not rows:
            return

        converted_rows = [_to_dict_or_row(r) for r in rows]
        first_row = converted_rows[0]

        if isinstance(first_row, Mapping):
            fnames = self.fieldnames or list(first_row.keys())
            writer = csv.DictWriter(file, fieldnames=fnames, delimiter=self.delimiter)
            writer.writeheader()
            for r in converted_rows:
                if isinstance(r, Mapping):
                    writer.writerow(r)
                else:
                    writer.writerow({fnames[0]: r})
        else:
            writer = csv.writer(file, delimiter=self.delimiter)
            for r in converted_rows:
                if isinstance(r, (list, tuple)):
                    writer.writerow(r)
                else:
                    writer.writerow([r])


csv_serializer = CsvSerializer()
