import pickle
from typing import Any


class PickleSerializer:
    extension: str = "pkl"

    def serialize(self, file: Any, data: Any) -> None:
        pickle.dump(data, file)


pickle_serializer = PickleSerializer()
