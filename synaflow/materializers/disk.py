from pathlib import Path
from collections.abc import Iterator
from typing import Any
from synaflow.core.types import MaterializeContext


def disk_materializer(
    path: Path | str,
    serializer: Any,
    file_name: str | None = None,
):
    base_path = Path(path)

    def factory(ctx: MaterializeContext):
        ext = getattr(serializer, "extension", "txt")
        fname = file_name or f"{ctx.dataset_name}.{ext}"
        target_path = base_path / fname

        def concrete(value: Any) -> Any:
            target_path.parent.mkdir(parents=True, exist_ok=True)

            if isinstance(value, Iterator):
                value = list(value)

            is_bin = ext in ("pkl", "pickle") or getattr(serializer, "binary", False)
            mode = "wb" if is_bin else "w"
            encoding = None if is_bin else "utf-8"

            with open(target_path, mode, encoding=encoding) as f:
                if hasattr(serializer, "serialize"):
                    serializer.serialize(f, value)
                else:
                    serializer(f, value)

            return value

        return concrete

    return factory
