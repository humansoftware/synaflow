import traceback
from pathlib import Path
from typing import Any

from synaflow.core.types import ErrorMaterializeContext, ErrorRecord
from synaflow.serializers import csv_serializer, json_serializer

from synaflow.core.dag_builder import log_error_materializer_factory


def log_error_materializer():
    return log_error_materializer_factory


def disk_error_materializer(
    path: Path | str,
    serializer: Any,
    file_name: str | None = None,
):
    if serializer in (json_serializer, csv_serializer):
        raise ValueError(
            f"disk_error_materializer does not support '{serializer.__class__.__name__}'. "
            "Please use an append-safe serializer like jsonl_serializer, text_serializer, or pickle_serializer."
        )

    base_path = Path(path)

    def factory(ctx: ErrorMaterializeContext):
        ext = getattr(serializer, "extension", "txt")
        fname = file_name or f"{ctx.dataset_name}.{ext}"
        target_path = base_path / fname

        def append_error_to_disk(exc: BaseException, runtime_context=None) -> None:
            target_path.parent.mkdir(parents=True, exist_ok=True)

            record = ErrorRecord(
                pipeline_name=ctx.pipeline_name,
                dataset_name=ctx.dataset_name,
                step_name=getattr(runtime_context, "step_name", ctx.step_name),
                run_id=getattr(runtime_context, "run_id", None),
                exception_type=type(exc).__name__,
                exception_message=str(exc),
                traceback=traceback.format_exc(),
            )

            is_bin = ext in ("pkl", "pickle") or getattr(serializer, "binary", False)
            mode = "ab" if is_bin else "a"
            encoding = None if is_bin else "utf-8"

            with open(target_path, mode, encoding=encoding) as f:
                if hasattr(serializer, "serialize"):
                    serializer.serialize(f, record)
                else:
                    serializer(f, record)

        return append_error_to_disk

    return factory
