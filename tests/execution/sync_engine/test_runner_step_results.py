import itertools
from collections.abc import Generator, Iterator
from typing import Any

import pytest

from synaflow.execution.sync_engine.pipeline import PipelineExecutor
from synaflow.execution.sync_engine.topology import SyncStreamManager
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS

SYNC_PACK_NAMES = (
    "sync_linear",
    "sync_diamond",
    "sync_complex_parallel",
    "sync_fibonacci",
    "sync_complex_parallel_mixed",
    "sync_sub_pipelines",
    "sync_deep_sub_pipelines",
)


@pytest.mark.parametrize("pack_name", SYNC_PACK_NAMES)
def test_step_results(pack_name):
    pack = SYNC_PACKS[pack_name]

    class TestSyncStreamManager(SyncStreamManager):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.recorded = {}

        def apply_materializer(self, name: str, iterator: Iterator) -> Any:
            res1, res2 = itertools.tee(iterator)
            self.recorded[name] = res1
            return super().apply_materializer(name, res2)

        def tee_iterator_for_consumers(self, name: str, iterator: Iterator) -> Any:
            res1, res2 = itertools.tee(iterator)
            self.recorded[name] = res1
            return super().tee_iterator_for_consumers(name, res2)

    class ContextRecorder(dict):
        def __init__(self, stream_manager, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.stream_manager = stream_manager

        def __setitem__(self, key, value):
            if isinstance(value, (Iterator, Generator)):
                res1, res2 = itertools.tee(value)
                self.stream_manager.recorded[key] = res1
                super().__setitem__(key, res2)
            elif type(value).__name__ != "TeeWrapper":
                self.stream_manager.recorded[key] = value
                super().__setitem__(key, value)
            else:
                super().__setitem__(key, value)

    class TestPipelineExecutor(PipelineExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.stream_manager = TestSyncStreamManager(self.pipeline)
            self.runner.stream_manager = self.stream_manager

            new_ctx = ContextRecorder(self.stream_manager, self.context)
            self.context = new_ctx
            self.resolver.context = new_ctx
            self.runner.context = new_ctx

    executor = TestPipelineExecutor(pack.pipeline)

    if pack.exception_match:
        with pytest.raises(Exception, match=pack.exception_match):
            executor.execute(pack.input_params)
        return

    executor.execute(pack.input_params)

    final_results = {}
    for key, val in executor.stream_manager.recorded.items():
        if isinstance(val, (Iterator, Generator)):
            final_results[key] = list(val)
        else:
            final_results[key] = val

    for key, expected_val in pack.step_results.items():
        assert final_results.get(key) == expected_val
