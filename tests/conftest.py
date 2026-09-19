import pytest

from synaflow.core.dag_builder import build_dag
from synaflow.execution.sync_engine.executor import run as sync_run


@pytest.fixture
def run_pipeline():
    """Run a pipeline through the sync engine.

    NOTE: this fixture is intentionally sync-only.  Pipelines are
    engine-specific by contract — a sync pipeline (plain ``def`` steps,
    ``Iterator`` streams) is rejected by ``async_run`` and vice versa —
    so a single parameterized fixture cannot execute the same
    definition on both engines.  Sync/async parity is guaranteed by the
    mirrored engine test directories (name-checked by
    ``tests/core/test_parity.py``) plus the corpus packs, which exist in
    sync and async flavors with identical topology.
    """

    def wrapper(pipeline, params, **kwargs):
        return sync_run(build_dag(pipeline), params, **kwargs)

    return wrapper
