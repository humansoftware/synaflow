from synaflow.core.dag_builder import build_dag
from tests.common.pipeline_pack import PipelinePack
import pytest
from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS

PACKS = {**SYNC_PACKS, **ASYNC_PACKS}


@pytest.mark.parametrize("pack_name, pack", PACKS.items())
def test_corpus_compiles_and_validates(pack_name: str, pack: PipelinePack):
    assert build_dag(pack.pipeline) is not None
    assert len(build_dag(pack.pipeline).steps) > 0
