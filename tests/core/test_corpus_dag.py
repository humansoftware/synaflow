import pytest

from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS
from tests.pipeline_pack import PipelinePack

PACKS = {**SYNC_PACKS, **ASYNC_PACKS}


@pytest.mark.parametrize("pack_name, pack", PACKS.items())
def test_corpus_compiles_and_validates(pack_name: str, pack: PipelinePack):
    assert pack.pipeline.dag is not None
    assert len(pack.pipeline.dag.steps) > 0
