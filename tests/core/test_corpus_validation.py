import pytest

from tests.execution.async_engine.corpus import PACKS as ASYNC_PACKS
from tests.execution.sync_engine.corpus import PACKS as SYNC_PACKS
from tests.pipeline_pack import PipelinePack

PACKS = {**SYNC_PACKS, **ASYNC_PACKS}


@pytest.mark.parametrize("pack_name, pack", PACKS.items())
def test_corpus_compiles_and_validates(pack_name: str, pack: PipelinePack):
    # This simply tests that the pipeline successfully built its DAG dictionary
    # during initialization and that it is fully compatible.
    dag = pack.pipeline.to_dict()
    assert isinstance(dag, dict)
    assert len(dag) > 0

    # Check that all nodes have the required keys
    for node_name, node_info in dag.items():
        assert "deps" in node_info
        assert "output" in node_info
        assert "needs_materialize" in node_info
