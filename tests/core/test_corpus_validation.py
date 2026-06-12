import pytest

from synaflow.core.pipeline import PipelineDef
from tests.execution.async_engine.corpus import EXAMPLES as ASYNC_EXAMPLES
from tests.execution.sync_engine.corpus import EXAMPLES as SYNC_EXAMPLES

EXAMPLES = {**SYNC_EXAMPLES, **ASYNC_EXAMPLES}


@pytest.mark.parametrize("pipeline_name, pipeline_def", EXAMPLES.items())
def test_corpus_compiles_and_validates(pipeline_name: str, pipeline_def: PipelineDef):
    # This simply tests that the pipeline successfully built its DAG dictionary
    # during initialization and that it is fully compatible.
    dag = pipeline_def.to_dict()
    assert isinstance(dag, dict)
    assert len(dag) > 0

    # Check that all nodes have the required keys
    for node_name, node_info in dag.items():
        assert "deps" in node_info
        assert "output" in node_info
        assert "needs_materialize" in node_info
