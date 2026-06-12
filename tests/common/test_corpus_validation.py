import pytest

from synaflow.pipeline import PipelineDef
import importlib

SYNC_EXAMPLES = importlib.import_module("tests.sync.corpus").EXAMPLES
ASYNC_EXAMPLES = importlib.import_module("tests.async.corpus").EXAMPLES

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
