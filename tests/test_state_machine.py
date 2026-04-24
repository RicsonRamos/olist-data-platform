from unittest.mock import MagicMock, patch

from pipeline.state_machine import PipelineState, StateMachine


@patch("pipeline.state_machine.get_engine")
def test_state_machine_transition(mock_get_engine):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    sm = StateMachine(run_id="test_run")
    sm.transition_to(PipelineState.RUNNING, metadata={"step": "ingestion"})

    mock_engine.begin.assert_called()


def test_pipeline_state_constants():
    assert PipelineState.PENDING == "PENDING"
    assert PipelineState.SUCCESS == "SUCCESS"
