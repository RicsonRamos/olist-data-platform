import pytest
from unittest.mock import MagicMock, patch
from pipeline.event_processor import EventProcessor
from pipeline.state_machine import PipelineState

def test_process_event_dedup():
    mock_bus = MagicMock()
    # Mock event already registered
    mock_bus.emit.return_value = None
    mock_state = MagicMock()
    
    ep = EventProcessor(mock_bus, mock_state)
    ep.process_event("TEST_EVENT")
    
    mock_bus.mark_processed.assert_not_called()

@patch("pipeline.event_processor.Orchestrator")
def test_process_file_arrival(mock_orch_class):
    mock_bus = MagicMock()
    mock_bus.emit.return_value = 123
    mock_state = MagicMock()
    
    mock_orch = mock_orch_class.return_value
    mock_orch.tasks = {"Ingestion": MagicMock(status="SUCCESS")}
    
    ep = EventProcessor(mock_bus, mock_state)
    
    with patch.object(ep, "_handle_file_arrival") as mock_handle:
        ep.process_event("FILE_ARRIVED", payload={"file": "x.csv"})
        mock_handle.assert_called_once()

def test_handle_file_arrival_success():
    mock_bus = MagicMock()
    mock_state = MagicMock()
    ep = EventProcessor(mock_bus, mock_state)
    
    with patch("pipeline.run_pipeline_tasks.run_ingestion"), \
         patch("pipeline.run_pipeline_tasks.run_dbt_transformations"), \
         patch("pipeline.run_pipeline_tasks.run_dbt_tests"):
        
        ep._handle_file_arrival({"file": "test.csv"})
        
        # Check if state transitioned to SUCCESS
        mock_state.transition_to.assert_any_call(PipelineState.SUCCESS)
        # Check if COMPLETED event was emitted
        mock_bus.emit.assert_any_call("PIPELINE_COMPLETED")
