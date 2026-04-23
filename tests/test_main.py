import pytest
import sys
from unittest.mock import patch, MagicMock
from pipeline.run_pipeline import main, start_infra

@patch("pipeline.run_pipeline.start_infra")
@patch("pipeline.run_pipeline.run_command")
@patch("builtins.print")
def test_main_success(mock_print, mock_run_cmd, mock_start):
    # Mock successful execution
    main()
    
    assert mock_start.call_count == 1
    assert mock_run_cmd.call_count >= 3 # ingestion, dbt run, dbt test

@patch("pipeline.run_pipeline.start_infra")
@patch("builtins.print")
def test_main_failure(mock_print, mock_start):
    # Mock failure
    mock_start.side_effect = Exception("infra fail")
    
    with pytest.raises(SystemExit) as excinfo:
        main()
    
    assert excinfo.value.code == 1

@patch("pipeline.run_pipeline.subprocess.run")
@patch("pipeline.run_pipeline.time.sleep")
@patch("builtins.print")
def test_start_infra_healthy(mock_print, mock_sleep, mock_run):
    with patch("pipeline.run_pipeline.run_command") as mock_run_cmd:
        mock_run.return_value = MagicMock(stdout="healthy")
        start_infra()
        mock_run_cmd.assert_called_with("docker compose up -d")
