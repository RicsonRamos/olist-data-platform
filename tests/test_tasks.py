import pytest
from unittest.mock import patch, MagicMock
from pipeline.run_pipeline_tasks import run_command, start_infra, run_ingestion, run_dbt_transformations, run_dbt_tests

def test_run_command_success():
    with patch("pipeline.run_pipeline_tasks.subprocess.Popen") as mock_popen:
        mock_process = mock_popen.return_value
        mock_process.wait.return_value = 0
        mock_process.returncode = 0
        
        run_command("echo hello")
        mock_popen.assert_called_once()

def test_run_command_failure():
    with patch("pipeline.run_pipeline_tasks.subprocess.Popen") as mock_popen:
        mock_process = mock_popen.return_value
        mock_process.wait.return_value = 1
        mock_process.returncode = 1
        
        with pytest.raises(Exception, match="Command failed"):
            run_command("false")

@patch("pipeline.run_pipeline_tasks.subprocess.run")
@patch("pipeline.run_pipeline_tasks.time.sleep")
def test_start_infra_healthy(mock_sleep, mock_run):
    with patch("pipeline.run_pipeline_tasks.run_command") as mock_run_cmd:
        mock_run.return_value = MagicMock(stdout="healthy")
        start_infra()
        mock_run_cmd.assert_called_with("docker compose up -d")

@patch("pipeline.run_pipeline_tasks.run_command")
def test_run_ingestion(mock_run_cmd):
    run_ingestion()
    mock_run_cmd.assert_called()

@patch("pipeline.run_pipeline_tasks.get_engine")
@patch("pipeline.run_pipeline_tasks.run_command")
def test_run_dbt_transformations(mock_run_cmd, mock_get_engine):
    mock_get_engine.return_value = MagicMock()
    run_dbt_transformations()
    mock_run_cmd.assert_called()

@patch("pipeline.run_pipeline_tasks.get_engine")
@patch("pipeline.run_pipeline_tasks.run_command")
def test_run_dbt_tests(mock_run_cmd, mock_get_engine):
    mock_get_engine.return_value = MagicMock()
    run_dbt_tests()
    mock_run_cmd.assert_called()
