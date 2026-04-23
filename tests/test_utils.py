import pytest
from unittest.mock import MagicMock, patch
from pipeline.utils import get_engine, log_job

def test_get_engine(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_pass")
    monkeypatch.setenv("POSTGRES_DB", "test_db")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    
    with patch("pipeline.utils.create_engine") as mock_create:
        get_engine()
        mock_create.assert_called_once_with("postgresql://test_user:test_pass@localhost:5432/test_db")

def test_log_job():
    mock_engine = MagicMock()
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    
    log_job(mock_engine, "test_job", "SUCCESS", 1000, rows=10)
    
    mock_engine.begin.assert_called_once()
    mock_conn.execute.assert_called_once()
    # Fixed call_args check
    args, kwargs = mock_conn.execute.call_args
    params = args[1] if len(args) > 1 else kwargs.get("parameters") or args[0].compile().params if hasattr(args[0], 'compile') else args[1]
    # Simple check on the dict passed to execute
    sent_params = args[1]
    assert sent_params["job_name"] == "test_job"
    assert sent_params["status"] == "SUCCESS"
    assert sent_params["rows"] == 10
