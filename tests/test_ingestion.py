import os
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from pipeline.ingestion import calculate_file_hash, clean_column_name, RAW_SCHEMA, ingest_data, is_file_processed

def test_calculate_file_hash(tmp_path):
    d = tmp_path / "test.txt"
    d.write_text("hello world")
    
    hash1 = calculate_file_hash(str(d))
    hash2 = calculate_file_hash(str(d))
    
    assert hash1 == hash2
    assert len(hash1) == 64 # SHA-256

def test_clean_column_name():
    assert clean_column_name("Order_ID_Dataset") == "order_id"
    assert clean_column_name("__Customer_Name__") == "customer_name"
    assert clean_column_name("Price") == "price"

def test_raw_schema_constant():
    assert RAW_SCHEMA == 'raw'

def test_is_file_processed():
    mock_engine = MagicMock()
    mock_conn = mock_engine.connect.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = (1,)
    
    assert is_file_processed(mock_engine, "hash123") is True
    
    mock_conn.execute.return_value.fetchone.return_value = None
    assert is_file_processed(mock_engine, "hash123") is False

@patch("pipeline.ingestion.get_engine")
@patch("pipeline.ingestion.os.listdir")
@patch("pipeline.ingestion.pd.read_csv")
@patch("pipeline.ingestion.os.makedirs")
@patch("pipeline.ingestion.shutil.move")
@patch("pipeline.ingestion.calculate_file_hash")
@patch("pipeline.ingestion.is_file_processed")
@patch("pipeline.ingestion.log_job")
@patch("builtins.print")
@patch("builtins.open")
def test_ingest_data_full_flow(mock_open, mock_print, mock_log, mock_is_proc, mock_hash, mock_move, mock_mkdir, mock_read_csv, mock_listdir, mock_get_engine):
    # Setup mocks
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    mock_listdir.return_value = ["test_dataset.csv"]
    mock_is_proc.return_value = False
    mock_hash.return_value = "hash123"
    
    # Mock for audit_setup.sql content
    mock_file = MagicMock()
    mock_file.read.return_value = "CREATE TABLE IF NOT EXISTS ..."
    mock_open.return_value.__enter__.return_value = mock_file
    
    # Use a real DataFrame but mock the IO methods to avoid side effects
    df_real = pd.DataFrame({"Order_ID_Dataset": [1]})
    mock_read_csv.return_value = df_real
    
    with patch.object(pd.DataFrame, "to_parquet") as mock_to_parquet, \
         patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
        
        ingest_data()
        
        mock_read_csv.assert_called_once()
        mock_to_parquet.assert_called_once()
        mock_to_sql.assert_called_once()
        mock_move.assert_called_once()
