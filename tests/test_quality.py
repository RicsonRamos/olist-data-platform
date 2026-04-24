from unittest.mock import MagicMock

import pandas as pd

from pipeline.quality import DataQuality


def test_check_schema_drift():
    df = pd.DataFrame({"col1": [1], "col2": [2]})
    expected = ["col1", "col2"]

    # Success
    assert DataQuality.check_schema_drift(df, expected, "test_table") is True

    # Drift (extra column)
    expected_small = ["col1"]
    assert DataQuality.check_schema_drift(df, expected_small, "test_table") is True

    # Missing (failure)
    expected_big = ["col1", "col2", "col3"]
    assert DataQuality.check_schema_drift(df, expected_big, "test_table") is False


def test_check_volume_anomaly():
    mock_engine = MagicMock()
    mock_conn = mock_engine.connect.return_value.__enter__.return_value

    # Mock average of 100
    mock_conn.execute.return_value.scalar.return_value = 100

    # Within threshold
    assert (
        DataQuality.check_volume_anomaly(mock_engine, "orders", 120, threshold=0.5)
        is True
    )

    # Outside threshold
    assert (
        DataQuality.check_volume_anomaly(mock_engine, "orders", 300, threshold=0.5)
        is False
    )
