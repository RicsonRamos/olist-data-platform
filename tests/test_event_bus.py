from unittest.mock import MagicMock, patch

from pipeline.event_bus import EventBus


@patch("pipeline.event_bus.get_engine")
def test_event_bus_emit(mock_get_engine):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.return_value = (1,)  # event_id

    bus = EventBus(run_id="test_run")
    event_id = bus.emit("FILE_ARRIVED", payload={"file": "test.csv"})

    assert event_id == 1
    mock_engine.begin.assert_called()


@patch("pipeline.event_bus.get_engine")
def test_event_bus_mark_processed(mock_get_engine):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    bus = EventBus(run_id="test_run")
    bus.mark_processed(1, "ingestion_group")

    mock_engine.begin.assert_called()


@patch("pipeline.event_bus.get_engine")
def test_event_bus_get_history(mock_get_engine):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine
    mock_conn = mock_engine.connect.return_value.__enter__.return_value

    # Mock row with _mapping
    mock_row = MagicMock()
    mock_row._mapping = {"event_id": 1, "event_type": "FILE_ARRIVED"}
    mock_conn.execute.return_value = [mock_row]

    bus = EventBus(run_id="test_run")
    history = bus.get_history(event_type="FILE_ARRIVED")

    assert len(history) == 1
    assert history[0]["event_id"] == 1
