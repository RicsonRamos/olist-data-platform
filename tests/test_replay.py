from unittest.mock import MagicMock, patch

from pipeline.replay_engine import ReplayEngine


@patch("builtins.print")
def test_replay_entity(mock_print):
    mock_bus = MagicMock()
    mock_processor = MagicMock()

    # Mock history
    mock_bus.get_history.return_value = [
        {"payload": {"file": "test.csv"}, "event_id": 1}
    ]

    re = ReplayEngine(mock_bus, mock_processor)
    re.replay_entity("orders")

    mock_bus.get_history.assert_called_once()
    mock_processor.process_event.assert_called_once()
