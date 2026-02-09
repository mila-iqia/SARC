"""Unit tests for the health poll command."""

from unittest.mock import MagicMock

from gifnoc.std import time

from sarc.alerts.healthcheck_state import HealthCheckState, HealthCheckStateRepository


class TestHealthCheckState:
    """Tests for HealthCheckState model."""

    def test_to_dict(self):
        """Test conversion to MongoDB document format."""
        state = HealthCheckState(
            name="test_check",
            last_run=None,
            last_status="ok",
            last_message="All good",
            active=True,
        )
        doc = state.to_dict()
        assert doc["name"] == "test_check"
        assert doc["last_status"] == "ok"
        assert doc["last_message"] == "All good"
        assert doc["active"] is True

    def test_from_dict(self):
        """Test creation from MongoDB document."""
        doc = {
            "name": "test_check",
            "last_run": None,
            "last_status": "failure",
            "last_message": "Something went wrong",
            "active": False,
        }
        state = HealthCheckState.from_dict(doc)
        assert state.name == "test_check"
        assert state.last_status == "failure"
        assert state.last_message == "Something went wrong"
        assert state.active is False

    def test_from_dict_defaults(self):
        """Test that missing fields get defaults."""
        doc = {"name": "minimal"}
        state = HealthCheckState.from_dict(doc)
        assert state.name == "minimal"
        assert state.last_run is None
        assert state.last_status == "absent"
        assert state.last_message is None
        assert state.active is True


class TestHealthCheckStateRepository:
    """Tests for HealthCheckStateRepository."""

    def test_get_state_not_found(self):
        """Test get_state returns None for missing check."""
        mock_collection = MagicMock()
        mock_collection.find_one.return_value = None
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        repo = HealthCheckStateRepository(mock_db)

        result = repo.get_state("nonexistent")
        assert result is None

    def test_get_state_found(self):
        """Test get_state returns HealthCheckState for existing check."""
        mock_collection = MagicMock()
        mock_collection.find_one.return_value = {
            "name": "my_check",
            "last_status": "ok",
        }
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        repo = HealthCheckStateRepository(mock_db)

        result = repo.get_state("my_check")
        assert result is not None
        assert result.name == "my_check"
        assert result.last_status == "ok"

    def test_update_state(self):
        """Test update_state calls MongoDB update_one with correct params."""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        repo = HealthCheckStateRepository(mock_db)
        now = time.now()

        repo.update_state("test", "ok", "Success", now)

        mock_collection.update_one.assert_called_once()
        call_args = mock_collection.update_one.call_args
        assert call_args[0][0] == {"name": "test"}  # filter
        assert call_args[1]["upsert"] is True

    def test_set_active(self):
        """Test set_active calls MongoDB update_one."""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        repo = HealthCheckStateRepository(mock_db)

        repo.set_active("test", False)

        mock_collection.update_one.assert_called_once()
        call_args = mock_collection.update_one.call_args
        assert call_args[0][0] == {"name": "test"}
        assert call_args[1]["upsert"] is True
