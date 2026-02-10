"""Unit tests for the health poll command."""

from unittest.mock import MagicMock

from gifnoc.std import time

from sarc.alerts.common import CheckStatus
from sarc.alerts.healthcheck_state import HealthCheckState, HealthCheckStateRepository


class TestHealthCheckState:
    """Tests for HealthCheckState model."""

    def test_model_dump(self):
        """Test conversion to dict."""
        state = HealthCheckState(
            name="test_check",
            last_run=None,
            last_status="ok",
            last_message="All good",
            active=True,
        )
        doc = state.model_dump()
        assert doc["name"] == "test_check"
        assert doc["last_status"] == "ok"
        assert doc["last_message"] == "All good"
        assert doc["active"] is True

    def test_model_validate(self):
        """Test creation from dict."""
        doc = {
            "name": "test_check",
            "last_run": None,
            "last_status": "failure",
            "last_message": "Something went wrong",
            "active": False,
        }
        state = HealthCheckState.model_validate(doc)
        assert state.name == "test_check"
        assert state.last_status == "failure"
        assert state.last_message == "Something went wrong"
        assert state.active is False

    def test_model_validate_defaults(self):
        """Test that missing fields get defaults."""
        doc = {"name": "minimal"}
        state = HealthCheckState.model_validate(doc)
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
        """Test update_state calls MongoDB save (via update_one or insert_one)."""
        mock_collection = MagicMock()
        # Mock find_one to return None (new state) or a dict (existing state)
        mock_collection.find_one.return_value = None
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        repo = HealthCheckStateRepository(mock_db)
        now = time.now()

        repo.update_state("test", CheckStatus.OK, "Success", now)

        # AbstractRepository.save() typically calls replace_one or similar
        # depending on whether the object has an ID.
        assert mock_collection.replace_one.called or mock_collection.insert_one.called

    def test_set_active(self):
        """Test set_active calls MongoDB."""
        mock_collection = MagicMock()
        mock_collection.find_one.return_value = None
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        repo = HealthCheckStateRepository(mock_db)

        repo.set_active("test", False)

        assert mock_collection.replace_one.called or mock_collection.insert_one.called

    def test_get_all_states(self):
        """Test get_all_states returns a dict of HealthCheckState."""
        mock_collection = MagicMock()
        mock_collection.find.return_value = [
            {"name": "check1", "last_status": "ok"},
            {"name": "check2", "last_status": "failure"},
        ]
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        repo = HealthCheckStateRepository(mock_db)

        states = repo.get_all_states()
        assert len(states) == 2
        assert states["check1"].name == "check1"
        assert states["check1"].last_status == "ok"
        assert states["check2"].name == "check2"
        assert states["check2"].last_status == "failure"
