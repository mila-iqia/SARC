"""MongoDB model and repository for health check state persistence."""

from __future__ import annotations

from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.alerts.common import HealthCheck, CheckResult
from sarc.config import config
from sarc.model import BaseModel


class HealthCheckState(BaseModel):
    """State of a health check stored in MongoDB.

    This is used by the polling system to track when checks were last run
    and what their status was, enabling scheduled execution without a
    permanent daemon process.
    """

    # Database ID
    id: PydanticObjectId | None = None

    # Check configuration
    check: HealthCheck

    # Check last result (None if never run)
    # Contains `status` and `issue_date` (last run time)
    last_result: CheckResult | None = None

    # Optional summary message (e.g., error description)
    last_message: str | None = None


class HealthCheckStateRepository(AbstractRepository[HealthCheckState]):
    """Repository for managing health check state in MongoDB."""

    class Meta:
        collection_name = "healthcheck"

    def get_state(self, name: str) -> HealthCheckState | None:
        """Get the state for a specific check by name."""
        return self.find_one_by({"check.name": name})

    def update_state(self, state: HealthCheckState):
        assert state.check is not None
        if state.last_result is not None and state.last_result.check is not None:
            assert state.last_result.check is state.check
            state.last_result.check = None
        self.save(state)


def get_healthcheck_state_collection() -> HealthCheckStateRepository:
    """Return the health check state collection in the current MongoDB."""
    db = config().mongo.database_instance
    return HealthCheckStateRepository(db)
