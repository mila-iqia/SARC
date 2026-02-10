"""MongoDB model and repository for health check state persistence."""

from __future__ import annotations

from datetime import datetime

from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.alerts.common import CheckStatus
from sarc.core.models.validators import datetime_utc
from sarc.model import BaseModel


class HealthCheckState(BaseModel):
    """State of a health check stored in MongoDB.

    This is used by the polling system to track when checks were last run
    and what their status was, enabling scheduled execution without a
    permanent daemon process.
    """

    # Database ID
    id: PydanticObjectId | None = None

    # Unique check name (matches HealthCheck.name)
    name: str

    # Timestamp of last execution (None if never run)
    last_run: datetime_utc | None = None

    # Status of the last run: "ok", "failure", "error", "absent"
    last_status: CheckStatus = CheckStatus.ABSENT

    # Optional summary message (e.g., error description)
    last_message: str | None = None

    # Flag to disable check from database (overrides config)
    active: bool = True


class HealthCheckStateRepository(AbstractRepository[HealthCheckState]):
    """Repository for managing health check state in MongoDB."""

    class Meta:
        collection_name = "healthcheck"

    def get_state(self, name: str) -> HealthCheckState | None:
        """Get the state for a specific check by name."""
        return self.find_one_by({"name": name})

    def get_all_states(self) -> dict[str, HealthCheckState]:
        """Get all check states as a dict keyed by name."""
        return {state.name: state for state in self.find_by({})}

    def update_state(
        self,
        name: str,
        status: CheckStatus,
        message: str | None = None,
        run_time: datetime | None = None,
    ) -> None:
        """Update state for a check after execution."""
        state = self.get_state(name)
        if state is None:
            state = HealthCheckState(name=name)

        state.last_run = run_time  # type: ignore[assignment]
        state.last_status = status
        state.last_message = message
        self.save(state)

    def set_active(self, name: str, active: bool) -> None:
        """Enable or disable a check."""
        state = self.get_state(name)
        if state is None:
            state = HealthCheckState(name=name)

        state.active = active
        self.save(state)
