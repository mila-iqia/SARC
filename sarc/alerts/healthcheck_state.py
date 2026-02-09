"""MongoDB model and repository for health check state persistence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from gifnoc.std import time

if TYPE_CHECKING:
    from pymongo.collection import Collection
    from pymongo.database import Database


@dataclass
class HealthCheckState:
    """State of a health check stored in MongoDB.

    This is used by the polling system to track when checks were last run
    and what their status was, enabling scheduled execution without a
    permanent daemon process.
    """

    # Unique check name (matches HealthCheck.name)
    name: str

    # Timestamp of last execution (None if never run)
    last_run: datetime | None = None

    # Status of the last run: "ok", "failure", "error", "absent"
    last_status: str = "absent"

    # Optional summary message (e.g., error description)
    last_message: str | None = None

    # Flag to disable check from database (overrides config)
    active: bool = True

    def to_dict(self) -> dict:
        """Convert to MongoDB document format."""
        return {
            "name": self.name,
            "last_run": self.last_run,
            "last_status": self.last_status,
            "last_message": self.last_message,
            "active": self.active,
        }

    @classmethod
    def from_dict(cls, data: dict) -> HealthCheckState:
        """Create from MongoDB document."""
        return cls(
            name=data["name"],
            last_run=data.get("last_run"),
            last_status=data.get("last_status", "absent"),
            last_message=data.get("last_message"),
            active=data.get("active", True),
        )


class HealthCheckStateRepository:
    """Repository for managing health check state in MongoDB."""

    COLLECTION_NAME = "healthcheck"

    def __init__(self, database: Database):
        self._database = database

    def get_collection(self) -> Collection:
        """Return the healthcheck collection."""
        return self._database[self.COLLECTION_NAME]

    def get_state(self, name: str) -> HealthCheckState | None:
        """Get the state for a specific check by name."""
        doc = self.get_collection().find_one({"name": name})
        if doc is None:
            return None
        return HealthCheckState.from_dict(doc)

    def get_all_states(self) -> dict[str, HealthCheckState]:
        """Get all check states as a dict keyed by name."""
        return {
            doc["name"]: HealthCheckState.from_dict(doc)
            for doc in self.get_collection().find()
        }

    def update_state(
        self,
        name: str,
        status: str,
        message: str | None = None,
        run_time: datetime | None = None,
    ) -> None:
        """Update state for a check after execution."""
        if run_time is None:
            run_time = time.now()

        self.get_collection().update_one(
            {"name": name},
            {
                "$set": {
                    "last_run": run_time,
                    "last_status": status,
                    "last_message": message,
                },
                "$setOnInsert": {
                    "name": name,
                    "active": True,
                },
            },
            upsert=True,
        )

    def set_active(self, name: str, active: bool) -> None:
        """Enable or disable a check."""
        self.get_collection().update_one(
            {"name": name},
            {
                "$set": {"active": active},
                "$setOnInsert": {
                    "name": name,
                    "last_run": None,
                    "last_status": "absent",
                    "last_message": None,
                },
            },
            upsert=True,
        )
