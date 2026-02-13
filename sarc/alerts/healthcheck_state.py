"""MongoDB model and repository for health check state persistence."""

from __future__ import annotations

from typing import Any, Annotated

from pydantic import PlainSerializer, BeforeValidator
from pydantic_mongo import AbstractRepository, PydanticObjectId
from serieux import serialize, deserialize, TaggedSubclass

from sarc.alerts.common import HealthCheck, CheckResult
from sarc.config import config
from sarc.model import BaseModel


def _serialize_health_check(hc: HealthCheck) -> dict[str, Any]:
    """
    Special serializer for HealthCheck.

    Use serieux serializer with TaggedSubclass
    to be sure check class is saved in model
    """
    return serialize(TaggedSubclass[HealthCheck], hc)


def _validate_health_check(v: Any) -> HealthCheck:
    """Special deserializer for HealthCheck"""
    if isinstance(v, HealthCheck):
        return v
    assert isinstance(v, dict)
    return deserialize(TaggedSubclass[HealthCheck], v)


HealthCheckPydantic = Annotated[
    HealthCheck,
    PlainSerializer(_serialize_health_check, return_type=dict),
    BeforeValidator(_validate_health_check),
]


def _serialize_check_result(result: CheckResult) -> dict[str, Any]:
    """
    Special serializer for CheckResult

    Since we already save associated HealthCheck object,
    we prevent recursion and save space by cleaning `result.check`.
    """
    result.check = None
    return serialize(TaggedSubclass[CheckResult], result)


def _validate_check_result(v: Any) -> CheckResult:
    """Special deserializer for CheckResult"""
    if isinstance(v, CheckResult):
        return v
    assert isinstance(v, dict)
    return deserialize(TaggedSubclass[CheckResult], v)


CheckResultPydantic = Annotated[
    CheckResult,
    PlainSerializer(_serialize_check_result, return_type=dict),
    BeforeValidator(_validate_check_result),
]


class HealthCheckState(BaseModel):
    """State of a health check stored in MongoDB.

    This is used by the polling system to track when checks were last run
    and what their status was, enabling scheduled execution without a
    permanent daemon process.

    Combine:
    - a HealthCheck configuration object,
    - the last check result
    - the last result message
    """

    # Database ID
    id: PydanticObjectId | None = None

    # Check configuration
    # With annotated class for custom serialization
    check: HealthCheckPydantic

    # Last check result (None if never run)
    # With annotated class for custom serialization
    # Contains `status` and `issue_date` (last run time)
    last_result: CheckResultPydantic | None = None

    # Optional summary message (e.g., error description)
    last_message: str | None = None


class HealthCheckStateRepository(AbstractRepository[HealthCheckState]):
    """Repository for managing health check state in MongoDB."""

    class Meta:
        collection_name = "healthcheck"

    def get_state(self, name: str) -> HealthCheckState | None:
        """Get the state for a specific check by name."""
        return self.find_one_by({"check.name": name})


def get_healthcheck_state_collection() -> HealthCheckStateRepository:
    """Return the health check state collection in the current MongoDB."""
    db = config().mongo.database_instance
    return HealthCheckStateRepository(db)
