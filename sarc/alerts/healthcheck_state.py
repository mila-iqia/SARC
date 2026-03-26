"""MongoDB model and repository for health check state persistence."""

from __future__ import annotations

from typing import Annotated, Any, Iterable

import pymongo
from pydantic import BaseModel, BeforeValidator, PlainSerializer, WithJsonSchema
from pydantic_mongo import AbstractRepository, PydanticObjectId
from serieux import RefPolicy, TaggedSubclass, deserialize, schema, serialize

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.config import config

SERIEUX_CLASS_NAME = "$class"
MONGODB_CLASS_NAME = "_class_"


def _serialize_class_field(data: dict):
    """
    Encode serieux field name `$class` after serialization,
    so that MongoDB won't complain.

    serieux saves class name in a field named "$class",
    but some versions of MongoDB won't accept field names
    that start with "$". As a workaround, this class
    replaces "$class" with "_class_" after serializing,
    and "_class_" with "$class" before deserializing.
    """
    if SERIEUX_CLASS_NAME in data:
        data[MONGODB_CLASS_NAME] = data.pop(SERIEUX_CLASS_NAME)
    for value in data.values():
        if isinstance(value, dict):
            _serialize_class_field(value)


def _deserialize_class_field(data: dict):
    """
    Decode serieux field name `$class` before deserialization,
    so that serieux won't complain.
    """
    if MONGODB_CLASS_NAME in data:
        data[SERIEUX_CLASS_NAME] = data.pop(MONGODB_CLASS_NAME)
    for value in data.values():
        if isinstance(value, dict):
            _deserialize_class_field(value)


def _serialize_health_check(hc: HealthCheck) -> dict[str, Any]:
    """
    Special serializer for HealthCheck.

    Use serieux serializer with TaggedSubclass
    to be sure check class is saved in model
    """
    data = serialize(TaggedSubclass[HealthCheck], hc)
    _serialize_class_field(data)
    return data


def _validate_health_check(v: Any) -> HealthCheck:
    """Special deserializer for HealthCheck"""
    if isinstance(v, HealthCheck):
        return v
    assert isinstance(v, dict)
    _deserialize_class_field(v)
    return deserialize(TaggedSubclass[HealthCheck], v)


HealthCheckPydantic = Annotated[
    HealthCheck,
    PlainSerializer(_serialize_health_check, return_type=dict),
    BeforeValidator(_validate_health_check),
    # Add explicit schema for HealthCheck dataclass, using serieux schema compiler.
    # Necessary to make sure HealthCheck schema is included into OpenAPI JSON schema.
    WithJsonSchema(schema(HealthCheck).compile(ref_policy=RefPolicy.NEVER)),
]


def _serialize_check_result(result: CheckResult) -> dict[str, Any]:
    """
    Special serializer for CheckResult

    Since we already save associated HealthCheck object,
    we prevent recursion and save space by cleaning `result.check`.
    """
    result.check = None
    data = serialize(TaggedSubclass[CheckResult], result)
    _serialize_class_field(data)
    return data


def _validate_check_result(v: Any) -> CheckResult:
    """Special deserializer for CheckResult"""
    if isinstance(v, CheckResult):
        return v
    assert isinstance(v, dict)
    _deserialize_class_field(v)
    return deserialize(TaggedSubclass[CheckResult], v)


CheckResultPydantic = Annotated[
    CheckResult,
    PlainSerializer(_serialize_check_result, return_type=dict),
    BeforeValidator(_validate_check_result),
    # Add explicit schema for CheckResult dataclass, using serieux schema compiler.
    # Necessary to make sure CheckResult schema is included into OpenAPI JSON schema.
    WithJsonSchema(schema(CheckResult).compile(ref_policy=RefPolicy.NEVER)),
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

    def get_states(self) -> Iterable[HealthCheckState]:
        """Get an iterable of all states saved in database."""
        return self.find_by({}, sort=[("check.name", pymongo.ASCENDING)])


def get_healthcheck_state_collection() -> HealthCheckStateRepository:
    """Return the health check state collection in the current MongoDB."""
    db = config().mongo.database_instance
    return HealthCheckStateRepository(db)
