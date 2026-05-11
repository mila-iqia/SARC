"""MongoDB model and repository for health check state persistence."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, PlainSerializer, WithJsonSchema
from serieux import RefPolicy, TaggedSubclass, deserialize, schema, serialize

from sarc.alerts.common import CheckResult, HealthCheck


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

    # Check configuration
    # With annotated class for custom serialization
    check: HealthCheckPydantic

    # Last check result (None if never run)
    # With annotated class for custom serialization
    # Contains `status` and `issue_date` (last run time)
    last_result: CheckResultPydantic | None = None

    # Optional summary message (e.g., error description)
    last_message: str | None = None
