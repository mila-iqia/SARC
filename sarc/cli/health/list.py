# ruff: noqa: T201
import logging
from dataclasses import dataclass
from dataclasses import fields as dataclass_fields

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.alerts.healthcheck_state import (
    HealthCheckState,
    get_healthcheck_state_collection,
)

logger = logging.getLogger(__name__)


@dataclass
class HealthListCommand:
    """Show health check states saved in database."""

    def execute(self) -> int:
        repo = get_healthcheck_state_collection()
        nb_states = repo.get_collection().count_documents({})
        logger.info(f"There are {nb_states} health check states saved in database.")
        for state in repo.get_states():
            _pretty_print_state(state)
        return 0


_base_check_fields = {field.name for field in dataclass_fields(HealthCheck)}
_base_result_fields = {field.name for field in dataclass_fields(CheckResult)}


def _pretty_print_state(state: HealthCheckState):
    check = state.check
    cls = type(check)
    cls_name = f"{cls.__module__}:{cls.__qualname__}"
    print(
        f"Check: {check.name} [{'NOT ' if not check.active else ''}active] | {cls_name}"
    )
    if check.parameters:
        print("\tParameters:")
        for key, value in check.parameters.items():
            print(f"\t\t{key}: {value}")
    if check.depends:
        print("\tDepends on:")
        for dependency in check.depends:
            print(f"\t\t{dependency}")
    other_fields = [
        field.name
        for field in dataclass_fields(cls)
        if field.name not in _base_check_fields
    ]
    if other_fields:
        for field_name in other_fields:
            value = getattr(check, field_name)
            value_repr = (
                value if isinstance(value, (str, bool, int, float)) else str(value)
            )
            print(f"\t{field_name}: {value_repr}")

    if state.last_result is not None:
        result = state.last_result
        print(f"Last result: {result.status.name}, at: {result.issue_date}")
        if result.statuses:
            print("\tStatuses:")
            for key, value in result.statuses.items():
                print(f"\t\t{key}: {value}")
        other_fields = [
            field.name
            for field in dataclass_fields(type(result))
            if field.name not in _base_result_fields
        ]
        if other_fields:
            for field_name in other_fields:
                value = getattr(result, field_name)
                value_repr = (
                    value if isinstance(value, (str, bool, int, float)) else str(value)
                )
                print(f"\t{field_name}: {value_repr}")
        if result.exception:
            print(f"\tException: {result.exception.type}")
            if result.exception.trace:
                for frame in result.exception.trace:
                    print(
                        f"\t\tFile {frame.filename!r}, line {frame.line}, in {frame.name}"
                    )
                    print(f"\t\t\t{frame.code}")
            print(f"\t\t{result.exception.type}: {result.exception.message}")

    if state.last_message is not None:
        print(f"Last message: {state.last_message}")

    print()
