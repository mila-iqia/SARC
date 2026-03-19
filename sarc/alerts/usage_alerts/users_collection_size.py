import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.core.models.validators import datetime_utc

logger = logging.getLogger(__name__)


@dataclass
class CollectionSizeCheckResult(CheckResult):
    latest_checked: datetime_utc | None = None
    latest_size: int = 0


@dataclass
class UsersCollectionSizeCheck(HealthCheck):
    __result_class__ = CollectionSizeCheckResult

    def check(self) -> CheckResult:
        """
        Check if number of users in database unexpectedly changes.

        Number of users may change after a users parsing (`sarc parse users`),
        but should not change neither between two parsing, nor after any other SARC command.
        """

        from sarc.alerts.healthcheck_state import get_healthcheck_state_collection
        from sarc.config import config
        from sarc.core.models.runstate import get_parsed_date
        from sarc.users.db import get_user_collection

        try:
            latest_parsed_date = get_parsed_date(
                config().mongo.database_instance, "users"
            )
        except Exception as exc:
            logger.debug(f"No latest parsed date for users: {type(exc)}: {exc}")
            latest_parsed_date = None

        previous_state = get_healthcheck_state_collection().get_state(self.name)
        current_date = datetime.now(UTC)
        current_size = get_user_collection().get_collection().count_documents({})
        if (
            previous_state is None
            or previous_state.last_result is None
            or cast(
                CollectionSizeCheckResult, previous_state.last_result
            ).latest_checked
            is None
        ):
            logger.info(
                f"First check for user collection: at {current_date}, size: {current_size}"
            )
        else:
            latest_result = cast(CollectionSizeCheckResult, previous_state.last_result)

            latest_checked = latest_result.latest_checked
            latest_size = latest_result.latest_size
            assert latest_checked is not None
            if latest_parsed_date is not None and latest_parsed_date > latest_checked:
                logger.info(
                    "Users parsed after latest checking, comparison might be irrelevant"
                )
            elif latest_size != current_size:
                event_name = "increased" if latest_size < current_size else "decreased"
                logger.error(
                    f"Nb. users {event_name} ({latest_size} -> {current_size}) "
                    f"since latest check at: {latest_checked}"
                )
                return self.fail(latest_checked=current_date, latest_size=current_size)
        return self.ok(latest_checked=current_date, latest_size=current_size)
