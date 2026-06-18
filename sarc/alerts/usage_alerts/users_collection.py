import logging
from dataclasses import dataclass

import sqlmodel

from sarc.alerts.common import CheckResult, HealthCheck

logger = logging.getLogger(__name__)


@dataclass
class UsersCollectionCheck(HealthCheck):
    def check(self) -> CheckResult:
        """Check that there are no duplicate users in the collection.

        Users come from different sources and are merged together.
        If the merge is correct, no two users should share the same
        email or display_name.
        """

        from sarc.config import config
        from sarc.db.users import UserDB

        has_duplicates = False
        with config.db.session() as sess:
            email_duplicates = (
                sqlmodel.select(
                    UserDB.email,
                    sqlmodel.func.count(sqlmodel.col(UserDB.email)),
                    sqlmodel.func.array_agg(UserDB.id),
                )
                .where(UserDB.email != "")
                .group_by(UserDB.email)
                .having(sqlmodel.func.count(sqlmodel.col(UserDB.email)) > 1)
                .order_by(UserDB.email)
            )
            for email, count, user_indices in sess.exec(email_duplicates):
                logger.error(
                    f"Duplicate email '{email}' "
                    f"shared by {count} users: {sorted(user_indices)}"
                )
                has_duplicates = True

            name_duplicates = (
                sqlmodel.select(
                    UserDB.display_name,
                    sqlmodel.func.count(sqlmodel.col(UserDB.display_name)),
                    sqlmodel.func.array_agg(UserDB.id),
                )
                .where(UserDB.display_name != "")
                .group_by(UserDB.display_name)
                .having(sqlmodel.func.count(sqlmodel.col(UserDB.display_name)) > 1)
                .order_by(UserDB.display_name)
            )
            for display_name, count, user_indices in sess.exec(name_duplicates):
                logger.error(
                    f"Duplicate display_name '{display_name}' "
                    f"shared by {count} users: {sorted(user_indices)}"
                )
                has_duplicates = True

        return self.fail() if has_duplicates else self.ok()
