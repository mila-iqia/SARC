import logging
from dataclasses import dataclass

from sarc.alerts.common import CheckResult, HealthCheck

logger = logging.getLogger(__name__)


def _find_duplicates(collection, field: str) -> list[dict]:
    """Find documents sharing the same value for given field.

    Returns a list of dicts with keys: _id (the duplicated value),
    count, and uuids (list of UUIDs sharing that value).
    """
    return list(
        collection.aggregate(
            [
                {"$match": {field: {"$ne": ""}}},
                {
                    "$group": {
                        "_id": f"${field}",
                        "count": {"$sum": 1},
                        "uuids": {"$push": "$uuid"},
                    }
                },
                {"$match": {"count": {"$gt": 1}}},
                {"$sort": {"_id": 1}},
            ]
        )
    )


@dataclass
class UsersCollectionCheck(HealthCheck):
    def check(self) -> CheckResult:
        """Check that there are no duplicate users in the collection.

        Users come from different sources and are merged together.
        If the merge is correct, no two users should share the same
        email or display_name.
        """

        from sarc.users.db import get_user_collection

        collection = get_user_collection().get_collection()

        email_duplicates = _find_duplicates(collection, "email")
        name_duplicates = _find_duplicates(collection, "display_name")

        for dup in email_duplicates:
            logger.error(
                f"Duplicate email '{dup['_id']}' "
                f"shared by {dup['count']} users: {dup['uuids']}"
            )
        for dup in name_duplicates:
            logger.error(
                f"Duplicate display_name '{dup['_id']}' "
                f"shared by {dup['count']} users: {dup['uuids']}"
            )

        if email_duplicates or name_duplicates:
            return self.fail()
        return self.ok()
