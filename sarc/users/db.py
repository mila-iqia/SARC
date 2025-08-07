"""
This script is basically a wrapper around the "read_mila_ldap.py" script.
Instead of taking arguments from the command line, it takes them from
the SARC configuration file.

This is possible because the "read_mila_ldap.py" script has a `run` function
that takes the arguments as parameters, so the argparse step comes earlier.

As a result of running this script, the values in the collection
referenced by "cfg.ldap.mongo_collection_name" will be updated.
"""

import logging

from pydantic import UUID4
from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.config import config
from sarc.core.models.users import UserData
from sarc.core.models.validators import DateOverlapError
from sarc.core.scraping.users import MatchID, UserMatch

logger = logging.getLogger(__name__)


class UserDB(UserData):
    """
    Data for one user
    """

    # Database ID
    id: PydanticObjectId | None = None


class UserRepository(AbstractRepository[UserDB]):
    class Meta:
        collection_name = "users"

    def update_user(self, user: UserMatch) -> None:
        results = list(
            self.find_by({"matching_id": {user.matching_id.name: user.matching_id.mid}})
        )
        if len(results) == 0:
            return self._insert_new(user)
        elif len(results) == 1:
            return self._merge_and_update(results[0], user)
        else:
            db_merged = results[0]
            for db_user_extra in results[1:]:
                db_merged = self._combine_users(db_merged, db_user_extra)
                # Even if the merge fails for some attributes, we have the data
                # to recover missing info in the cache files.
                self.delete_by_id(db_user_extra.id)
            return self._merge_and_update(db_merged, user)

    def _lookup_matching_id(self, mid: MatchID) -> UserDB | None:
        result = list(self.find_by({"matching_id": {mid.name: mid.mid}}))
        if len(result) == 0:
            return None
        elif len(result) > 1:
            logger.error(
                "Multiple matching users in DB for match id (%s), selecting the first one",
                mid,
            )
        return result[0]

    def _update_supervisors(self, db_user: UserDB, user: UserMatch) -> None:
        for val in user.supervisor.values:
            db_val = self._lookup_matching_id(val.value)
            if db_val is None:
                logger.warning(
                    "supervisor (%s) not in db for user %s", val.value, db_user.uuid
                )
            else:
                db_user.supervisor.insert(
                    db_val.uuid, start=val.valid_start, end=val.valid_end
                )
        for cval in user.co_supervisors.values:
            db_set: set[UUID4] = set()
            for cmid in cval.value:
                db_val = self._lookup_matching_id(cmid)
                if db_val is None:
                    logger.warning(
                        "co_supervisor (%s) not in db for user %s", cmid, db_user.uuid
                    )
                else:
                    db_set.add(db_val.uuid)
            db_user.co_supervisors.insert(
                db_set, start=cval.valid_start, end=cval.valid_end
            )

    def _insert_new(self, user: UserMatch) -> None:
        if user.display_name is None or user.email is None:
            logger.error(
                "Attempting to add a new user with missing attributes: %s", user
            )
            return
        db_user = UserDB(
            display_name=user.display_name,
            email=user.email,
            matching_ids={},
            member_type=user.member_type,
            associated_accounts=user.associated_accounts,
            github_username=user.github_username,
            google_scholar_profile=user.google_scholar_profile,
        )
        for mid in user.known_matches:
            db_user.matching_ids[mid.name] = mid.mid
        db_user.matching_ids[user.matching_id.name] = user.matching_id.mid
        self._update_supervisors(db_user, user)
        self.save(db_user)

    def _merge_and_update(self, db_user: UserDB, user: UserMatch) -> None:
        if user.display_name is not None:
            db_user.display_name = user.display_name
        if user.email is not None:
            db_user.email = user.email
        try:
            db_user.member_type.merge_with(user.member_type)
        except DateOverlapError as e:
            logger.error(
                "Cant update member_type for user %s, date overlap error: %s",
                db_user.uuid,
                e,
            )
        try:
            db_user.github_username.merge_with(user.github_username)
        except DateOverlapError as e:
            logger.error(
                "Cant update github_username for user %s, date overlap error: %s",
                db_user.uuid,
                e,
            )
        try:
            db_user.google_scholar_profile.merge_with(user.google_scholar_profile)
        except DateOverlapError as e:
            logger.error(
                "Cant update google_scholar_profile for user %s, date overlap error: %s",
                db_user.uuid,
                e,
            )
        for name, creds in user.associated_accounts.items():
            if name in db_user.associated_accounts:
                db_user.associated_accounts[name].merge_with(creds)
            else:
                db_user.associated_accounts[name] = creds
        self._update_supervisors(db_user, user)
        for mid in user.known_matches:
            if mid.name not in db_user.matching_ids:
                db_user.matching_ids[mid.name] = mid.mid
            elif db_user.matching_ids[mid.name] != mid.mid:
                logger.error(
                    "User %s has matching id (%s:%s) but update has %s",
                    db_user.uuid,
                    mid.name,
                    db_user.matching_ids[mid.name],
                    mid,
                )
        self.save(db_user)

    def _combine_users(self, db_user1: UserDB, db_user2: UserDB) -> UserDB:
        # Merge db_user2 into db_user1

        if db_user2.display_name != db_user1.display_name:
            logger.warning(
                "Merging user %s into user %s and their display_name differs (%s vs %s), ignoring",
                db_user2.uuid,
                db_user1.uuid,
                db_user2.display_name,
                db_user1.display_name,
            )
        # we ignore email as it's of no consequence.

        try:
            db_user1.member_type.merge_with(db_user2.member_type)
        except DateOverlapError as e:
            logger.error(
                "Cant update member_type for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        try:
            db_user1.github_username.merge_with(db_user2.github_username)
        except DateOverlapError as e:
            logger.error(
                "Cant update github_username for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        try:
            db_user1.google_scholar_profile.merge_with(db_user2.google_scholar_profile)
        except DateOverlapError as e:
            logger.error(
                "Cant update google_scholar_profile for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        for name, creds in db_user2.associated_accounts.items():
            if name in db_user1.associated_accounts:
                try:
                    db_user1.associated_accounts[name].merge_with(creds)
                except DateOverlapError as e:
                    logger.error(
                        "Cant update credentials for user %s, domain %s, date overlap error: %s",
                        db_user1.uuid,
                        name,
                        e,
                    )
            else:
                db_user1.associated_accounts[name] = creds

        try:
            db_user1.supervisor.merge_with(db_user2.supervisor)
        except DateOverlapError as e:
            logger.error(
                "Cant update supervisor for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        try:
            db_user1.co_supervisors.merge_with(db_user2.co_supervisors)
        except DateOverlapError as e:
            logger.error(
                "Cant update co_supervisors for user %s, date overlap error: %s",
                db_user1.uuid,
                e,
            )

        # Merge matching_ids - prefer values from db_user2 if there's a conflict
        for name, mid in db_user2.matching_ids.items():
            if name not in db_user1.matching_ids:
                db_user1.matching_ids[name] = mid
            elif db_user1.matching_ids[name] != mid:
                logger.warning(
                    "User %s has matching id (%s:%s) but db_user2 has %s, using db_user2 value",
                    db_user1.uuid,
                    name,
                    db_user1.matching_ids[name],
                    mid,
                )
                db_user1.matching_ids[name] = mid

        return db_user1


def get_user_collection() -> UserRepository:
    db = config().mongo.database_instance
    return UserRepository(database=db)


def get_users(
    query: dict | None = None, query_options: dict | None = None
) -> list[UserData]:
    if query_options is None:
        query_options = {}

    if query is None:
        query = {}

    results = get_user_collection().find_by(query, **query_options)

    return list(results)


def get_user(email: str) -> UserData | None:
    return get_user_collection().find_one_by({"email": email})
