"""
Implements the API as it was defined in
https://mila-iqia.atlassian.net/wiki/spaces/IDT/pages/2190737548/Planification

"""

from __future__ import annotations

from datetime import datetime

from pydantic_mongo import AbstractRepository, PydanticObjectId

from sarc.config import config
from sarc.model import BaseModel
from sarc.users.revision import query_latest_records


class Credentials(BaseModel):
    username: str
    email: str
    active: bool


class User(BaseModel):
    id: PydanticObjectId | None = None

    name: str

    mila: Credentials
    drac: Credentials | None = None

    teacher_delegations: list[str] | None = None

    mila_ldap: dict
    drac_members: dict | None = None
    drac_roles: dict | None = None

    record_start: datetime | None = None
    record_end: datetime | None = None


class _UserRepository(AbstractRepository[User]):
    class Meta:
        collection_name = "users"

    # The API created by pydantic is too simplistic
    # inserting into the users collection need to
    # take into account revisions
    # use: revision.update_user


def _users_collection() -> _UserRepository:
    """Return the jobs collection in the current MongoDB."""
    db = config().mongo.database_instance
    return _UserRepository(database=db)


def get_users(
    query: dict | None = None, query_options: dict | None = None, latest: bool = True
) -> list[User]:
    if query_options is None:
        query_options = {}

    if query is None:
        query = {}

    if latest:
        query = {
            "$and": [
                query_latest_records(),
                query,
            ]
        }

    results = _users_collection().find_by(query, **query_options)

    return list(results)


def get_user(
    mila_email_username: str | None = None,
    mila_cluster_username: str | None = None,
    drac_account_username: str | None = None,
) -> User | None:
    if mila_email_username is not None:
        query = {
            "$and": [
                query_latest_records(),
                {"mila_ldap.mila_email_username": mila_email_username},
            ]
        }
    elif mila_cluster_username is not None:
        query = {
            "$and": [
                query_latest_records(),
                {"mila_ldap.mila_cluster_username": mila_cluster_username},
            ]
        }
    elif drac_account_username is not None:
        query = {
            "$and": [
                query_latest_records(),
                {
                    "$or": [
                        {"drac_roles.username": drac_account_username},
                        {"drac_members.username": drac_account_username},
                    ]
                },
            ]
        }
    else:
        raise ValueError("At least one of the arguments must be provided.")

    users = get_users(query)

    assert len(users) <= 1
    if len(users) == 1:
        return users[0]
    else:
        return None
