"""
Implements the API as it was defined in
https://mila-iqia.atlassian.net/wiki/spaces/IDT/pages/2190737548/Planification

"""

from __future__ import annotations

from typing import Optional

from pydantic_mongo import AbstractRepository, ObjectIdField

from sarc.config import BaseModel, config


class Credentials(BaseModel):
    username: str
    email: str
    active: bool


class User(BaseModel):
    id: ObjectIdField = None

    name: str

    mila: Credentials
    drac: Optional[Credentials]

    mila_ldap: dict
    drac_members: Optional[dict]
    drac_roles: Optional[dict]


class UserRepository(AbstractRepository[User]):
    class Meta:
        collection_name = "users"

    def save_user(self, model: User):
        document = self.to_document(model)
        return self.get_collection().update_one(
            {
                "_id": model.id,
            },
            {"$set": document},
            upsert=True,
        )


def users_collection():
    """Return the jobs collection in the current MongoDB."""
    db = config().mongo.database_instance
    return UserRepository(database=db)


def get_users(query=None, query_options: dict | None = None):
    if query_options is None:
        query_options = {}

    if query is None:
        query = {}
    return list(users_collection().find_by(query, query_options))


def get_user(
    mila_email_username=None, mila_cluster_username=None, drac_account_username=None
):
    if mila_email_username is not None:
        query = {"mila_ldap.mila_email_username": mila_email_username}
    elif mila_cluster_username is not None:
        query = {"mila_ldap.mila_cluster_username": mila_cluster_username}
    elif drac_account_username is not None:
        query = {
            "$or": [
                {"drac_roles.username": drac_account_username},
                {"drac_members.username": drac_account_username},
            ]
        }
    else:
        raise ValueError("At least one of the arguments must be provided.")

    L = get_users(query)

    assert len(L) <= 1
    if len(L) == 1:
        return L[0]
    else:
        return None
