"""
Implements the API as it was defined in
https://mila-iqia.atlassian.net/wiki/spaces/IDT/pages/2190737548/Planification

"""
from typing import Optional

from sarc.config import BaseModel, config


class Credentials(BaseModel):
    username: str
    email: str
    active: bool


class User(BaseModel):
    name: str

    mila: Credentials
    drac: Optional[Credentials]

    mila_ldap: dict
    drac_members: Optional[dict]
    drac_roles: Optional[dict]


def get_user(
    mila_email_username=None, mila_cluster_username=None, drac_account_username=None
):
    # note that this `cfg = config()` is cached inside the `config` call
    # so we don't need to caching it here to avoid
    # paying a price each time we call `get_user`
    cfg = config()

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

    L = list(
        cfg.mongo.database_instance[cfg.ldap.mongo_collection_name].find(
            query, {"_id": False}
        )
    )
    assert len(L) <= 1
    if len(L) == 1:
        return User(**L[0])
    else:
        return None


def get_users():
    cfg = config()
    query = {}
    return [
        User(**u)
        for u in cfg.mongo.database_instance[cfg.ldap.mongo_collection_name].find(
            query, {"_id": False}
        )
    ]
