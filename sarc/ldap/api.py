"""
Implements the API as it was defined in
https://mila-iqia.atlassian.net/wiki/spaces/IDT/pages/2190737548/Planification

"""

from __future__ import annotations

from datetime import date
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
    
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class UserRepository(AbstractRepository[User]):
    class Meta:
        collection_name = "users"
        
    def close_current_record(self, model: User):
        today = date.today()
        return self.get_collection().update_one(
            {
                "_id": model.id,
            },
            {
                "$set": {
                    "end": today
                }
            },
            upsert=True,
        )

    def save_user(self, model: User):
        if model.id is not None:
            self.close_current_record(model)
        
        today = date.today()
        document = self.to_document(model)
        document['start'] = today
        
        return self.get_collection().insert(
            document,
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
         
    pipeline = [
        query,
        {"$sort": {"start_date": 1}},
        #
        #   Group by email, those should be unique
        #   because the group operation replace the _id
        #   we save the original document as well
        {
            "$group": {
                "_id": "mila_ldap.mila_email_username",
                "document": { "$last": "$$ROOT" },
                "start_date": {
                    "$last": "$start_date"
                }
            }
        },
        #
        #  Group is done but the _id was changed
        #  now we replace the new _id (email) by the old _id (Object Id)
        #  that we need to to udpates
        #
        {
            "$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": [ "$document", { "_id": "$_id" } ] 
                }
            }
        }
    ]
    
    return list(users_collection().aggregate(pipeline))


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

    users = get_users(query)
    
    assert len(users) <= 1
    if len(users) == 1:
        return users[0]
    else:
        return None
