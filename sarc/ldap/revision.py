import json
import os
import ssl
import warnings
from copy import deepcopy
from datetime import datetime

from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection

from .api import get_users


def make_user_update(collection: Collection, update: dict) -> list:
    dbentry = collection.find_one(
        {"mila_ldap.mila_email_username": update["mila_email_username"]}
    )

    if not has_changed():
        # No change detected
        return []

    today = datetime.today()

    queries = []
    # Close current record
    queries.append(
        UpdateOne(
            {"_id": dbentry["_id"]},
            {
                "$set": {
                    "end_date": today,  # date it was archived
                    "mila_ldap.status": "archived",  # Update status to archive
                }
            },
        )
    )

    # if user not archived
    # insert new records with updated value
    if update["status"] != "archived":
        new_record = deepcopy(dbentry)
        _ = new_record.pop("_id")
        new_record["mila_ldap"] = update
        new_record["start_date"] = update.pop("start_date", today)

        queries.append(InsertOne(new_record))

    return queries


def make_user_updates(collection: Collection, updates: list[dict]) -> list:
    queries = []
    for update in updates:
        queries.extend(make_user_update(collection, update))
    return queries


def make_user_insert(newuser: dict) -> list:
    end_date = None
    start_date = newuser.pop("start_date", datetime.today())

    if newuser["status"] in ("archived",):
        end_date = datetime.today()

    return InsertOne(
        {
            "mila_ldap": newuser["mila_ldap"],
            "name": newuser["name"],
            "mila": newuser["mila"],
            "drac": newuser["drac"],
            "drac_roles": newuser["drac_roles"],
            "drac_members": newuser["drac_members"],
            "start_date": start_date,
            "end_date": end_date,
        }
    )


def fill_computed_fields(data: dict):
    mila_ldap = data.get("mila_ldap", {}) or {}
    drac_members = data.get("drac_members", {}) or {}
    drac_roles = data.get("drac_roles", {}) or {}

    if "name" not in data:
        data["name"] = mila_ldap.get("display_name", "???")

    if "mila" not in data:
        data["mila"] = {
            "username": mila_ldap.get("mila_cluster_username", "???"),
            "email": mila_ldap.get("mila_email_username", "???"),
            "active": mila_ldap.get("status", None) == "enabled",
        }

    if "drac" not in data:
        if drac_members:
            data["drac"] = {
                "username": drac_members.get("username", "???"),
                "email": drac_members.get("email", "???"),
                "active": drac_members.get("activation_status", None) == "activated",
            }
        elif drac_roles:
            data["drac"] = {
                "username": drac_roles.get("username", "???"),
                "email": drac_roles.get("email", "???"),
                "active": drac_roles.get("status", None) == "Activated",
            }
        else:
            data["drac"] = None

    return data


def has_changed(user_db, user_latest):
    return user_db != user_latest


def close_record(user_db: dict, end_date=None):
    if end_date is None:
        end_date = datetime.today()

    return UpdateOne(
        {"_id": user_db["_id"]},
        {
            "$set": {
                "end_date": end_date,  # date it was archived
                "mila_ldap.status": "archived",  # Update status to archive
            }
        },
    )


def compute_update(username: str, user_db: dict, user_latest: dict) -> list:

    user_latest = fill_computed_fields(user_latest)
    assert user_latest["mila_ldap"]["mila_email_username"] == username

    # new user
    if user_db is None:
        return make_user_insert(user_latest)

    # no change
    if not has_changed(user_db, user_latest):
        return []

    # change
    # What if the change is to archive the user ?
    return [
        close_record(user_db, end_date=user_latest["start_date"]),
        make_user_insert(user_latest),
    ]


def commit_matches_to_database(users_collection, DD_persons_matched, verbose=False):
    users_db = get_users()
    user_to_update = []

    # Match db user to their user updates
    for user_db in users_db:
        mila_email_username = user_db["mila_ldap"]["mila_email_username"]

        matched_user = DD_persons_matched.pop(mila_email_username, None)

        user_to_update.append((mila_email_username, user_db, matched_user))

    # Those are new users that were not found in the DB
    for mila_email_username, D_match in DD_persons_matched.items():
        user_to_update.append((mila_email_username, None, D_match))

    # Compute the updates to make
    L_updates_to_do = []
    for username, user_db, user_latest in user_to_update:
        L_updates_to_do.extend(compute_update(username, user_db, user_latest))

    # Final write
    if L_updates_to_do:
        result = users_collection.bulk_write(L_updates_to_do)  #  <- the actual commit
        if verbose:
            print(result.bulk_api_result)
    else:
        if verbose:
            print("Nothing to do.")

    # might as well return this result in case we'd like to write tests for it
    return result
