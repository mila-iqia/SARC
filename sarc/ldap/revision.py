"""This module handles the creation of revision for users

The revison works as follow:

- every document has a start & end date field
- every documents are inserted with a start date and NO end date
    - the start date defaults to today if not set (usually by mymila)

- All current documents have NO end date
    - this makes query simple as we kind just look for missing end date,
      because previous db had no revision system none of the documents have
      end dates, so all their documents are the current ones 

- All past version have an end date
"""

from datetime import datetime

from pymongo import InsertOne, UpdateOne

from .api import get_users

DEFAULT_DATE = datetime.datetime.utcfromtimestamp(0)


def is_date_missing(date):
    return date is None or date == DEFAULT_DATE


def has_changed(user_db, user_latest):
    return user_db != user_latest


def guess_date(date):
    # If the end_date is unknown (default or None) we put it at today
    if is_date_missing(date):
        return datetime.today()
    return date


def close_record(user_db: dict, end_date=None):
    # usually, end_date will be the start_date of the new record
    # because start_date is a new field its date might not always be accurate
    #
    # enforce that end_date is always present on a closed record
    end_date = guess_date(end_date)

    return UpdateOne(
        {"_id": user_db["_id"]},
        {
            "$set": {
                "end_date": end_date,
            }
        },
    )


def user_insert(newuser: dict) -> list:
    return InsertOne(
        {
            "mila_ldap": newuser["mila_ldap"],
            "name": newuser["name"],
            "mila": newuser["mila"],
            "drac": newuser["drac"],
            "drac_roles": newuser["drac_roles"],
            "drac_members": newuser["drac_members"],
            # enforce that a start_date is always there
            "start_date": guess_date(newuser.get("start_date")),
            # latest record NEVER have an end date
            # this so we can query latest record easily
            "end_date": None,
        }
    )


def compute_update(username: str, user_db: dict, user_latest: dict) -> list:

    user_latest = fill_computed_fields(user_latest)
    assert user_latest["mila_ldap"]["mila_email_username"] == username

    # new user
    if user_db is None:
        return user_insert(user_latest)

    # no change
    if not has_changed(user_db, user_latest):
        return []

    return [
        close_record(user_db, end_date=user_latest.get("start_date")),
        user_insert(user_latest),
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
