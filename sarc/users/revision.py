"""This module handles the creation of revision for users

The revison works as follow:

- every document has a start & end date field
- every documents are inserted with a start date and NO end date
    - the start date defaults to today if not set (usually by mymila)

- All current documents have NO end date
    - this makes query simple as we can just look for missing end date,
      because previous db had no revision system none of the documents have
      end dates, so all their documents are the current ones 

- All past version have an end date
"""

from copy import deepcopy
from datetime import datetime

from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection

DEFAULT_DATE = datetime.utcfromtimestamp(0)


def is_date_missing(date):
    return date is None or date == DEFAULT_DATE


def has_changed(
    user_db,
    user_latest,
    excluded=(
        "_id",
        "record_start",
        "record_end",
    ),
):
    keys = set(list(user_db.keys()) + list(user_latest.keys()))

    for k in keys:
        if k not in excluded:
            v1 = user_db.get(k)
            v2 = user_latest.get(k)

            if v1 != v2:
                return True

    return False


def guess_date(date):
    # If the end_date is unknown (default or None) we put it at today
    if is_date_missing(date):
        return datetime.utcnow()
    return date


def close_record(user_db: dict, end_date=None):
    # usually, end_date will be the record_start of the new record
    # because record_end is a new field its date might not always be accurate
    #
    # enforce that end_date is always present on a closed record
    end_date = guess_date(end_date)

    return UpdateOne(
        {"_id": user_db["_id"]},
        {
            "$set": {
                "record_end": end_date,
            }
        },
    )


def update_user(collection: Collection, user: dict):
    username = user.get("mila_ldap", {}).get("mila_email_username")

    if username is None:
        raise RuntimeError("mila_ldap.mila_email_username is none")

    userdb = list(
        collection.find(
            {
                "$and": [
                    query_latest_records(),
                    {"mila_ldap.mila_email_username": username},
                ]
            }
        )
    )

    if len(userdb) == 0:
        return collection.bulk_write([user_insert(user)])
    else:
        userdb = list(userdb)[0]

        if has_changed(userdb, user):
            return collection.bulk_write(
                [
                    close_record(userdb, end_date=user.get("record_start")),
                    user_insert(user),
                ]
            )

    return 0


def user_insert(newuser: dict) -> list:
    expected_keys = (
        "mila_ldap",
        "name",
        "drac",
        "mila",
        "drac_roles",
        "drac_members",
    )

    update = {
        # enforce that a start_date is always there
        "record_start": guess_date(newuser.get("record_start")),
        # latest record NEVER have an end date
        # this so we can query latest record easily
        "record_end": None,
    }

    for key in expected_keys:
        if key in newuser:
            update[key] = newuser[key]

    return InsertOne(update)


def user_disapeared(user_db):
    # this is an archived user and is already saved as such in the DB
    if user_db["mila_ldap"]["status"] == "archived":
        return []

    # status is not archived but the user does not exist
    newuser = deepcopy(user_db)
    newuser.pop("_id")
    newuser["mila_ldap"]["status"] = "archived"

    return [
        close_record(user_db, end_date=None),
        user_insert(newuser),
    ]


def compute_update(username: str, user_db: dict, user_latest: dict) -> list:
    if user_latest is None:
        return user_disapeared(user_db)

    assert user_latest["mila_ldap"]["mila_email_username"] == username

    # new user
    if user_db is None:
        return [user_insert(user_latest)]

    # no change
    if not has_changed(user_db, user_latest):
        return []

    return [
        close_record(user_db, end_date=user_latest.get("record_start")),
        user_insert(user_latest),
    ]


def query_latest_records() -> dict:
    """Condition latest records need to follow"""
    return {"$or": [{"record_end": {"$exists": False}}, {"record_end": None}]}


def get_all_users(users_collection):
    """returns all the users latest record"""
    query = query_latest_records()

    results = users_collection.find(query)

    return list(results)


def commit_matches_to_database(users_collection, DD_persons_matched, verbose=False):
    users_db = get_all_users(users_collection)
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
    result = 0
    if L_updates_to_do:
        result = users_collection.bulk_write(L_updates_to_do)  #  <- the actual commit
        if verbose:
            print(result.bulk_api_result)
    else:
        if verbose:
            print("Nothing to do.")

    # might as well return this result in case we'd like to write tests for it
    return result
