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

from collections import defaultdict
from copy import deepcopy
from datetime import datetime

from pymongo import InsertOne, UpdateOne

DEFAULT_DATE = datetime.utcfromtimestamp(0)


def is_date_missing(date):
    return date is None or date == DEFAULT_DATE


def has_changed(user_db, user_latest, excluded=("_id",)):
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
                "record_end": end_date,
            }
        },
    )


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
        # enforce that a record_start is always there
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
    # this is an archived user
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
    return {"$or": [{"end_date": {"$exists": False}}, {"end_date": None}]}


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


START_DATE_KEY = "Start Date with MILA"
END_DATE_KEY = "End Date with MILA"


def _check_timeline_consistency(history):
    start = None
    end = None

    for entry in history:
        new_start = entry.get(START_DATE_KEY)
        new_end = entry.get(END_DATE_KEY)

        if new_start is not None:
            if start is not None:
                assert new_start > start

            if end is not None:
                assert new_start >= end

        if new_end is not None:
            if end is not None:
                assert new_end > end

            if new_start is not None:
                assert new_end > new_start

        start = new_start
        end = new_end


def insert_history(user, original_history):
    updates = []
    
    # ignore latest entry
    # it will be handled by the regular update
    history = original_history[:-1]

    # Insert the old entries has past records
    for entry in history:
        updates.insert(
            InsertOne({
                "mila_ldap": {
                    "mila_email_username": user,
                },
                "record_start": entry[START_DATE_KEY],
                "record_end": entry[END_DATE_KEY]
            })
        )

    return updates


def sync_history_diff(user, original_history, original_history_db):
    # ignore latest entry
    # it will be handled by the regular update
    history_db = original_history_db[:-1]
    history = original_history[:-1]
    
    def entry_match(entry, entry_db):
        # criterion for the entries to match
        start = entry_db["record_start"]
        end = entry_db["record_end"]
        
        # We are working on past entries, we should know all of those
        assert start is not None
        assert end is not None
        assert entry[END_DATE_KEY] is not None
        assert entry[START_DATE_KEY] is not None
        
        start_match = entry[START_DATE_KEY] == start 
        end_match = entry[END_DATE_KEY] == end
        
        assert start_match == end_match, "Either both match or None, else that would be a headache"
        return start_match and end_match
    
    missing_entries = []
    matched_entries = defaultdict(int)

    for entry in history:
        for entry_db in history_db:
            if entry_match(entry, entry_db):
                matched_entries[id(entry_db)] += 1
                break
        else:
            missing_entries.append(entry)

    for values in matched_entries.values():
        if values > 1:
            raise RuntimeError("Multiple records matched the same period")

    updates = []
    for missing in missing_entries:
        updates.append(InsertOne({
            "mila_ldap": {
                "mila_email_username": user,
            },
            "record_start": missing[START_DATE_KEY],
            "record_end": missing[END_DATE_KEY]
        }))
        
    return updates


def user_history_diff(users_collection, userhistory: dict[str, list[dict]]):
    dbusers = users_collection.find({})
    userhistory_db = defaultdict(list)
    updates = []

    # Group user by their emails
    for user in dbusers:
        index = user["mila_ldap"]["mila_email_username"]
        userhistory_db[index].append(user)

    for user, history_db in userhistory_db.items():
        history_db.sort(key=lambda item: item["record_start"])

    users = set(list(userhistory.keys()) + list(userhistory_db.keys()))

    for user in users:
        history = userhistory.get(user, [])
        history_db = userhistory_db.get(user, [])

        assert len(history) > 1, "One entry means no history to speak of"

        # No history found insert the one we have
        if len(history_db) <= 1:
            updates.extend(insert_history(user, history))

        # make sure the history match
        else:
            updates.extend(sync_history_diff(user, history, history_db))

    return updates


def user_history_backfill(users_collection, LD_users, backfill=False):
    userhistory = defaultdict(list)
    updates = None

    # Group user by their emails
    for user in LD_users:
        index = user["mila_email_username"]
        userhistory[index].append(user)

    # make sure the history is clean
    for user, history in userhistory.items():
        history.sort(key=lambda item: item[START_DATE_KEY])

        _check_timeline_consistency(history)

    # check if the history exists in the db
    if backfill:
        # users that have a single entry will be upated
        # by the regular flow
        user_with_history = dict()
        for user, history in userhistory.items():
            if len(history) > 1:
                user_with_history[user] = history

        updates = user_history_diff(users_collection, user_with_history)

    # latest records
    latest = dict()
    for user, history in userhistory.items():
        latest[user] = history[-1]

    return updates, latest
