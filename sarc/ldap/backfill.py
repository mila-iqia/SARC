from collections import defaultdict
from copy import deepcopy
from datetime import datetime

from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection

from sarc.config import config
from sarc.ldap.mymila import fetch_mymila
from sarc.ldap.revision import END_DATE_KEY, START_DATE_KEY


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
            InsertOne(
                {
                    "mila_ldap": {
                        "mila_email_username": user,
                    },
                    "record_start": entry[START_DATE_KEY],
                    "record_end": entry[END_DATE_KEY],
                }
            )
        )

    return updates


def sync_entries(entry, entry_db) -> list:
    if entry != entry_db:
        return [
            UpdateOne(
                {"_id": entry_db["_id"]},
                {
                    # ...
                },
            )
        ]
    return []


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

        assert (
            start_match == end_match
        ), "Either both match or None, else that would be a headache"
        return start_match and end_match

    missing_entries = []
    matched_entries = defaultdict(int)
    matched = []

    for entry in history:
        for entry_db in history_db:
            if entry_match(entry, entry_db):
                matched_entries[id(entry_db)] += 1
                matched.append((entry, entry_db))
                break
        else:
            missing_entries.append(entry)

    for values in matched_entries.values():
        if values > 1:
            raise RuntimeError("Multiple records matched the same period")

    for match in matched:
        updates.extend(sync_entries(*match))

    updates = []
    for missing in missing_entries:
        updates.append(
            InsertOne(
                {
                    "mila_ldap": {
                        "mila_email_username": user,
                    },
                    "record_start": missing[START_DATE_KEY],
                    "record_end": missing[END_DATE_KEY],
                }
            )
        )

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


def _user_record_backfill(cfg, user_collection):
    """No global version for simpler testing"""
    from sarc.ldap.read_mila_ldap import fetch_ldap

    mymila_data = fetch_mymila(cfg, [])

    return user_history_backfill(user_collection, mymila_data)


def user_record_backfill():
    cfg = config()

    user_collection = cfg.mongo.database_instance[cfg.ldap.mongo_collection_name]

    _user_record_backfill(cfg, user_collection)
