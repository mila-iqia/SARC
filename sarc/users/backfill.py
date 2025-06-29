from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime

from pymongo import InsertOne, UpdateOne
from pymongo.collection import Collection

from sarc.cache import CachePolicy
from sarc.config import Config, config
from sarc.traces import using_trace
from sarc.users.mymila import fetch_mymila

START = "mymila_start"
END = "mymila_end"


def _check_timeline_consistency(history: list[dict[str, datetime | None]]) -> None:
    start = None
    end = None

    for entry in history:
        new_start = entry.get(START)
        new_end = entry.get(END)

        if new_start is not None:
            if start is not None:
                assert new_start >= start

            if end is not None:
                assert new_start >= end

        if new_end is not None:
            if end is not None:
                assert new_end > end

            if new_start is not None:
                assert new_end > new_start

        start = new_start
        end = new_end


def user_from_entry(username: str, entry: dict) -> dict:
    return {
        "name": entry["display_name"],
        "mila_ldap": {
            "mila_email_username": username,
            "display_name": entry["display_name"],
            "supervisor": entry["supervisor"],
            "co_supervisor": entry["co_supervisor"],
            "status": entry["status"],
        },
        "record_start": entry[START],
        "record_end": entry[END],
    }


def insert_history(user: str, original_history: list[dict]):
    updates = []

    # ignore latest entry
    # it will be handled by the regular update
    history = original_history[:-1]

    # Insert the old entries has past records
    for entry in history:
        updates.append(InsertOne(user_from_entry(user, entry)))

    return updates


def compute_entry_diff[V](
    entry: dict[str, V],
    entry_db: dict[str, V],
    diff: dict[str, V] | None = None,
    excluded: Iterable[str] = ("_id",),
) -> dict[str, V]:
    keys = set(entry.keys())

    if diff is None:
        diff = {}

    for k in keys:
        if k in excluded:
            continue

        if k not in entry_db:
            diff[k] = entry[k]

        elif entry_db[k] != entry[k]:
            diff[k] = entry[k]

    return diff


def sync_entries[V](user: str, entry: dict[str, V], entry_db: dict[str, V]) -> list:
    diff = compute_entry_diff(user_from_entry(user, entry), entry_db)

    if len(diff) > 0:
        return [
            UpdateOne(
                {"_id": entry_db["_id"]},
                diff,
            )
        ]
    return []


def sync_history_diff(
    user: str, original_history: list[dict], original_history_db: list[dict]
) -> list:
    # ignore latest entry
    # it will be handled by the regular update
    history_db = original_history_db
    if original_history_db[-1]["record_end"] is None:
        history_db = original_history_db[:-1]

    history = original_history
    if original_history[-1]["mymila_end"] is None:
        history = original_history[:-1]

    def entry_match(entry, entry_db):
        # criterion for the entries to match
        start = entry_db["record_start"]
        end = entry_db["record_end"]

        # We are working on past entries, we should know all of those
        assert start is not None
        assert end is not None
        assert entry[END] is not None
        assert entry[START] is not None

        start_match = entry[START] == start
        end_match = entry[END] == end

        assert start_match == end_match, (
            "Either both match or None, else that would be a headache"
        )
        return start_match and end_match

    missing_entries: list[dict] = []
    matched_entries: dict[int, int] = defaultdict(int)
    matched: list[tuple[str, dict, dict]] = []

    for entry in history:
        for entry_db in history_db:
            if entry_match(entry, entry_db):
                matched_entries[id(entry_db)] += 1
                matched.append((user, entry, entry_db))
                break
        else:
            missing_entries.append(entry)

    for values in matched_entries.values():
        if values > 1:
            raise RuntimeError("Multiple records matched the same period")

    updates = []
    for match in matched:
        updates.extend(sync_entries(*match))

    for missing in missing_entries:
        updates.append(InsertOne(user_from_entry(user, missing)))

    return updates


def user_history_diff(
    users_collection: Collection, userhistory: dict[str, list[dict]]
) -> list:
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


def user_history_backfill(
    users_collection: Collection, LD_users: list[dict], backfill: bool = True
) -> tuple[list | None, dict[str, dict]]:
    userhistory: dict[str, list[dict]] = defaultdict(list)
    updates = None

    # Group user by their emails
    for user in LD_users:
        index = user["mila_email_username"]
        userhistory[index].append(user)

    # make sure the history is clean
    for history in userhistory.values():
        history.sort(key=lambda item: item[START])

        _check_timeline_consistency(history)

    # check if the history exists in the db
    if backfill:
        # users that have a single entry will be upated
        # by the regular flow
        user_with_history: dict[str, list[dict]] = {}
        for u, history in userhistory.items():
            if len(history) > 1:
                user_with_history[u] = history

        updates = user_history_diff(users_collection, user_with_history)

    # latest records
    latest: dict[str, dict] = {}
    for u2, history in userhistory.items():
        latest[u2] = history[-1]

    return updates, latest


def _user_record_backfill(
    cfg: Config,
    user_collection: Collection,
    cache_policy: CachePolicy = CachePolicy.use,
) -> tuple[list | None, dict[str, dict]]:
    """No global version for simpler testing"""
    # We do not set expected exceptions, so that any exception will be re-raised by tracing.
    with using_trace(
        "sarc.users.backfill", "_user_record_backfill", exception_types=()
    ) as span:
        span.add_event("Backfilling record history from mymila ...")

        mymila_data = fetch_mymila(cfg, [], cache_policy=cache_policy)

        return user_history_backfill(user_collection, mymila_data)


def user_record_backfill(cache_policy: CachePolicy = CachePolicy.use):
    cfg = config("scraping")

    assert cfg.ldap is not None
    user_collection = cfg.mongo.database_instance[cfg.ldap.mongo_collection_name]

    _user_record_backfill(cfg, user_collection, cache_policy=cache_policy)
