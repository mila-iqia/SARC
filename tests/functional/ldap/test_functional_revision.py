from copy import deepcopy
from datetime import datetime

import pymongo
import pytest
from sarc_mocks import dictset

from sarc.ldap.api import get_user, users_collection
from sarc.ldap.revision import commit_matches_to_database, update_user

statuses = ("disabled", "archived", "enabled")


def make_user(status, **kwargs):
    return {
        "mila_ldap": {
            "mila_email_username": "mail",
            "mila_cluster_username": "posixUid",
            "mila_cluster_uid": "uidNumber",
            "mila_cluster_gid": "gidNumber",
            "display_name": "displayName",
            "supervisor": None,
            "co_supervisor": None,
            "status": status,
        },
        **kwargs,
    }


def user_dict(users):
    return {u["mila_ldap"]["mila_email_username"]: u for u in users}


def transitions():
    all_transitions = []

    for start_status in statuses:
        for end_status in statuses:
            all_transitions.append((start_status, end_status))
    return all_transitions


def insert_user_history(username, history):
    users = users_collection().get_collection()

    user_template = {
        "name": username,
        "mila": {
            "username": username,
            "email": "2",
            "active": True,
        },
        "mila_ldap": {
            "mila_email_username": username,
            "supervisor": None,
        },
    }

    for entry in history:
        update_user(users, dictset(user_template, entry))


def user_with_history(username):
    insert_user_history(
        username,
        [
            {"mila_ldap.supervisor": "1", "record_start": datetime(2001, 1, 1)},
            {"mila_ldap.supervisor": "2", "record_start": datetime(2002, 1, 1)},
            {"mila_ldap.supervisor": "3", "record_start": datetime(2003, 1, 1)},
            {"mila_ldap.supervisor": "4", "record_start": datetime(2004, 1, 1)},
        ],
    )


@pytest.mark.usefixtures("write_setup")
def test_get_user_fetch_latest():

    user_with_history("user1")

    #
    # Check latest
    #
    latest_user = get_user(mila_email_username="user1")
    assert latest_user is not None, "Latest user should be found"
    assert latest_user.record_start.year == 2004
    assert latest_user.record_end is None
    assert latest_user.mila_ldap["supervisor"] == "4"

    #
    # Check history
    #
    users = users_collection().get_collection()
    records = list(users.find({}))
    assert len(records) == 4, "History should have been saved"

    # MongoDB should return it sorted implicitly but to be safe
    records.sort(key=lambda item: item["record_start"])
    start = None
    end = None

    for r in records:
        current_start = r["record_start"]
        current_end = r["record_end"]

        if start is None:
            start = current_start
            end = current_end
            continue

        assert end == current_start, "Record shoulve have been closed"
        start = current_start
        end = current_end

    assert end is None, "Latest record should be open"


@pytest.mark.usefixtures("write_setup")
@pytest.mark.parametrize("status", statuses)
def test_update_status_nodb_snapshots(status):
    collection = users_collection().get_collection()
    newusers = user_dict([make_user(status)])
    documents_before = list(collection.find({}))

    # initial insert
    commit_matches_to_database(collection, newusers)

    documents_after = list(collection.find({}))
    n_new = len(documents_after) - len(documents_before)

    assert len(documents_after) == 1, "new record should have been created"
    assert n_new == 1, "User does not exist in DB, record should have been inserted"

    latest = documents_after[0]

    assert latest["record_start"] is not None, "record_start should be set"
    assert latest["mila_ldap"]["status"] == status, "status should match ldap"
    assert latest.get("record_end", None) is None, "record_end should not be set"


@pytest.mark.usefixtures("write_setup")
@pytest.mark.parametrize("status", statuses)
def test_update_status_db_nosnapshots(status):

    collection = users_collection().get_collection()
    update_user(collection, make_user(status))
    users = user_dict([])
    documents_before = list(collection.find({}))

    # initial insert
    commit_matches_to_database(collection, users)

    documents_after = list(collection.find({}))
    n_new = len(documents_after) - len(documents_before)

    if status in ("archived",):
        assert n_new == 0, "User is already archived, should not no updates"
    else:
        assert len(documents_after) == 2, "New document should have been inserted"
        assert n_new == 1, "User is in db, but does not exist in snapshots"

        closed = documents_after[0]
        assert closed["record_start"] is not None
        assert closed["record_end"] is not None

        latest = documents_after[1]
        assert latest["record_start"] is not None
        assert latest["record_end"] is None
        assert latest["mila_ldap"]["status"] == "archived"


@pytest.mark.usefixtures("write_setup")
@pytest.mark.parametrize("start,end", transitions())
def test_update_status_users_exists_on_both(start, end):

    collection = users_collection().get_collection()
    update_user(collection, make_user(start, record_start=datetime(2000, 1, 1)))
    documents_before = list(collection.find({}))

    users = user_dict([make_user(end, record_start=datetime(2000, 1, 1))])

    #
    commit_matches_to_database(collection, users, verbose=True)

    documents_after = list(collection.find({}))
    n_new = len(documents_after) - len(documents_before)

    # nothing
    if start == end:
        assert n_new == 0, "DB and snapshots should match"
    else:
        assert len(documents_after) == 2, "Old and new record"
        assert n_new == 1, "should have inserted a new record"

        closed = documents_after[0]
        assert closed["record_start"] is not None
        assert closed["record_end"] is not None
        assert closed["mila_ldap"]["status"] == start

        latest = documents_after[1]
        assert latest["record_start"] is not None
        assert latest["record_end"] is None
        assert latest["mila_ldap"]["status"] == end
