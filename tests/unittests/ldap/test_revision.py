from collections import namedtuple
from copy import deepcopy
from datetime import datetime

import pytest
from pymongo import InsertOne, UpdateOne

from sarc.ldap.revision import commit_matches_to_database as _save_to_mongo
from sarc.ldap.revision import query_latest_records


class MockCollection:
    def __init__(self, data=None) -> None:
        self._id = 0

        if data is None:
            data = []

        for line in data:
            line["_id"] = self._id
            self._id += 1

        self.data = data
        self.writes = []

    def find(self, query=None):
        assert query == query_latest_records()
        return deepcopy(self.data)

    def find_one(self, query):
        email = query["mila_ldap.mila_email_username"]

        for line in self.data:
            if email == line["mila_ldap"]["mila_email_username"]:
                return deepcopy(line)

        return None

    def bulk_write(self, operations):
        self.writes = operations
        return namedtuple("Result", ["bulk_api_result"])(len(operations))


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


def transitions():
    all_transitions = []

    for start_status in statuses:
        for end_status in statuses:
            all_transitions.append((start_status, end_status))
    return all_transitions


def user_dict(users):
    return {u["mila_ldap"]["mila_email_username"]: u for u in users}


@pytest.mark.parametrize("status", statuses)
def test_ldap_update_status_nodb_ldap(status):
    collection = MockCollection()
    newusers = user_dict([make_user(status)])

    # initial insert
    _save_to_mongo(collection, newusers)

    assert len(collection.writes) == 1, "User does not exist in DB, simple insert"

    written_user = collection.writes[0]._doc
    assert written_user["start_date"] is not None, "start_date was set"
    assert written_user["mila_ldap"]["status"] == status, "status match ldap"
    assert written_user.get("end_date", None) is None, "end_date was not set"


@pytest.mark.parametrize("status", statuses)
def test_ldap_update_status_db_noldap(status):
    collection = MockCollection([make_user(status)])
    ldap_users = user_dict([])

    # initial insert
    _save_to_mongo(collection, ldap_users)

    if status in ("archived",):
        assert len(collection.writes) == 0, "User is already archived, no updates"
    else:
        assert len(collection.writes) == 2, "User is in db, but does not exist in ldap"

        assert isinstance(collection.writes[0], UpdateOne)
        written_user = collection.writes[0]._doc["$set"]

        assert written_user.get("start_date") is None, "start_date was NOT UPDATED"
        assert written_user["end_date"] is not None, "end_date was set"
        assert len(written_user) == 1, "record was left as is"

        assert isinstance(collection.writes[1], InsertOne)
        entry_insert = collection.writes[1]._doc
        assert entry_insert["start_date"] is not None, "start_date was set"
        assert entry_insert.get("end_date") is None, "end_date was NOT set"
        assert (
            entry_insert["mila_ldap"]["status"] == "archived"
        ), "Status was moved to archived"


@pytest.mark.parametrize("start,end", transitions())
def test_ldap_update_status_users_exists_on_both(start, end):

    collection = MockCollection([make_user(start, start_date=datetime(2000, 1, 1))])
    ldap_users = user_dict([make_user(end, start_date=datetime(2000, 1, 1))])

    # initial insert
    _save_to_mongo(collection, ldap_users)

    # nothing
    if start == end:
        assert len(collection.writes) == 0, "DB and LDAP match"
    else:
        assert len(collection.writes) == 2, "DB close record and insert update"

        assert isinstance(collection.writes[0], UpdateOne)
        entry_update = collection.writes[0]._doc["$set"]

        assert entry_update.get("start_date") is None, "start_date was NOT UPDATED"
        assert entry_update["end_date"] is not None, "end_date was set"
        assert len(entry_update) == 1, "record was left as is"

        assert isinstance(collection.writes[1], InsertOne)
        entry_insert = collection.writes[1]._doc
        assert entry_insert["start_date"] is not None, "start_date was set"
        assert entry_insert.get("end_date") is None, "end_date was NOT set"
        assert entry_insert["mila_ldap"]["status"] == end, "Status match ldap"
