from collections import namedtuple
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd
import pytest
from opentelemetry.trace import StatusCode
from pymongo import InsertOne
from sarc_mocks import dictset, fake_mymila_data, mymila_template

from sarc.ldap.backfill import _user_record_backfill


def userhistory(email, history):
    """Helper to build a record history for a given user"""
    base_user = dictset(
        mymila_template,
        {
            "Status": "Active",
            "Last Name": "last_name",
            "Preferred First Name": "first_name",
            "MILA Email": email,
            "Supervisor Principal": "whatever",
            "Co-Supervisor": "co_whatever",
        },
    )

    records = []
    prev_fields = None
    latest_record = base_user

    for start, fields in history:
        if prev_fields is not None:
            prev_fields["End Date with MILA"] = start
            latest_record = dictset(latest_record, prev_fields)
            records.append(latest_record)

        fields["Start Date with MILA"] = start
        prev_fields = fields

    if prev_fields is not None:
        prev_fields["End Date with MILA"] = None
        records.append(dictset(latest_record, prev_fields))

    return records


def user_with_history():
    return pd.DataFrame(
        userhistory(
            "user123",
            [
                (date(year=2000, month=1, day=1), {"Program of study": "Bac"}),
                (date(year=2003, month=1, day=1), {"Program of study": "Msc"}),
                (date(year=2005, month=1, day=1), {"Program of study": "Phd"}),
                (date(year=2008, month=1, day=1), {"Status": "Inactive"}),  # <= latests
            ],
        )
    )


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


@pytest.fixture
def mymiladata(patch_return_values):
    def patch(data):
        patch_return_values(
            {
                "sarc.ldap.mymila.query_mymila_csv": data,
            }
        )

    return patch


@dataclass
class FakeConfig:
    mymila = None


def test_no_backfill(mymiladata):
    collection = MockCollection()
    mymiladata(fake_mymila_data())

    updates, latest = _user_record_backfill(FakeConfig(), collection)

    assert len(latest) == 10
    assert updates == []


def test_backfill_simple_insert_history(mymiladata, captrace):
    collection = MockCollection()
    mymiladata(user_with_history())

    updates, latest = _user_record_backfill(FakeConfig(), collection)

    assert len(latest) == 1, "Should have a single user"
    assert (
        len(updates) == 3
    ), "Single user should have 3 previous records to be inserted"

    assert updates[0]._doc["record_start"] == datetime(2000, 1, 1)
    assert updates[0]._doc["record_end"] == updates[1]._doc["record_start"]
    assert updates[1]._doc["record_end"] == updates[2]._doc["record_start"]
    assert updates[2]._doc["record_end"] == datetime(2008, 1, 1)

    # Check trace
    spans = captrace.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "_user_record_backfill"
    assert spans[0].status.status_code == StatusCode.OK
    assert len(spans[0].events) == 1
    assert spans[0].events[0].name == "Backfilling record history from mymila ..."


Timestamp = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")


def mongo_db_expected_history():
    return [
        {
            "name": "first_name last_name",
            "mila_ldap": {
                "mila_email_username": "user123",
                "display_name": "first_name last_name",
                "supervisor": "whatever",
                "co_supervisor": "co_whatever",
                "status": "Active",
            },
            "record_start": Timestamp("2000-01-01 00:00:00"),
            "record_end": Timestamp("2003-01-01 00:00:00"),
        },
        {
            "name": "first_name last_name",
            "mila_ldap": {
                "mila_email_username": "user123",
                "display_name": "first_name last_name",
                "supervisor": "whatever",
                "co_supervisor": "co_whatever",
                "status": "Active",
            },
            "record_start": Timestamp("2003-01-01 00:00:00"),
            "record_end": Timestamp("2005-01-01 00:00:00"),
        },
        {
            "name": "first_name last_name",
            "mila_ldap": {
                "mila_email_username": "user123",
                "display_name": "first_name last_name",
                "supervisor": "whatever",
                "co_supervisor": "co_whatever",
                "status": "Active",
            },
            "record_start": Timestamp("2005-01-01 00:00:00"),
            "record_end": Timestamp("2008-01-01 00:00:00"),
        },
        {
            "name": "first_name last_name",
            "mila_ldap": {
                "mila_email_username": "user123",
                "display_name": "first_name last_name",
                "supervisor": "whatever",
                "co_supervisor": "co_whatever",
                "status": "Inactive",
            },
            "record_start": Timestamp("2008-01-01 00:00:00"),
            "record_end": None,
        },
    ]


def test_backfill_history_match(mymiladata):
    collection = MockCollection(mongo_db_expected_history())
    mymiladata(user_with_history())

    updates, latest = _user_record_backfill(FakeConfig(), collection)

    assert len(latest) == 1, "Should have a single user"
    assert updates == [], "Should not update anything"


@pytest.mark.parametrize("missing_idx", [0, 1, 2])
def test_backfill_insert_missing_entries(mymiladata, missing_idx):
    dbstate = mongo_db_expected_history()

    partial_history = dbstate[:missing_idx] + dbstate[missing_idx + 1 :]

    collection = MockCollection(partial_history)
    mymiladata(user_with_history())

    updates, latest = _user_record_backfill(FakeConfig(), collection)

    assert len(latest) == 1, "Should have a single user"
    assert len(updates) == 1, "Should have one inserrt"

    missing_entry = dbstate[missing_idx]
    assert isinstance(updates[0], InsertOne)

    doc = updates[0]._doc

    record_end = doc.pop("record_end")
    record_start = doc.pop("record_start")

    def todate(x):
        return x.to_pydatetime()

    assert todate(record_end) == missing_entry.pop("record_end")
    assert todate(record_start) == missing_entry.pop("record_start")

    assert doc == missing_entry


def test_backfill_sync_history_diff(mymiladata):
    """History match but the entries are different"""

    bad_mongo_history = mongo_db_expected_history()

    bad_entry = bad_mongo_history[1]
    bad_entry["mila_ldap"]["display_name"] = "Not my favorite name"

    collection = MockCollection(bad_mongo_history)
    mymiladata(user_with_history())

    updates, latest = _user_record_backfill(FakeConfig(), collection)

    assert len(latest) == 1, "Should have a single user"
    assert len(updates) == 1, "Should have one update"
    assert (
        updates[0]._doc["mila_ldap"]["display_name"] == "first_name last_name"
    ), "Should have the right display name"


def test_backfill_fail(mymiladata):
    dbstate = mongo_db_expected_history()

    # remove one entry, and close the gap between the records
    partial_history = dbstate[:1] + dbstate[2:]
    partial_history[0]["record_end"] = partial_history[1]["record_start"]

    collection = MockCollection(partial_history)
    mymiladata(user_with_history())

    with pytest.raises(AssertionError):
        _user_record_backfill(FakeConfig(), collection)
