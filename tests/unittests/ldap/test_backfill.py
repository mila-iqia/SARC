from collections import namedtuple
from copy import deepcopy
from dataclasses import dataclass
from datetime import date

import pandas as pd
from sarc_mocks import dictset, fake_mymila_data, mymila_template

import sarc.ldap.mymila
from sarc.config import config
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
        print(start, fields)

        if prev_fields is not None:
            prev_fields["End date of studies"] = start
            latest_record = dictset(latest_record, prev_fields)
            records.append(latest_record)

        fields["Start date of studies"] = start
        prev_fields = fields

    if prev_fields is not None:
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
                (date(year=2008, month=1, day=1), {"Status": "Inactive"}),
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


def mymiladata(monkeypatch, data):
    def wrapper(*args):
        return data

    monkeypatch.setattr(sarc.ldap.mymila, "query_mymila", wrapper)


@dataclass
class FakeConfig:
    mymila = None


def test_no_backfill(monkeypatch):
    collection = MockCollection()
    mymiladata(monkeypatch, fake_mymila_data())

    updates, latest = _user_record_backfill(FakeConfig(), collection)

    assert len(latest) == 10
    assert updates == []


def test_backfill_history(monkeypatch):
    collection = MockCollection()
    mymiladata(monkeypatch, user_with_history())

    updates, latest = _user_record_backfill(FakeConfig(), collection)

    assert len(latest) == 1, "Should have a single user"
    assert (
        len(updates) == 3
    ), "Single user should have 3 previous records to be inserted"
