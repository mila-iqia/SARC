from collections import namedtuple
from copy import deepcopy
from dataclasses import dataclass

import pandas as pd
from sarc_mocks import fake_mymila_data

import sarc.ldap.mymila
from sarc.config import config
from sarc.ldap.backfill import _user_record_backfill


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
        print(data.columns)
        return data

    monkeypatch.setattr(sarc.ldap.mymila, "query_mymila", wrapper)


@dataclass
class FakeConfig:
    mymila = None


def test_(monkeypatch):
    collection = MockCollection()
    mymiladata(monkeypatch, fake_mymila_data())

    updates, latest = _user_record_backfill(FakeConfig(), collection)

    print(len(latest))
    print(updates)
