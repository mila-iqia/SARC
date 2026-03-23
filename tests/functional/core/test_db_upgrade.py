import pymongo
import pytest

from sarc.core import db_init
from sarc.core.db_init import CURRENT_SCHEMA_VERSION, db_upgrade
from sarc.core.models.runstate import get_parsed_date
from sarc.core.models.validators import START_TIME


def _assert_schema_created(db):
    for collection_name in [
        "jobs",
        "allocations",
        "diskusage",
        "users",
        "clusters",
        "gpu_billing",
        "node_gpu_mapping",
        "healthcheck",
        "runstate",
    ]:
        assert db[collection_name].index_information()

    assert get_parsed_date(db, "jobs") == START_TIME
    assert get_parsed_date(db, "users") == START_TIME

    scraped_time_index = [
        index
        for index in db["jobs"].index_information().values()
        if index["key"]
        == [
            ("cluster_name", pymongo.ASCENDING),
            ("latest_scraped_start", pymongo.ASCENDING),
            ("latest_scraped_end", pymongo.ASCENDING),
        ]
    ]
    assert len(scraped_time_index) == 1


def test_db_upgrade_no_version(empty_read_write_db):
    db = empty_read_write_db

    db_upgrade(db)

    _assert_schema_created(db)
    assert db.version.find_one()["value"] == CURRENT_SCHEMA_VERSION


def test_db_upgrade_older_version(empty_read_write_db):
    db = empty_read_write_db
    db.version.replace_one({}, {"value": CURRENT_SCHEMA_VERSION - 1})

    db_upgrade(db)

    _assert_schema_created(db)
    assert db.version.find_one()["value"] == CURRENT_SCHEMA_VERSION


def test_db_upgrade_same_version(empty_read_write_db, monkeypatch):
    db = empty_read_write_db
    db_upgrade(db)

    def fail(db):
        raise AssertionError("should not call create_clusters")

    monkeypatch.setattr(db_init, "create_clusters", fail)

    db_upgrade(db)


def test_db_upgrade_newer_version(empty_read_write_db):
    db = empty_read_write_db
    db.version.replace_one({}, {"value": CURRENT_SCHEMA_VERSION + 1}, upsert=True)

    with pytest.raises(RuntimeError, match="Database schema is newer than the code"):
        db_upgrade(db)
