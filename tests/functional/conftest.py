from __future__ import annotations

import pytest

from sarc.config import using_config

from .allocations.factory import create_allocations
from .diskusage.factory import create_diskusages
from .jobs.factory import create_jobs


@pytest.fixture
def db_allocations():
    return create_allocations()


@pytest.fixture
def db_jobs():
    return create_jobs()


def custom_db_config(cfg, db_name):
    assert "test" in db_name
    new_cfg = cfg.replace(mongo=cfg.mongo.replace(database=db_name))
    db = new_cfg.mongo.instance
    # Ensure we do not use and thus wipe the production database
    assert db.name == db_name
    return new_cfg


def clear_db(db):
    db.allocations.drop()
    db.jobs.drop()
    db.diskusage.drop()
    db.users.drop()


def fill_db(db):
    db.allocations.insert_many(create_allocations())
    db.jobs.insert_many(create_jobs())
    db.diskusage.insert_many(create_diskusages())


def create_db_configuration_fixture(db_name, empty=False, scope="function"):
    @pytest.fixture(scope=scope)
    def fixture(standard_config_object):
        cfg = custom_db_config(standard_config_object, db_name)
        db = cfg.mongo.instance
        clear_db(db)
        if not empty:
            fill_db(db)
        yield

    return fixture


empty_read_write_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-write-test",
    empty=True,
    scope="function",
)


read_write_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-write-test",
    scope="function",
)


read_only_db_config_object = create_db_configuration_fixture(
    db_name="sarc-read-only-test",
    scope="session",
)


@pytest.fixture
def empty_read_write_db(standard_config, empty_read_write_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-write-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.instance


@pytest.fixture
def read_write_db(standard_config, read_write_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-write-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.instance


@pytest.fixture
def read_only_db(standard_config, read_only_db_config_object):
    cfg = custom_db_config(standard_config, "sarc-read-only-test")
    with using_config(cfg) as cfg:
        yield cfg.mongo.instance
