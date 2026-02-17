import hashlib
from pathlib import Path

import gifnoc
import pytest

from sarc.config import config

here = Path(__file__).parent


@pytest.fixture
def frozen_gifnoc_time():
    with gifnoc.overlay(
        {"time": {"$class": "FrozenTime", "time": "2024-01-01T00:00", "sleep_beat": 0}}
    ):
        yield


@pytest.fixture
def beans_config():
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "beans.yaml"):
        yield config().health_monitor


@pytest.fixture
def deps_config():
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "deps.yaml"):
        yield config().health_monitor


@pytest.fixture
def params_config():
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "params.yaml"):
        yield config().health_monitor


@pytest.fixture
def health_config():
    with gifnoc.overlay(here / "configs" / "diskusage.yaml"):
        yield config().health_monitor


@pytest.fixture(scope="function")
def empty_read_write_db(request):
    m = hashlib.md5()
    m.update(request.node.nodeid.encode())
    db_name = f"test-db-{m.hexdigest()}"
    with gifnoc.overlay({"sarc.mongo.database_name": db_name}):
        assert config().mongo.database_instance.name == db_name
        db = config().mongo.database_instance
        yield db_name
        db.client.drop_database(db_name)


@pytest.fixture(scope="function")
def read_write_db(empty_read_write_db):
    """Read-write db with some data inside"""
    db = config().mongo.database_instance
    db["a_collection"].insert_one(
        {"a_text": ",".join(str(i) for i in range(40 * 1024))}
    )
    yield empty_read_write_db
