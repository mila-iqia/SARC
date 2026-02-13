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
def beans_config(tmpdir):
    setdir = {"sarc": {"health_monitor": {"directory": str(tmpdir)}}}
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "beans.yaml", setdir):
        yield config().health_monitor


@pytest.fixture
def deps_config(tmpdir):
    setdir = {"sarc": {"health_monitor": {"directory": str(tmpdir)}}}
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "deps.yaml", setdir):
        yield config().health_monitor


@pytest.fixture
def params_config(tmpdir):
    setdir = {"sarc": {"health_monitor": {"directory": str(tmpdir)}}}
    cfgdir = here / "configs"
    with gifnoc.overlay(cfgdir / "base.yaml", cfgdir / "params.yaml", setdir):
        yield config().health_monitor


@pytest.fixture(scope="function")
def empty_read_write_db(request):
    m = hashlib.md5()
    m.update(request.node.nodeid.encode())
    db_name = f"test-db-{m.hexdigest()}"
    with gifnoc.overlay({"sarc.mongo.database_name": db_name}):
        assert config().mongo.database_instance.name == db_name
        db = config().mongo.database_instance
        for collection_name in db.list_collection_names():
            db[collection_name].drop()
        yield db_name
