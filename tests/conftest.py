import zoneinfo
from pathlib import Path

import pytest

from sarc.config import (
    ClusterConfig,
    Config,
    MongoConfig,
    config,
    parse_config,
    using_config,
)

pytest_plugins = "fabric.testing.fixtures"


@pytest.fixture(scope="session")
def standard_config_object():
    yield parse_config(Path(__file__).parent / "sarc-test.json")


@pytest.fixture(scope="session", autouse=True)
def use_standard_config(standard_config_object):  # pylint: disable=redefined-outer-name
    with using_config(standard_config_object):
        yield


@pytest.fixture
def tzlocal_is_mtl(monkeypatch):
    monkeypatch.setattr("sarc.config.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))
    monkeypatch.setattr("sarc.jobs.job.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))


@pytest.fixture
def test_config(
    tmp_path,
    request,
):
    current = config()

    vals = getattr(request, "param", dict())

    mongo_url = vals.get("mongo_url", current.mongo.url)
    mongo_database = vals.get("mongo_database", current.mongo.database)
    cluster_name = vals.get("cluster_name", "test")

    def get_cluster_conf(attr, default):
        cluster = getattr(current.clusters, cluster_name, None)
        return getattr(cluster, attr, default)

    cluster_host = vals.get("cluster_host", get_cluster_conf("host", "test"))
    cluster_timezone = vals.get(
        "cluster_timezone", get_cluster_conf("timezone", "America/Montreal")
    )
    cluster_sacct_bin = vals.get(
        "cluster_sacct_bin", get_cluster_conf("sacct_bin", "sacct")
    )
    cluster_accounts = vals.get("cluster_accounts", get_cluster_conf("accounts", None))

    mongo_conf = MongoConfig(url=mongo_url, database=mongo_database)
    cluster_conf = ClusterConfig(
        host=cluster_host,
        timezone=cluster_timezone,
        sacct_bin=cluster_sacct_bin,
        accounts=cluster_accounts,
    )
    conf = Config(
        mongo=mongo_conf,
        sshconfig=None,
        cache=tmp_path,
        clusters={cluster_name: cluster_conf},
    )
    with using_config(conf):
        yield conf
