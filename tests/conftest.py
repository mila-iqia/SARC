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


@pytest.fixture(autouse=True)
def standard_config(standard_config_object, tmp_path):
    cfg = standard_config_object.replace(cache=tmp_path / "sarc-tmp-test-cache")
    with using_config(cfg) as cfg:
        yield cfg


@pytest.fixture
def disabled_cache():
    cfg = config().replace(cache=None)
    with using_config(cfg) as cfg:
        yield


@pytest.fixture
def tzlocal_is_mtl(monkeypatch):
    monkeypatch.setattr("sarc.config.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))
    monkeypatch.setattr("sarc.jobs.job.TZLOCAL", zoneinfo.ZoneInfo("America/Montreal"))


@pytest.fixture
def test_config(
    request,
):
    current = config()

    vals = getattr(request, "param", dict())

    mongo_repl = vals.pop("mongo", {})
    clusters_repl = vals.pop("clusters", {})
    clusters_orig = current.clusters

    new_clusters = {}
    for name in clusters_orig:
        if name in clusters_repl:
            new_clusters[name] = clusters_orig[name].replace(**clusters_repl[name])
        else:
            # This is to make a clone
            new_clusters[name] = clusters_orig[name].replace()

    # Look at all the new names in repl
    for name in set(clusters_repl.keys()) - set(clusters_orig.keys()):
        new_clusters[name] = ClusterConfig(
            **(dict(host="test", timezone="America/Montreal") | clusters_repl[name])
        )

    conf = current.replace(
        mongo=current.mongo.replace(**mongo_repl),
        sshconfig=None,
        clusters=new_clusters,
    )
    with using_config(conf):
        yield conf


@pytest.fixture
def cli_main():
    from sarc.cli import main
    from sarc.cli.utils import clusters

    # Update possible choices based on the current test config
    clusters.choices = list(config().clusters.keys())

    yield main
