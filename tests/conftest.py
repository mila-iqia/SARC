from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import freezegun
import gifnoc
import pytest
from freezegun.api import FakeDatetime
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import set_tracer_provider
from pytest_regressions.data_regression import RegressionYamlDumper

from sarc.config import config, using_sarc_mode

_tracer_provider = TracerProvider()
_exporter = InMemorySpanExporter()
_tracer_provider.add_span_processor(SimpleSpanProcessor(_exporter))
set_tracer_provider(_tracer_provider)
del _tracer_provider

sys.path.append(os.path.join(os.path.dirname(__file__), "common"))

pytest_plugins = "fabric.testing.fixtures"

RegressionYamlDumper.add_custom_yaml_representer(
    FakeDatetime, lambda dumper, data: dumper.represent_datetime(data)
)


@pytest.fixture
def client_mode():
    with using_sarc_mode("client"):
        yield


@pytest.fixture
def scraping_mode():
    with using_sarc_mode("scraping"):
        yield


@pytest.fixture(scope="session")
def test_config_path():
    yield Path(__file__).parent / "sarc-test.yaml"


@pytest.fixture(scope="session", autouse=True)
def base_config(test_config_path):
    with gifnoc.use(test_config_path):
        with using_sarc_mode("scraping"):
            yield


@pytest.fixture(scope="session")
def base_config_with_logging():
    """To be used where config.logging is required"""
    with gifnoc.use(Path(__file__).parent / "sarc-test-with-logging.yaml"):
        with using_sarc_mode("scraping"):
            yield


@pytest.fixture
def enabled_cache(tmp_path):
    with gifnoc.overlay({"sarc.cache": str(tmp_path / "sarc-tmp-test-cache")}):
        yield


@pytest.fixture
def disabled_cache():
    with gifnoc.overlay({"sarc.cache": None}):
        yield


@pytest.fixture
def tzlocal_is_mtl():
    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = "America/Montreal"
    time.tzset()
    yield
    if old_tz is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = old_tz
    time.tzset()


@pytest.fixture
def test_config(request):
    with gifnoc.overlay({"sarc": getattr(request, "param", dict())}):
        yield config()


@pytest.fixture
def captrace():
    """
    To get the captured traces, use the `.get_finished_traces()`
    method on the captrace object in your test method. This will
    return a list of ReadableSpan objects documented here:
    https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.html#opentelemetry.sdk.trace.ReadableSpan
    """
    _exporter.clear()
    yield _exporter
    _exporter.clear()


@pytest.fixture
def cli_main():
    from sarc.cli import main

    yield main


@pytest.fixture
def prom_custom_query_mock(monkeypatch):
    """Mock the custom_query method of PrometheusConnect to avoid any real query.
    The object `prom_custom_query_mock` may then be used to check the query strings passed
    to `custom_query` using `prom_custom_query_mock.call_args[0][0]`."""
    from prometheus_api_client import PrometheusConnect

    monkeypatch.setattr(PrometheusConnect, "custom_query", MagicMock(return_value=[]))

    yield PrometheusConnect.custom_query


@pytest.fixture
def patch_return_values(monkeypatch):
    def returner(v):
        return lambda *_, **__: v

    def patch(values):
        for k, v in values.items():
            monkeypatch.setattr(k, returner(v))

    yield patch


@pytest.fixture
def no_pkey(monkeypatch):
    """Fix to ignore problems with the pkey argument to connect()"""
    import fabric
    from fabric import Connection

    def Connection_mock(*args, connect_kwargs=None, **kwargs):
        return Connection(*args, **kwargs)

    monkeypatch.setattr(fabric, "Connection", Connection_mock)


# this is to make the pytest-freezegun types serializable by pyyaml
# (for use in pytest-regression)
def repr_fakedatetime(dumper, data):
    value = data.isoformat(" ")
    return dumper.represent_scalar("tag:yaml.org,2002:timestamp", value)


RegressionYamlDumper.add_custom_yaml_representer(
    freezegun.api.FakeDatetime, repr_fakedatetime
)


@pytest.fixture
def db_allocations():
    from .functional.allocations.factory import create_allocations

    return create_allocations()


@pytest.fixture
def db_jobs():
    from .functional.jobs.factory import create_jobs

    return create_jobs()


@contextmanager
def custom_db_config(db_name, additional_overrides={}):
    assert db_name.startswith("test-db-")
    with gifnoc.overlay(
        {
            "sarc.mongo.database_name": db_name,
            "sarc.mongo.connection_string": "localhost:27017",
            **additional_overrides,
        }
    ):
        # Ensure we do not use and thus wipe the production database
        assert config().mongo.database_instance.name == db_name
        yield


@dataclass
class DbConfiguration:
    base_name: str
    empty: bool = False
    with_users: bool = False
    with_clusters: bool = False
    job_patch: Any = None
    read_only: bool = False

    @cached_property
    def db_name(self):
        return f"test-db-{self.base_name}-{uuid4().hex}"

    def _clear(self, db):
        db.allocations.drop()
        db.jobs.drop()
        db.diskusage.drop()
        db.users.drop()
        db.clusters.drop()
        db.gpu_billing.drop()
        db.node_gpu_mapping.drop()
        db.healthcheck.drop()
        db.runstate.drop()
        db.version.drop()

    def _fill(self, db):
        from .functional.allocations.factory import create_allocations
        from .functional.diskusage.factory import create_diskusages
        from .functional.jobs.factory import (
            create_cluster_entries,
            create_gpu_billings,
            create_jobs,
            create_users,
        )

        db.allocations.insert_many(create_allocations())
        db.jobs.insert_many(create_jobs(job_patch=self.job_patch))
        db.diskusage.insert_many(create_diskusages())
        db.gpu_billing.insert_many(create_gpu_billings())
        if self.with_users:
            db.users.insert_many(create_users())

        if self.with_clusters:
            # Fill collection `clusters`.
            db.clusters.insert_many(create_cluster_entries())

    def __call__(self, request):
        with custom_db_config(self.db_name):
            db = config().mongo.database_instance
            self._clear(db)
        if not self.empty:
            self._fill(db)
        try:
            yield self.db_name
        finally:
            db.client.drop_database(db)

    def fixture(self):
        scope = "session" if self.read_only else "function"
        return pytest.fixture(scope=scope)(self.__call__)


empty_read_write_db_config_object = DbConfiguration("empty-rw", empty=True).fixture()

read_write_db_config_object = DbConfiguration("rw", with_clusters=True).fixture()

read_write_db_with_users_config_object = DbConfiguration(
    "rwu", with_users=True
).fixture()

read_write_db_with_many_cpu_jobs_config_object = DbConfiguration(
    "r-jobs",
    job_patch={
        "allocated": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
        "requested": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
    },
).fixture()

read_only_db_config_object = DbConfiguration(
    "r", with_clusters=True, read_only=True
).fixture()

read_only_db_with_users_config_object = DbConfiguration(
    "ru", with_users=True, with_clusters=True, read_only=True
).fixture()


@pytest.fixture
def empty_read_write_db(empty_read_write_db_config_object):
    with custom_db_config(empty_read_write_db_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_write_db(read_write_db_config_object):
    with custom_db_config(read_write_db_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_write_db_with_users(read_write_db_with_users_config_object):
    with custom_db_config(read_write_db_with_users_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_write_db_with_many_cpu_jobs(read_write_db_with_many_cpu_jobs_config_object):
    with custom_db_config(read_write_db_with_many_cpu_jobs_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_only_db(read_only_db_config_object):
    with custom_db_config(read_only_db_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_only_db_with_users(read_only_db_with_users_config_object):
    with custom_db_config(read_only_db_with_users_config_object):
        yield config().mongo.database_instance
