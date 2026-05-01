from __future__ import annotations

import json
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
from sqlalchemy import text
from sqlmodel import create_engine, select

from sarc.config import config, using_sarc_mode
from sarc.db.cluster import SlurmClusterDB

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


@contextmanager
def custom_db_config(db_name, additional_overrides={}):

    assert db_name.startswith("test-db-")

    with gifnoc.overlay(
        {
            "sarc.db.name": db_name,
            "sarc.db.host": "localhost",
            "sarc.db.auto_upgrade": True,
            **additional_overrides,
        }
    ):
        assert config().db.name == db_name
        yield


@dataclass
class DbConfiguration:
    base_name: str
    empty: bool = False
    job_patch: Any = None
    read_only: bool = False

    @cached_property
    def db_name(self):
        return f"test-db-{self.base_name}-{uuid4().hex}"

    def _fill(self, db):
        from .db.factory import (
            create_allocations,
            create_diskusages,
            create_gpu_billings,
            create_jobs,
            create_users,
        )

        with db.session() as sess:
            # Clusters are populated through db_upgrade given our configuration
            clusters = sess.exec(select(SlurmClusterDB)).all()

            billings = create_gpu_billings(clusters=clusters)
            sess.add_all(billings)

            allocations = create_allocations(clusters=clusters)
            sess.add_all(allocations)

            diskusages = create_diskusages()
            sess.add_all(diskusages)

            users = create_users()
            sess.add_all(users)

            jobs = create_jobs(clusters=clusters, users=users)
            sess.add_all(jobs)

            sess.commit()

    def executive(self, req):
        admin_engine = create_engine(
            "postgresql+psycopg://localhost/postgres", isolation_level="AUTOCOMMIT"
        )
        with admin_engine.connect() as conn:
            conn.execute(text(req))
        admin_engine.dispose()

    def __call__(self, request):
        with custom_db_config(self.db_name):
            self.executive(f'CREATE DATABASE "{self.db_name}"')
            try:
                if not self.empty:
                    self._fill(config().db)
                yield self.db_name
            finally:
                self.executive(f'DROP DATABASE "{self.db_name}" WITH (FORCE)')

    def fixture(self):
        scope = "session" if self.read_only else "function"
        return pytest.fixture(scope=scope)(self.__call__)


empty_read_write_db_config_object = DbConfiguration("empty-rw", empty=True).fixture()

read_write_db_config_object = DbConfiguration("rw").fixture()

read_write_db_with_many_cpu_jobs_config_object = DbConfiguration(
    "r-jobs",
    job_patch={
        "allocated": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
        "requested": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
    },
).fixture()

read_only_db_config_object = DbConfiguration("r", read_only=True).fixture()


@pytest.fixture
def empty_read_write_db(empty_read_write_db_config_object):
    with custom_db_config(empty_read_write_db_config_object):
        with config().db.session() as session:
            yield session


@pytest.fixture
def read_write_db(read_write_db_config_object):
    with custom_db_config(read_write_db_config_object):
        with config().db.session() as session:
            yield session


@pytest.fixture
def read_write_db_with_many_cpu_jobs(read_write_db_with_many_cpu_jobs_config_object):
    with custom_db_config(read_write_db_with_many_cpu_jobs_config_object):
        with config().db.session() as session:
            yield session


@pytest.fixture
def read_only_db(read_only_db_config_object):
    with custom_db_config(read_only_db_config_object):
        with config().db.session() as session:
            yield session


@pytest.fixture
def results_regression(file_regression):
    def check(results):
        txt = f"Found {len(results)} result(s):\n"
        for i, x in enumerate(sorted(results, key=lambda x: x.id)):
            txt += f"\nResult #{i + 1}\n"
            md = json.loads(x.model_dump_json(exclude={"id": True}, indent=4))
            md = {k: v for k, v in sorted(md.items())}
            txt += json.dumps(md, indent=4)
        file_regression.check(txt)

    return check
