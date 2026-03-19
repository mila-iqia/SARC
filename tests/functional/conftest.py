from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import freezegun
import gifnoc
import pytest
from pytest_regressions.data_regression import RegressionYamlDumper

from sarc.config import config

from .allocations.factory import create_allocations
from .diskusage.factory import create_diskusages
from .jobs.factory import (
    create_cluster_entries,
    create_gpu_billings,
    create_jobs,
    create_users,
)


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
    return create_allocations()


@pytest.fixture
def db_jobs():
    return create_jobs()


@contextmanager
def custom_db_config(db_name):
    assert "test" in db_name
    with gifnoc.overlay({"sarc.mongo.database_name": db_name}):
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

    def db_name(self, request=None):
        if request is None:
            return f"test-db-{self.base_name}"
        else:
            m = hashlib.md5()
            m.update(request.node.nodeid.encode())
            return f"test-db-{self.base_name}-{m.hexdigest()}"

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
        db_name = self.db_name(None if self.read_only else request)
        with custom_db_config(db_name):
            db = config().mongo.database_instance
            self._clear(db)
        if not self.empty:
            self._fill(db)
        try:
            yield db_name
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

read_only_db_config_object = DbConfiguration(
    "r", with_clusters=True, read_only=True
).fixture()

read_only_db_with_many_cpu_jobs_config_object = DbConfiguration(
    "r-jobs",
    job_patch={
        "allocated": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
        "requested": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
    },
    read_only=True,
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
def read_only_db(read_only_db_config_object):
    with custom_db_config(read_only_db_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_only_db_with_many_cpu_jobs(read_only_db_with_many_cpu_jobs_config_object):
    with custom_db_config(read_only_db_with_many_cpu_jobs_config_object):
        yield config().mongo.database_instance


@pytest.fixture
def read_only_db_with_users(read_only_db_with_users_config_object):
    with custom_db_config(read_only_db_with_users_config_object):
        yield config().mongo.database_instance
