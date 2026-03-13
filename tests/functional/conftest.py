from __future__ import annotations

import hashlib
from contextlib import contextmanager

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


def clear_db(db):
    db.allocations.drop()
    db.jobs.drop()
    db.diskusage.drop()
    db.users.drop()
    db.clusters.drop()
    db.gpu_billing.drop()
    db.node_gpu_mapping.drop()
    db.healthcheck.drop()
    db.runstate.drop()


def fill_db(db, with_users=False, with_clusters=False, job_patch=None):
    db.allocations.insert_many(create_allocations())
    db.jobs.insert_many(create_jobs(job_patch=job_patch))
    db.diskusage.insert_many(create_diskusages())
    db.gpu_billing.insert_many(create_gpu_billings())
    if with_users:
        db.users.insert_many(create_users())

    if with_clusters:
        # Fill collection `clusters`.
        db.clusters.insert_many(create_cluster_entries())


def create_db_configuration_fixture(
    empty=False,
    with_users=False,
    with_clusters=False,
    job_patch=None,
):
    @pytest.fixture(scope="function")
    def fixture(request):
        m = hashlib.md5()
        m.update(request.node.nodeid.encode())
        db_name = f"test-db-{m.hexdigest()}"
        with custom_db_config(db_name):
            db = config().mongo.database_instance
            clear_db(db)
            if not empty:
                fill_db(
                    db,
                    with_users=with_users,
                    with_clusters=with_clusters,
                    job_patch=job_patch,
                )
            yield db_name

    return fixture


empty_read_write_db_config_object = create_db_configuration_fixture(empty=True)

read_write_db_config_object = create_db_configuration_fixture(with_clusters=True)

read_write_db_with_users_config_object = create_db_configuration_fixture(
    with_users=True
)

read_only_db_config_object = create_db_configuration_fixture(with_clusters=True)

read_only_db_with_many_cpu_jobs_config_object = create_db_configuration_fixture(
    job_patch={
        "allocated": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
        "requested": {"billing": 0, "cpu": 0, "gres_gpu": 0, "mem": 0, "node": 0},
    }
)

read_only_db_with_users_config_object = create_db_configuration_fixture(
    with_users=True,
    with_clusters=True,
)


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
