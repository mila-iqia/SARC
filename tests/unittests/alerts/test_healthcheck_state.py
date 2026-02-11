from datetime import timedelta
from pathlib import Path

import pytest

from sarc.alerts.common import HealthCheck, CheckStatus
from sarc.alerts.healthcheck_state import HealthCheckState, HealthCheckStateRepository
from sarc.config import config
from tests.unittests.alerts.definitions import BeanCheck


@pytest.fixture
def testing_repo():
    db = config().mongo.database_instance
    repo = HealthCheckStateRepository(db)
    db.healthcheck.delete_many({})
    return repo


@pytest.fixture
def testing_tmpdir(tmpdir):
    return Path(str(tmpdir))


def test_get_state_none(testing_repo):
    assert testing_repo.get_state("non_existent") is None


def test_config_read_write_into_db(beans_config, testing_repo):
    check_many_beans = config().health_monitor.checks["many_beans"]
    assert type(check_many_beans) is BeanCheck

    state = HealthCheckState(check=check_many_beans)
    assert testing_repo.get_state("many_beans") is None
    testing_repo.save(state)
    db_state = testing_repo.get_state("many_beans")
    assert type(db_state.check) is BeanCheck
    assert db_state == state


def test_HealthCheckStateRepository(testing_repo, testing_tmpdir):
    hc = HealthCheck(
        name="test_check",
        active=True,
        interval=timedelta(hours=1),
        directory=testing_tmpdir,
    )
    state = HealthCheckState(check=hc)

    # Test save and retrieving
    testing_repo.save(state)
    retrieved = testing_repo.get_state("test_check")
    assert retrieved is not None
    assert retrieved.check == hc
    assert retrieved.id == state.id
    assert retrieved.last_result is None
    assert retrieved.last_message is None
    assert retrieved == state

    # Test update with result
    result = hc.ok()
    state.last_result = result
    state.last_message = "All good"
    testing_repo.save(state)

    retrieved_updated = testing_repo.get_state("test_check")
    assert retrieved_updated.last_result is not None
    assert retrieved_updated.last_result.status == CheckStatus.OK
    assert retrieved_updated.last_message == "All good"
    assert retrieved_updated == state

    # Ensure circular reference is broken on update
    assert retrieved_updated.last_result.check is None
