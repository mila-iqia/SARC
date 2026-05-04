from sarc.alerts.common import CheckStatus, HealthCheck
from sarc.alerts.healthcheck_state import HealthCheckState
from sarc.config import config
from sarc.db.heatlhcheck import HealthCheckStateDB
from tests.unittests.alerts.definitions import BeanCheck


def test_get_state_none(empty_read_write_db):
    assert HealthCheckStateDB.get_state(empty_read_write_db, "non_existent") is None


def test_config_read_write_into_db(empty_read_write_db, beans_config):
    check_many_beans = config().health_monitor.checks["many_beans"]
    assert type(check_many_beans) is BeanCheck

    state = HealthCheckState(check=check_many_beans)
    assert HealthCheckStateDB.get_state(empty_read_write_db, "many_beans") is None
    HealthCheckStateDB.get_or_create(empty_read_write_db, state)

    db_state = HealthCheckStateDB.get_state(empty_read_write_db, "many_beans")
    assert type(db_state.check) is BeanCheck
    assert db_state == state


def test_get_states_empty(empty_read_write_db):
    assert list(HealthCheckStateDB.get_states(empty_read_write_db)) == []


def test_get_states_returns_sorted(empty_read_write_db):
    for name in ["charlie", "alpha", "bravo"]:
        hc = HealthCheck(name=name, active=True)
        HealthCheckStateDB.get_or_create(HealthCheckState(check=hc))

    states = list(HealthCheckStateDB.get_states(empty_read_write_db))
    assert len(states) == 3
    assert [s.check.name for s in states] == ["alpha", "bravo", "charlie"]


def test_get_states_with_results(empty_read_write_db):
    hc_ok = BeanCheck(name="check_ok", active=True, beans=15)
    hc_fail = HealthCheck(name="check_fail", active=True)

    state_ok = HealthCheckState(
        check=hc_ok, last_result=hc_ok.ok(), last_message="good"
    )
    state_fail = HealthCheckState(check=hc_fail, last_result=hc_fail.fail())
    HealthCheckStateDB.get_or_create(empty_read_write_db, state_ok)
    HealthCheckStateDB.get_or_create(empty_read_write_db, state_fail)

    states = list(HealthCheckStateDB.get_states(empty_read_write_db))
    assert len(states) == 2
    assert states[0].check.name == "check_fail"
    assert states[0].last_result.status == CheckStatus.FAILURE
    assert states[1].check.name == "check_ok"
    assert isinstance(states[1].check, BeanCheck)
    assert states[1].check.beans == 15
    assert states[1].last_result.status == CheckStatus.OK
    assert states[1].last_message == "good"


def test_HealthCheckStateRepository(testing_repo, tmpdir):
    hc = HealthCheck(name="test_check", active=True)
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
