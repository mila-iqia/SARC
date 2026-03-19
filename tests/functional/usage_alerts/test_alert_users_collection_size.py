"""
Test alert function `UsersCollectionSizeCheck`.

This check detects unexpected changes in the number of users in the database.
It should:
- Return OK on first check (no previous state)
- Return OK when size hasn't changed since last check
- Return FAIL when size increased unexpectedly
- Return FAIL when size decreased unexpectedly
- Return OK when size changed but users were parsed after last check
"""

import logging
import re
from datetime import UTC, datetime

import gifnoc
import pytest
import time_machine
import yaml

from sarc.config import config
from sarc.core.models.runstate import set_parsed_date
from sarc.users.db import UserDB

MOCK_TIME = datetime(2023, 11, 22, 0, 0, 0, tzinfo=UTC)

HEALTH_CONFIG_YAML = """
sarc:
  health_monitor:
    checks:
      users_collection_size:
        $class: "sarc.alerts.usage_alerts.users_collection_size:UsersCollectionSizeCheck"
        active: true
"""

CHECK_NAME = "users_collection_size"


@pytest.fixture
def health_config():
    """Special sarc config with health checks for testing"""
    with gifnoc.overlay(yaml.safe_load(HEALTH_CONFIG_YAML)):
        yield config().health_monitor


def _run_check(cli_main):
    """Run the users collection size health check via CLI."""
    return cli_main(["health", "run", "--check", CHECK_NAME])


def _get_error_logs(text: str) -> list[str]:
    """Parse error logs from caplog text."""
    errors = []
    for line in text.splitlines():
        if line.startswith("ERROR "):
            error_msg = re.sub(r"^ERROR +sarc\..+\.py:[0-9]+ +", "", line.lstrip())
            assert error_msg
            errors.append(error_msg)
    return errors


def _insert_fake_users(db, count: int):
    """Insert fake user documents into the users collection."""
    users = [
        UserDB(
            display_name=f"Fake User {i}",
            email=f"fake_user_{i}@example.com",
            matching_ids={},
        ).model_dump()
        for i in range(count)
    ]
    db.users.insert_many(users)


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db_with_users", "health_config")
def test_first_check(caplog, cli_main):
    """First check with no previous state should return OK."""
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert "First check for user collection" in caplog.text
        assert not _get_error_logs(caplog.text)


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db_with_users", "health_config")
def test_same_size(caplog, cli_main):
    """Running check twice without changes should return OK both times."""
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert "First check for user collection" in caplog.text

    caplog.clear()

    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert not _get_error_logs(caplog.text)
        assert "First check for user collection" not in caplog.text


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db_with_users", "health_config")
def test_size_increased(caplog, cli_main):
    """Adding users between checks should return FAIL."""
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0

    # Add extra users
    db = config().mongo.database_instance
    _insert_fake_users(db, 3)

    caplog.clear()

    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert _get_error_logs(caplog.text) == [
            "Nb. users increased (10 -> 13) "
            "since latest check at: 2023-11-22 00:00:00+00:00",
            "[users_collection_size] FAILURE: users_collection_size",
        ]


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db_with_users", "health_config")
def test_size_decreased(caplog, cli_main):
    """Removing users between checks should return FAIL."""
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0

    # Remove some users
    db = config().mongo.database_instance
    db.users.delete_one({})

    caplog.clear()

    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert _get_error_logs(caplog.text) == [
            "Nb. users decreased (10 -> 9) "
            "since latest check at: 2023-11-22 00:00:00+00:00",
            "[users_collection_size] FAILURE: users_collection_size",
        ]


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("read_write_db_with_users", "health_config")
def test_size_changed_but_users_parsed(caplog, cli_main):
    """Size change after a user parsing should return OK (change is expected)."""
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0

    # Add extra users
    db = config().mongo.database_instance
    _insert_fake_users(db, 3)

    # Set a parsed_date in the future (after the last check)
    set_parsed_date(db, "users", datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC))

    caplog.clear()

    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert not _get_error_logs(caplog.text)
        assert "Users parsed after latest checking" in caplog.text


@pytest.mark.usefixtures("read_write_db_with_users", "health_config")
def test_state_tracks_previous_check(caplog, cli_main):
    """Verify that each check references the previous check's date in its error message.

    Run 1 (T1): first check, OK, size=10
    Run 2 (T2): add users, FAIL, error message cites T1
    Run 3 (T3): add more users, FAIL, error message cites T2 (not T1)
    """
    t1 = datetime(2023, 6, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2023, 9, 15, 8, 30, 0, tzinfo=UTC)
    t3 = datetime(2024, 1, 10, 16, 45, 0, tzinfo=UTC)
    db = config().mongo.database_instance

    # Run 1 at T1: first check
    with time_machine.travel(t1, tick=False), caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert "First check for user collection" in caplog.text
        assert not _get_error_logs(caplog.text)

    # Add 2 users (10 -> 12)
    _insert_fake_users(db, 2)
    caplog.clear()

    # Run 2 at T2: should cite T1 in error
    with time_machine.travel(t2, tick=False), caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert _get_error_logs(caplog.text) == [
            "Nb. users increased (10 -> 12) "
            "since latest check at: 2023-06-01 12:00:00+00:00",
            "[users_collection_size] FAILURE: users_collection_size",
        ]

    # Add 3 more users (12 -> 15)
    _insert_fake_users(db, 3)
    caplog.clear()

    # Run 3 at T3: should cite T2 (not T1) in error
    with time_machine.travel(t3, tick=False), caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert _get_error_logs(caplog.text) == [
            "Nb. users increased (12 -> 15) "
            "since latest check at: 2023-09-15 08:30:00+00:00",
            "[users_collection_size] FAILURE: users_collection_size",
        ]


@time_machine.travel(MOCK_TIME, tick=False)
@pytest.mark.usefixtures("health_config")
def test_first_check_empty_db(caplog, cli_main, empty_read_write_db):
    """First check on an empty DB (no users, no runstate) should return OK."""
    with caplog.at_level(logging.DEBUG):
        assert _run_check(cli_main) == 0
        assert "First check for user collection" in caplog.text
        assert "No latest parsed date for users" in caplog.text
        assert not _get_error_logs(caplog.text)
