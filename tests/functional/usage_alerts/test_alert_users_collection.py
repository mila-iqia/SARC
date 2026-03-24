"""
Test alert function `UsersCollectionCheck`.

This check detects duplicate users in the database (same email or display_name).
Users come from different sources and are merged; duplicates indicate a merge issue.
"""

import logging
import re

import pytest

from sarc.config import config
from sarc.users.db import UserDB

CHECK_NAME = "users_collection"


def _run_check(cli_main):
    return cli_main(["health", "run", "--check", CHECK_NAME])


def _get_error_logs(text: str) -> list[str]:
    """Extract error log messages (without the logger prefix)."""
    errors = []
    for line in text.splitlines():
        if line.startswith("ERROR "):
            error_msg = re.sub(r"^ERROR +sarc\..+\.py:[0-9]+ +", "", line.lstrip())
            assert error_msg
            errors.append(error_msg)
    return errors


def _insert_users(db, users: list[dict]):
    """Insert user documents into the users collection."""
    docs = [
        UserDB(
            display_name=u["display_name"], email=u["email"], matching_ids={}
        ).model_dump()
        for u in users
    ]
    db.users.insert_many(docs)


@pytest.mark.usefixtures("health_config")
def test_no_duplicates(caplog, cli_main, empty_read_write_db):
    """Unique emails and display_names should return OK."""
    db = config().mongo.database_instance
    _insert_users(
        db,
        [
            {"display_name": "Alice", "email": "alice@example.com"},
            {"display_name": "Bob", "email": "bob@example.com"},
            {"display_name": "Charlie", "email": "charlie@example.com"},
        ],
    )
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert not _get_error_logs(caplog.text)


@pytest.mark.usefixtures("health_config")
def test_duplicate_emails(caplog, cli_main, empty_read_write_db):
    """Two users sharing the same email should return FAIL."""
    db = config().mongo.database_instance
    _insert_users(
        db,
        [
            {"display_name": "Alice", "email": "shared@example.com"},
            {"display_name": "Bob", "email": "shared@example.com"},
            {"display_name": "Charlie", "email": "charlie@example.com"},
        ],
    )
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        errors = _get_error_logs(caplog.text)
        assert any("Duplicate email 'shared@example.com'" in e for e in errors)
        assert not any("Duplicate display_name" in e for e in errors)


@pytest.mark.usefixtures("health_config")
def test_duplicate_display_names(caplog, cli_main, empty_read_write_db):
    """Two users sharing the same display_name should return FAIL."""
    db = config().mongo.database_instance
    _insert_users(
        db,
        [
            {"display_name": "Alice", "email": "alice1@example.com"},
            {"display_name": "Alice", "email": "alice2@example.com"},
            {"display_name": "Bob", "email": "bob@example.com"},
        ],
    )
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        errors = _get_error_logs(caplog.text)
        assert any("Duplicate display_name 'Alice'" in e for e in errors)
        assert not any("Duplicate email" in e for e in errors)


@pytest.mark.usefixtures("health_config")
def test_duplicate_emails_and_display_names(caplog, cli_main, empty_read_write_db):
    """Duplicates in both email and display_name should both be reported."""
    db = config().mongo.database_instance
    _insert_users(
        db,
        [
            {"display_name": "Alice", "email": "shared@example.com"},
            {"display_name": "Alice", "email": "shared@example.com"},
            {"display_name": "Bob", "email": "bob@example.com"},
        ],
    )
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        errors = _get_error_logs(caplog.text)
        assert any("Duplicate email 'shared@example.com'" in e for e in errors)
        assert any("Duplicate display_name 'Alice'" in e for e in errors)


@pytest.mark.usefixtures("health_config")
def test_empty_db(caplog, cli_main, empty_read_write_db):
    """An empty users collection should return OK."""
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        assert not _get_error_logs(caplog.text)


@pytest.mark.usefixtures("health_config")
def test_factory_data_has_duplicates(caplog, cli_main, read_write_db_with_users):
    """The default test factory data contains duplicates and should FAIL.

    The factory creates multiple users with email='test@example.com'
    and display_name='Test User'.
    """
    with caplog.at_level(logging.INFO):
        assert _run_check(cli_main) == 0
        errors = _get_error_logs(caplog.text)
        assert any("Duplicate email 'test@example.com'" in e for e in errors)
        assert any("Duplicate display_name 'Test User'" in e for e in errors)
