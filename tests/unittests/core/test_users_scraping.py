"""Tests for the user scraping plugin system."""

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib.metadata import EntryPoint, EntryPoints
from typing import Any
from unittest.mock import patch

import pytest

from sarc.cache import Cache
from sarc.core.models.users import Credentials
from sarc.core.scraping.users import (
    MatchID,
    UserMatch,
    UserScraper,
    _builtin_scrapers,
    fetch_users,
    get_user_scraper,
    parse_users,
    update_user_match,
)
from sarc.users.db import get_user, get_user_collection

one_hour = timedelta(hours=1)


class UserPluginTester:
    plugin: UserScraper
    raw_config: Any = None
    parsed_config: Any = {}

    def test_config_validation(self):
        conf = self.plugin.validate_config(self.raw_config)
        assert isinstance(conf, self.plugin.config_type)
        assert conf == self.parsed_config

    def test_fetch_data(self):
        raise NotImplementedError()

    def test_parse_data(self, raw_data, data_regression):
        raise NotImplementedError()
        # This code is what you should put in your implementation, but
        # limitations on parametrize require that the method be in your class
        # rather than this one.
        data = self.plugin.parse_user_data(self.parsed_config, raw_data)
        data_regression.check(data)


@dataclass
class MockConfig:
    api_url: str
    api_key: str


class MockUserScraper(UserScraper[MockConfig]):
    config_type = MockConfig

    def get_user_data(self, config: MockConfig) -> bytes:
        return config.api_url.encode()

    def parse_user_data(self, _data: bytes) -> Iterable[UserMatch]:
        users = []

        user1 = UserMatch(
            display_name="John Doe",
            email="john.doe@example.com",
            matching_id=MatchID(name="mock_plugin", mid="user1"),
        )
        user1.github_username.insert("johndoe")
        user1.google_scholar_profile.insert("scholar.google.com/citations?user=abc123")
        users.append(user1)

        user2 = UserMatch(
            display_name="Jane Smith",
            email=None,
            matching_id=MatchID(name="mock_plugin", mid="user2"),
        )
        users.append(user2)

        user3 = UserMatch(
            display_name="Bob Wilson",
            email="bob.wilson@example.com",
            matching_id=MatchID(name="mock_plugin", mid="user3"),
            known_matches={
                MatchID(name="other_plugin", mid="bob_wilson"),
                MatchID(name="another_plugin", mid="bwilson"),
            },
        )
        users.append(user3)

        return users


@dataclass
class TestConfig:
    domain: str


class TestPlugin(UserScraper[TestConfig]):
    config_type = TestConfig

    def validate_config(self, config_data: Any) -> TestConfig:
        return TestConfig(domain=config_data)

    def get_user_data(self, config: TestConfig) -> bytes:
        return config.domain.encode("utf-8")

    def parse_user_data(self, data: bytes) -> Iterable[UserMatch]:
        users = []
        domain = data.decode("utf-8")
        users.append(
            UserMatch(
                email=f"john@{domain}",
                matching_id=MatchID(name="test", mid="john"),
            )
        )
        users.append(
            UserMatch(
                email=f"jane@{domain}",
                matching_id=MatchID(name="test", mid="jane"),
            )
        )
        return users


@pytest.fixture
def user_plugin(monkeypatch):
    yield monkeypatch.setattr(
        "sarc.core.scraping.users._user_scrapers",
        EntryPoints(
            [
                EntryPoint(
                    name="test_plugin",
                    group="sarc.user_scraper",
                    value=f"{MockUserScraper.__module__}:{MockUserScraper.__name__}",
                )
            ]
        ),
    )


@pytest.fixture
def mock_scraper(monkeypatch):
    mock_scraper = MockUserScraper()
    yield monkeypatch.setitem(_builtin_scrapers, "test_scraper", mock_scraper)


def test_match_id():
    mid1 = MatchID(name="test_plugin", mid="user123")
    mid2 = MatchID(name="test_plugin", mid="user123")
    mid3 = MatchID(name="test_plugin", mid="user456")

    assert hash(mid1) == hash(mid2)
    assert mid1 == mid2
    assert hash(mid1) != hash(mid3)
    assert mid1 != mid3
    assert mid1 != "not_a_match_id"


def test_user_match_equality():
    """Test UserMatch equality based on matching_id."""
    mid1 = MatchID(name="test_plugin", mid="user123")
    mid2 = MatchID(name="test_plugin", mid="user456")

    user1 = UserMatch(matching_id=mid1, display_name="A")
    user2 = UserMatch(matching_id=mid1, display_name="B")
    user3 = UserMatch(matching_id=mid2)

    assert hash(user1) == hash(user2)
    assert user1 == user2
    assert user1 != user3


def test_update_user_match_fill_missing_data():
    base_user = UserMatch(
        display_name=None,
        email=None,
        matching_id=MatchID(name="plugin1", mid="user1"),
    )

    update_user = UserMatch(
        display_name="John Doe",
        email="john@example.com",
        matching_id=MatchID(name="plugin2", mid="user1"),
    )

    update_user_match(value=base_user, update=update_user)

    assert base_user.display_name == "John Doe"
    assert base_user.email == "john@example.com"


def test_update_user_match_preserve_existing_data():
    base_user = UserMatch(
        display_name="John Doe",
        email="john@example.com",
        matching_id=MatchID(name="plugin1", mid="user1"),
    )

    update_user = UserMatch(
        display_name="Different Name",
        email="different@example.com",
        matching_id=MatchID(name="plugin2", mid="user1"),
    )

    update_user_match(value=base_user, update=update_user)

    assert base_user.display_name == "John Doe"
    assert base_user.email == "john@example.com"


def test_update_user_match_merge_known_matches():
    base_user = UserMatch(
        matching_id=MatchID(name="plugin1", mid="user1"),
        known_matches={MatchID(name="plugin2", mid="user1")},
    )

    update_user = UserMatch(
        matching_id=MatchID(name="plugin3", mid="user1"),
        known_matches={MatchID(name="plugin4", mid="user1")},
    )

    update_user_match(value=base_user, update=update_user)

    expected_matches = {
        MatchID(name="plugin2", mid="user1"),
        MatchID(name="plugin3", mid="user1"),
        MatchID(name="plugin4", mid="user1"),
    }
    assert base_user.known_matches == expected_matches


def test_update_user_match_merge_valid_fields():
    base_user = UserMatch(
        matching_id=MatchID(name="plugin1", mid="user1"),
    )

    update_user = UserMatch(
        matching_id=MatchID(name="plugin2", mid="user1"),
    )

    # Add some data to the update user
    update_user.github_username.insert("newuser")
    update_user.google_scholar_profile.insert(
        "scholar.google.com/citations?user=newuser"
    )

    update_user_match(value=base_user, update=update_user)

    # Check that the data was merged
    assert base_user.github_username.get_value() == "newuser"
    assert (
        base_user.google_scholar_profile.get_value()
        == "scholar.google.com/citations?user=newuser"
    )


def test_get_user_scraper_builtin(mock_scraper):
    result = get_user_scraper("test_scraper")
    assert isinstance(result, MockUserScraper)


def test_get_user_scraper_plugin(user_plugin):
    result = get_user_scraper("test_plugin")
    assert isinstance(result, MockUserScraper)


def test_get_user_scraper_not_found():
    with pytest.raises(KeyError):
        get_user_scraper("non_existent_scraper")


def test_fetch_and_parse_users_single_plugin(mock_scraper, enabled_cache):
    scrapers = [
        ("test_scraper", {"api_url": "https://api.example.com", "api_key": "secret"})
    ]

    # First, fetch the data and store it in cache
    fetch_users(scrapers)

    # Then, parse the cached data
    users = list(parse_users(datetime.now(UTC) - one_hour))

    assert len(users) == 3
    assert all(isinstance(user, UserMatch) for user in users)

    # Check that the first plugin name was assigned to matching_id.name
    for user in users:
        assert user.matching_id.name == "test_scraper"


def test_fetch_and_parse_users_multiple_plugins(monkeypatch, enabled_cache):
    """Test fetching and parsing users with multiple plugins."""
    mock_scraper = MockUserScraper()
    monkeypatch.setitem(_builtin_scrapers, "plugin1", mock_scraper)
    monkeypatch.setitem(_builtin_scrapers, "plugin2", mock_scraper)

    scrapers = [
        ("plugin1", {"api_url": "https://api1.example.com", "api_key": "secret1"}),
        ("plugin2", {"api_url": "https://api2.example.com", "api_key": "secret2"}),
    ]

    # First, fetch the data and store it in cache
    fetch_users(scrapers)

    # Then, parse the cached data
    users = list(parse_users(datetime.now(UTC) - one_hour))

    assert len(users) == 5  # 3 from plugin1 + 2 from plugin2 (user3 is merged)

    plugin1_users = [u for u in users if u.matching_id.name == "plugin1"]
    plugin2_users = [u for u in users if u.matching_id.name == "plugin2"]
    assert len(plugin1_users) == 3
    assert len(plugin2_users) == 2


def test_invalid_scraper(enabled_cache, caplog):
    scrapers = [("invalid_scraper", {"api_url": "https://api.example.com"})]

    fetch_users(scrapers)
    assert caplog.record_tuples[0] == (
        "sarc.core.scraping.users",
        logging.ERROR,
        "Could not fetch user scraper: invalid_scraper",
    )

    cache = Cache(subdirectory="users")
    with cache.create_entry(datetime(2025, 6, 2, tzinfo=UTC)) as ce:
        ce.add_value("invalid_scraper", b"")

    with pytest.raises(ValueError, match="Invalid user scraper"):
        list(parse_users(datetime(2025, 6, 1, tzinfo=UTC)))


@patch("sarc.core.scraping.users.get_user_scraper")
def test_fetch_and_parse_users_user_matching(mock_get_scraper, enabled_cache):
    """Test that users with matching IDs are properly merged."""

    # Create a mock scraper that returns users with known matches
    class MatchingMockScraper(MockUserScraper):
        def parse_user_data(self, data: bytes) -> Iterable[UserMatch]:
            api_url = data.decode("utf-8")
            # Return different users based on the config (which plugin)
            if "api1" in api_url:
                user1 = UserMatch(
                    display_name="John Doe",
                    email="john@example.com",
                    matching_id=MatchID(name="plugin1", mid="user1"),
                    known_matches={MatchID(name="plugin2", mid="user1")},
                )
                return [user1]
            else:  # plugin2
                user2 = UserMatch(
                    display_name=None,
                    email="john@example.com",
                    matching_id=MatchID(name="plugin2", mid="user1"),
                )
                return [user2]

    mock_scraper = MatchingMockScraper()
    mock_get_scraper.return_value = mock_scraper

    scrapers = [
        ("plugin1", {"api_url": "https://api1.example.com", "api_key": "secret1"}),
        ("plugin2", {"api_url": "https://api2.example.com", "api_key": "secret2"}),
    ]

    # First, fetch the data and store it in cache
    fetch_users(scrapers)

    # Then, parse the cached data
    users = list(parse_users(datetime.now(UTC) - one_hour))

    # Should have only one user after merging
    assert len(users) == 1

    # The merged user should have data from both plugins
    merged_user = users[0]
    # The first plugin should win when there's a conflict
    assert merged_user.display_name == "John Doe"  # From plugin1 (first plugin wins)
    assert merged_user.email == "john@example.com"  # From both plugins
    assert merged_user.matching_id.name == "plugin1"  # First plugin wins


def test_update_user_match_merge_credentials_new_domain():
    """Test merging credentials when the update user has a new domain."""
    base_user = UserMatch(
        matching_id=MatchID(name="plugin1", mid="user1"),
        associated_accounts={
            "drac": Credentials(),
        },
    )
    base_user.associated_accounts["drac"].insert("user1_drac")

    update_user = UserMatch(
        matching_id=MatchID(name="plugin2", mid="user1"),
        associated_accounts={
            "mila": Credentials(),
        },
    )
    update_user.associated_accounts["mila"].insert("user1_mila")

    update_user_match(value=base_user, update=update_user)

    # Should have both domains
    assert "drac" in base_user.associated_accounts
    assert "mila" in base_user.associated_accounts
    assert base_user.associated_accounts["drac"].get_value() == "user1_drac"
    assert base_user.associated_accounts["mila"].get_value() == "user1_mila"


def test_update_user_match_merge_credentials_existing_domain():
    """Test merging credentials when both users have the same domain."""
    base_user = UserMatch(
        matching_id=MatchID(name="plugin1", mid="user1"),
        associated_accounts={
            "drac": Credentials(),
        },
    )
    base_user.associated_accounts["drac"].insert("user1_drac")

    update_user = UserMatch(
        matching_id=MatchID(name="plugin2", mid="user1"),
        associated_accounts={
            "drac": Credentials(),
        },
    )
    update_user.associated_accounts["drac"].insert("user1_drac_updated")

    update_user_match(value=base_user, update=update_user)

    assert "drac" in base_user.associated_accounts
    assert base_user.associated_accounts["drac"].get_value() == "user1_drac"
    assert base_user.associated_accounts["drac"].get_value() == "user1_drac"


def test_fetch_and_parse_multiple_different_scrapers(monkeypatch, enabled_cache):
    """Test fetching and parsing users with multiple different scrapers and verify merging behavior."""
    # Set up both scrapers in the builtin scrapers
    mock_scraper = MockUserScraper()
    test_plugin = TestPlugin()
    monkeypatch.setitem(_builtin_scrapers, "mock_scraper", mock_scraper)
    monkeypatch.setitem(_builtin_scrapers, "test_plugin", test_plugin)

    scrapers = [
        ("mock_scraper", {"api_url": "https://api.example.com", "api_key": "secret"}),
        ("test_plugin", "example.com"),
    ]

    # First, fetch the data and store it in cache
    fetch_users(scrapers)

    # Then, parse the cached data
    users = list(parse_users(datetime.now(UTC) - one_hour))

    # MockUserScraper returns 3 users, TestPlugin returns 2 users
    # Total should be 5 users (no merging expected since they have different matching IDs)
    assert len(users) == 5

    # Check that we have users from both scrapers
    mock_users = [u for u in users if u.matching_id.name == "mock_scraper"]
    test_users = [u for u in users if u.matching_id.name == "test_plugin"]

    assert len(mock_users) == 3
    assert len(test_users) == 2

    # Verify MockUserScraper users have the expected data
    mock_user_emails = {u.email for u in mock_users if u.email}
    expected_mock_emails = {"john.doe@example.com", "bob.wilson@example.com"}
    assert mock_user_emails == expected_mock_emails

    # Verify TestPlugin users have the expected domain-based emails
    test_user_emails = {u.email for u in test_users if u.email}
    expected_test_emails = {"john@example.com", "jane@example.com"}
    assert test_user_emails == expected_test_emails

    # Verify that all users have the correct scraper name in their matching_id
    for user in users:
        assert user.matching_id.name in ["mock_scraper", "test_plugin"]


@patch("sarc.core.scraping.users.get_user_scraper")
def test_parse_users_supervisor_ordering_before_fix(mock_get_scraper, enabled_cache):
    """Test that plugins returning users before supervisors are correctly ordered.

    This test reproduces the issue where a plugin returns user entries before
    their supervisor entry. The topological sort should ensure supervisors are
    yielded first, preventing database lookup failures and conflicts on second import.
    """

    class SupervisorOrderingMockScraper(MockUserScraper):
        def parse_user_data(self, data: bytes) -> Iterable[UserMatch]:
            student = UserMatch(
                display_name="Alice Student",
                email="alice@example.com",
                matching_id=MatchID(name="bad_order_plugin", mid="student1"),
            )
            student.co_supervisors.insert(
                {MatchID(name="bad_order_plugin", mid="supervisor1")},
                start=None,
                end=None,
            )

            supervisor = UserMatch(
                display_name="Bob Supervisor",
                email="bob@example.com",
                matching_id=MatchID(name="bad_order_plugin", mid="supervisor1"),
            )

            return [student, supervisor]

    mock_scraper = SupervisorOrderingMockScraper()
    mock_get_scraper.return_value = mock_scraper

    scrapers = [
        (
            "bad_order_plugin",
            {"api_url": "https://bad_order.example.com", "api_key": "secret"},
        ),
    ]

    fetch_users(scrapers)

    coll = get_user_collection()
    for um in parse_users(datetime.now(UTC) - one_hour):
        coll.update_user(um)

    u = get_user("alice@example.com")
    assert u is not None
    assert len(u.co_supervisors.values) == 1
    assert len(u.co_supervisors.values[0].value) == 1

    for um in parse_users(datetime.now(UTC) - one_hour):
        coll.update_user(um)

    u = get_user("alice@example.com")
    assert u is not None
    assert len(u.co_supervisors.values) == 1
    assert len(u.co_supervisors.values[0].value) == 1
