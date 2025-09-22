"""Tests for the user scraping plugin system."""

from collections.abc import Iterable
from dataclasses import dataclass
from importlib.metadata import EntryPoint, EntryPoints
from typing import Any
from unittest.mock import patch

import pytest

from sarc.core.models.users import Credentials
from sarc.core.scraping.users import (
    MatchID,
    UserMatch,
    UserScraper,
    _builtin_scrapers,
    get_user_scraper,
    scrape_users,
    update_user_match,
)


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
        return f"mock_data_from_{config.api_url}".encode()

    def parse_user_data(self, config: MockConfig, data: bytes) -> Iterable[UserMatch]:
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


def test_scrape_users_single_plugin(mock_scraper):
    scrapers = [
        ("test_scraper", {"api_url": "https://api.example.com", "api_key": "secret"})
    ]

    users = list(scrape_users(scrapers))

    assert len(users) == 3
    assert all(isinstance(user, UserMatch) for user in users)

    # Check that the first plugin name was assigned to matching_id.name
    for user in users:
        assert user.matching_id.name == "test_scraper"


def test_scrape_users_multiple_plugins(monkeypatch):
    """Test scraping users with multiple plugins."""
    mock_scraper = MockUserScraper()
    monkeypatch.setitem(_builtin_scrapers, "plugin1", mock_scraper)
    monkeypatch.setitem(_builtin_scrapers, "plugin2", mock_scraper)

    scrapers = [
        ("plugin1", {"api_url": "https://api1.example.com", "api_key": "secret1"}),
        ("plugin2", {"api_url": "https://api2.example.com", "api_key": "secret2"}),
    ]

    users = list(scrape_users(scrapers))

    assert len(users) == 5  # 3 from plugin1 + 2 from plugin2 (user3 is merged)

    plugin1_users = [u for u in users if u.matching_id.name == "plugin1"]
    plugin2_users = [u for u in users if u.matching_id.name == "plugin2"]
    assert len(plugin1_users) == 3
    assert len(plugin2_users) == 2


def test_scrape_users_invalid_scraper():
    scrapers = [("invalid_scraper", {"api_url": "https://api.example.com"})]

    with pytest.raises(ValueError, match="Invalid user scraper"):
        list(scrape_users(scrapers))


@patch("sarc.core.scraping.users.get_user_scraper")
def test_scrape_users_user_matching(mock_get_scraper):
    """Test that users with matching IDs are properly merged."""

    # Create a mock scraper that returns users with known matches
    class MatchingMockScraper(MockUserScraper):
        def parse_user_data(
            self, config: MockConfig, data: bytes
        ) -> Iterable[UserMatch]:
            # Return different users based on the config (which plugin)
            if "api1" in config.api_url:
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

    users = list(scrape_users(scrapers))

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
