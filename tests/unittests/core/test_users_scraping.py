"""Tests for the user scraping plugin system."""

from collections.abc import Iterable
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from sarc.core.scraping.users import (
    MatchID,
    UserMatch,
    UserScraper,
    get_user_scraper,
    scrape_users,
    update_user_match,
)


@dataclass
class MockConfig:
    """Mock configuration for testing."""

    api_url: str
    api_key: str


class MockUserScraper(UserScraper[MockConfig]):
    """Mock user scraper for testing the plugin interface."""

    config_type = MockConfig

    def get_user_data(self, config: MockConfig) -> bytes:
        """Return mock user data."""
        return f"mock_data_from_{config.api_url}".encode()

    def parse_user_data(self, config: MockConfig, data: bytes) -> Iterable[UserMatch]:
        """Parse mock user data and return UserMatch objects."""
        # Create some mock users based on the config
        users = []

        # User 1: Complete profile
        user1 = UserMatch(
            display_name="John Doe",
            email="john.doe@example.com",
            matching_id=MatchID(name="mock_plugin", mid="user1"),
        )
        user1.github_username.insert("johndoe")
        user1.google_scholar_profile.insert("scholar.google.com/citations?user=abc123")
        users.append(user1)

        # User 2: Partial profile
        user2 = UserMatch(
            display_name="Jane Smith",
            email=None,  # Missing email
            matching_id=MatchID(name="mock_plugin", mid="user2"),
        )
        users.append(user2)

        # User 3: With known matches
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


# Test functions for MatchID
def test_match_id_creation():
    """Test creating a MatchID."""
    mid = MatchID(name="test_plugin", mid="user123")
    assert mid.name == "test_plugin"
    assert mid.mid == "user123"


def test_match_id_hash():
    """Test that MatchID objects are hashable."""
    mid1 = MatchID(name="test_plugin", mid="user123")
    mid2 = MatchID(name="test_plugin", mid="user123")
    mid3 = MatchID(name="test_plugin", mid="user456")

    assert hash(mid1) == hash(mid2)
    assert hash(mid1) != hash(mid3)


def test_match_id_equality():
    """Test MatchID equality."""
    mid1 = MatchID(name="test_plugin", mid="user123")
    mid2 = MatchID(name="test_plugin", mid="user123")
    mid3 = MatchID(name="test_plugin", mid="user456")

    assert mid1 == mid2
    assert mid1 != mid3
    assert mid1 != "not_a_match_id"


# Test functions for UserMatch
def test_user_match_creation():
    """Test creating a UserMatch with minimal data."""
    mid = MatchID(name="test_plugin", mid="user123")
    user = UserMatch(matching_id=mid)

    assert user.matching_id == mid
    assert user.display_name is None
    assert user.email is None
    assert user.known_matches == set()
    assert user.associated_accounts == {}


def test_user_match_with_data():
    """Test creating a UserMatch with complete data."""
    mid = MatchID(name="test_plugin", mid="user123")
    user = UserMatch(
        display_name="Test User",
        email="test@example.com",
        matching_id=mid,
        known_matches={MatchID(name="other_plugin", mid="user123")},
    )

    assert user.display_name == "Test User"
    assert user.email == "test@example.com"
    assert user.matching_id == mid
    assert len(user.known_matches) == 1


def test_user_match_equality():
    """Test UserMatch equality based on matching_id."""
    mid1 = MatchID(name="test_plugin", mid="user123")
    mid2 = MatchID(name="test_plugin", mid="user456")

    user1 = UserMatch(matching_id=mid1)
    user2 = UserMatch(matching_id=mid1)
    user3 = UserMatch(matching_id=mid2)

    assert user1 == user2
    assert user1 != user3


def test_user_match_hash():
    """Test that UserMatch objects are hashable."""
    mid = MatchID(name="test_plugin", mid="user123")
    user1 = UserMatch(matching_id=mid)
    user2 = UserMatch(matching_id=mid)

    assert hash(user1) == hash(user2)


# Test functions for UserScraper protocol
def test_mock_scraper_protocol_compliance():
    """Test that MockUserScraper implements the UserScraper protocol."""
    scraper = MockUserScraper()

    # Test config validation
    config_data = {"api_url": "https://api.example.com", "api_key": "secret"}
    config = scraper.validate_config(config_data)
    assert isinstance(config, MockConfig)
    assert config.api_url == "https://api.example.com"
    assert config.api_key == "secret"

    # Test data retrieval
    data = scraper.get_user_data(config)
    assert isinstance(data, bytes)
    assert data == b"mock_data_from_https://api.example.com"

    # Test data parsing
    users = list(scraper.parse_user_data(config, data))
    assert len(users) == 3
    assert all(isinstance(user, UserMatch) for user in users)


# Test functions for update_user_match
def test_update_user_match_fill_missing_data():
    """Test updating a UserMatch with missing data."""
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
    """Test that existing data is preserved when updating."""
    base_user = UserMatch(
        display_name="John Doe",
        email="john@example.com",
        matching_id=MatchID(name="plugin1", mid="user1"),
    )

    update_user = UserMatch(
        display_name="Different Name",  # Should not override
        email="different@example.com",  # Should not override
        matching_id=MatchID(name="plugin2", mid="user1"),
    )

    update_user_match(value=base_user, update=update_user)

    assert base_user.display_name == "John Doe"  # Preserved
    assert base_user.email == "john@example.com"  # Preserved


def test_update_user_match_merge_known_matches():
    """Test merging known matches."""
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
    """Test merging ValidField objects."""
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


# Test functions for get_user_scraper
@patch("sarc.core.scraping.users._builtin_scrapers")
@patch("sarc.core.scraping.users._user_scrapers")
def test_get_user_scraper_builtin(mock_user_scrapers, mock_builtin_scrapers):
    """Test getting a builtin scraper."""
    mock_scraper = MockUserScraper()
    mock_builtin_scrapers.__getitem__.return_value = mock_scraper

    result = get_user_scraper("test_scraper")
    assert result == mock_scraper
    mock_builtin_scrapers.__getitem__.assert_called_once_with("test_scraper")


@patch("sarc.core.scraping.users._builtin_scrapers")
@patch("sarc.core.scraping.users._user_scrapers")
def test_get_user_scraper_plugin(mock_user_scrapers, mock_builtin_scrapers):
    """Test getting a plugin scraper."""
    mock_builtin_scrapers.__getitem__.side_effect = KeyError("not found")

    mock_entry_point = MagicMock()
    mock_entry_point.load.return_value = MockUserScraper()
    mock_user_scrapers.__getitem__.return_value = mock_entry_point

    result = get_user_scraper("test_plugin")
    assert isinstance(result, MockUserScraper)
    mock_user_scrapers.__getitem__.assert_called_once_with("test_plugin")


@patch("sarc.core.scraping.users._builtin_scrapers")
@patch("sarc.core.scraping.users._user_scrapers")
def test_get_user_scraper_not_found(mock_user_scrapers, mock_builtin_scrapers):
    """Test getting a non-existent scraper raises KeyError."""
    mock_builtin_scrapers.__getitem__.side_effect = KeyError("not found")
    mock_user_scrapers.__getitem__.side_effect = KeyError("not found")

    with pytest.raises(KeyError):
        get_user_scraper("non_existent_scraper")


# Test functions for scrape_users
@patch("sarc.core.scraping.users.get_user_scraper")
def test_scrape_users_single_plugin(mock_get_scraper):
    """Test scraping users with a single plugin."""
    mock_scraper = MockUserScraper()
    mock_get_scraper.return_value = mock_scraper

    scrapers = [
        ("mock_plugin", {"api_url": "https://api.example.com", "api_key": "secret"})
    ]

    users = list(scrape_users(scrapers))

    assert len(users) == 3
    assert all(isinstance(user, UserMatch) for user in users)

    # Check that the first plugin name was assigned to matching_id.name
    for user in users:
        assert user.matching_id.name == "mock_plugin"


@patch("sarc.core.scraping.users.get_user_scraper")
def test_scrape_users_multiple_plugins(mock_get_scraper):
    """Test scraping users with multiple plugins."""
    mock_scraper = MockUserScraper()
    mock_get_scraper.return_value = mock_scraper

    scrapers = [
        ("plugin1", {"api_url": "https://api1.example.com", "api_key": "secret1"}),
        ("plugin2", {"api_url": "https://api2.example.com", "api_key": "secret2"}),
    ]

    users = list(scrape_users(scrapers))

    # Should get users from both plugins, but user3 from plugin1 has known_matches
    # that reference plugin2, so it gets merged, resulting in 5 users total
    assert len(users) == 5  # 3 from plugin1 + 2 from plugin2 (user3 is merged)

    # Check that plugin names are correctly assigned
    plugin1_users = [u for u in users if u.matching_id.name == "plugin1"]
    plugin2_users = [u for u in users if u.matching_id.name == "plugin2"]
    assert len(plugin1_users) == 3
    assert (
        len(plugin2_users) == 2
    )  # user3 from plugin2 gets merged into plugin1's user3


@patch("sarc.core.scraping.users.get_user_scraper")
def test_scrape_users_invalid_scraper(mock_get_scraper):
    """Test scraping with an invalid scraper name."""
    mock_get_scraper.side_effect = KeyError("scraper not found")

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
            if "api1" in config.api_url:  # plugin1
                user1 = UserMatch(
                    display_name="John Doe",
                    email="john@example.com",
                    matching_id=MatchID(name="plugin1", mid="user1"),
                )
                return [user1]
            else:  # plugin2
                user2 = UserMatch(
                    display_name=None,  # Missing name
                    email="john@example.com",  # Same email as user1
                    matching_id=MatchID(
                        name="plugin2", mid="user1"
                    ),  # Same mid as user1
                    known_matches={
                        MatchID(name="plugin1", mid="user1")
                    },  # References user1
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


# Test functions for plugin interface
def test_plugin_interface_requirements():
    """Test that a plugin must implement the required interface."""
    # This should work
    scraper = MockUserScraper()
    assert hasattr(scraper, "config_type")
    assert hasattr(scraper, "validate_config")
    assert hasattr(scraper, "get_user_data")
    assert hasattr(scraper, "parse_user_data")


def test_plugin_config_validation():
    """Test plugin config validation."""
    scraper = MockUserScraper()

    # Valid config
    valid_config = {"api_url": "https://api.example.com", "api_key": "secret"}
    config = scraper.validate_config(valid_config)
    assert isinstance(config, MockConfig)

    # Invalid config should raise validation error
    invalid_config = {"api_url": "https://api.example.com"}  # Missing api_key
    with pytest.raises(Exception):  # Pydantic validation error
        scraper.validate_config(invalid_config)


def test_plugin_data_retrieval():
    """Test plugin data retrieval."""
    scraper = MockUserScraper()
    config = MockConfig(api_url="https://api.example.com", api_key="secret")

    data = scraper.get_user_data(config)
    assert isinstance(data, bytes)
    assert len(data) > 0


def test_plugin_data_parsing():
    """Test plugin data parsing."""
    scraper = MockUserScraper()
    config = MockConfig(api_url="https://api.example.com", api_key="secret")
    data = b"mock_data"

    users = list(scraper.parse_user_data(config, data))
    assert len(users) > 0
    assert all(isinstance(user, UserMatch) for user in users)


# Test functions for unified plugin testing
def test_plugin_lifecycle():
    """Test complete plugin lifecycle."""
    scraper = MockUserScraper()

    # 1. Config validation
    config_data = {"api_url": "https://api.example.com", "api_key": "secret"}
    config = scraper.validate_config(config_data)
    assert isinstance(config, scraper.config_type)

    # 2. Data retrieval
    data = scraper.get_user_data(config)
    assert isinstance(data, bytes)

    # 3. Data parsing
    users = list(scraper.parse_user_data(config, data))
    assert len(users) > 0
    assert all(isinstance(user, UserMatch) for user in users)

    # 4. Verify user data structure
    for user in users:
        assert isinstance(user.matching_id, MatchID)
        assert user.matching_id.name == "mock_plugin"  # Set by our mock


def test_plugin_error_handling():
    """Test plugin error handling."""

    class ErrorMockScraper(MockUserScraper):
        def get_user_data(self, config: MockConfig) -> bytes:
            raise Exception("API connection failed")

    scraper = ErrorMockScraper()
    config = MockConfig(api_url="https://api.example.com", api_key="secret")

    with pytest.raises(Exception, match="API connection failed"):
        scraper.get_user_data(config)


def test_plugin_data_consistency():
    """Test that plugin data is consistent across calls."""
    scraper = MockUserScraper()
    config = MockConfig(api_url="https://api.example.com", api_key="secret")

    # Get data twice
    data1 = scraper.get_user_data(config)
    data2 = scraper.get_user_data(config)

    # Should be consistent
    assert data1 == data2

    # Parse data twice
    users1 = list(scraper.parse_user_data(config, data1))
    users2 = list(scraper.parse_user_data(config, data2))

    # Should be consistent
    assert len(users1) == len(users2)
    for u1, u2 in zip(users1, users2):
        assert u1.matching_id == u2.matching_id
        assert u1.display_name == u2.display_name
        assert u1.email == u2.email
