"""Tests for Manual user scrapers."""

import pytest

from sarc.core.scraping.users import MatchID, UserMatch
from sarc.users.manual import ConfigMatchID, ManualUserConfig, ManualUserScraper
from tests.unittests.core.test_users_scraping import UserPluginTester


class TestManualUserScraper(UserPluginTester):
    plugin = ManualUserScraper()

    raw_config = {
        "id_pairs": {
            "user1": [
                {"name": "drac", "mid": "cci-001"},
                {"name": "mila_ldap", "mid": "user1@mila.quebec"},
            ],
            "user2": [{"name": "drac", "mid": "cci-002"}],
        }
    }

    parsed_config = ManualUserConfig(
        id_pairs={
            "user1": [
                ConfigMatchID(name="drac", mid="cci-001"),
                ConfigMatchID(name="mila_ldap", mid="user1@mila.quebec"),
            ],
            "user2": [ConfigMatchID(name="drac", mid="cci-002")],
        }
    )

    def test_fetch_data(self):
        data = self.plugin.get_user_data(self.parsed_config)
        assert data == b""

    @pytest.mark.parametrize(
        "config,expected",
        [
            pytest.param(ManualUserConfig(id_pairs={}), [], id="empty"),
            pytest.param(
                ManualUserConfig(
                    id_pairs={
                        "user1": [
                            MatchID(name="drac", mid="cci-001"),
                            MatchID(name="mila_ldap", mid="user1@mila.quebec"),
                        ],
                        "user2": [MatchID(name="drac", mid="cci-002")],
                    }
                ),
                [
                    UserMatch(
                        matching_id=MatchID(name="manual", mid="user1"),
                        known_matches={
                            MatchID(name="drac", mid="cci-001"),
                            MatchID(name="mila_ldap", mid="user1@mila.quebec"),
                        },
                    ),
                    UserMatch(
                        matching_id=MatchID(name="manual", mid="user2"),
                        known_matches={MatchID(name="drac", mid="cci-002")},
                    ),
                ],
            ),
        ],
    )
    def test_parse_data(self, config, expected):
        data = list(self.plugin.parse_user_data(config, b""))
        assert data == expected
