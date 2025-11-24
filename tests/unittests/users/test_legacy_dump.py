"""Tests for LegacyDump user scraper."""

from pathlib import Path

from sarc.users.legacy_dump import LegacyDumpConfig, LegacyDumpScraper
from tests.unittests.core.test_users_scraping import UserPluginTester


class TestLegacyDumpScraper(UserPluginTester):
    plugin = LegacyDumpScraper()

    raw_config = {"json_file_path": "/path/to/userdump.json"}
    parsed_config = LegacyDumpConfig(json_file_path=Path("/path/to/userdump.json"))

    def test_fetch_data(self, data_regression):
        """Test that get_user_data returns empty bytes as expected."""
        config = LegacyDumpConfig(
            json_file_path=Path(__file__).parent / "inputs" / "userdump_test.json"
        )
        data = self.plugin.get_user_data(config)
        assert data == b""

    def test_parse_data(self, data_regression):
        """Test parsing of consolidated legacy dump data file with all test cases."""
        config = LegacyDumpConfig(
            json_file_path=Path(__file__).parent / "inputs" / "userdump_test.json"
        )

        data = list(
            d.model_dump(mode="json") for d in self.plugin.parse_user_data(config, b"")
        )
        data_regression.check(data)
