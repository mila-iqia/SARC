"""Tests for LegacyDump user scraper."""

import json
from pathlib import Path

from sarc.users.legacy_dump import LegacyDumpConfig, LegacyDumpScraper
from tests.unittests.core.test_users_scraping import UserPluginTester


class TestLegacyDumpScraper(UserPluginTester):
    plugin = LegacyDumpScraper()

    data = (Path(__file__).parent / "inputs" / "userdump_test.json").read_text()
    raw_config = {"json": data}
    parsed_config = LegacyDumpConfig(json=data)

    def test_fetch_data(self, data_regression):
        """Test that get_user_data returns empty bytes as expected."""
        config = LegacyDumpConfig(json=self.data)
        data = self.plugin.get_user_data(config)
        records = json.loads(data.decode("utf-8"))
        assert len(records) > 0

    def test_parse_data(self, data_regression):
        json_path = Path(__file__).parent / "inputs" / "userdump_test.json"
        with open(json_path, "r", encoding="utf-8") as f:
            json_data = f.read().encode("utf-8")
        data = list(
            d.model_dump(mode="json") for d in self.plugin.parse_user_data(json_data)
        )
        data_regression.check(data)
