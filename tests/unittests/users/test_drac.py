"""Tests for DRAC user scrapers."""

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from sarc.users.drac import DRACMemberConfig, DRACMemberScraper
from tests.unittests.core.test_users_scraping import UserPluginTester


class TestDRACMemberScraper(UserPluginTester):
    plugin = DRACMemberScraper()

    data = (Path(__file__).parent / "inputs" / "drac_members.csv").read_text()
    raw_config = {"csv": data, "csv_date": "2025-12-31"}
    parsed_config = DRACMemberConfig(csv=data, csv_date=date(2025, 12, 31))

    def test_fetch_data(self, data_regression):
        config = DRACMemberConfig(csv=self.data, csv_date=date(2025, 12, 31))
        data = self.plugin.get_user_data(config)
        # Uncomment to update the input data for the parse test
        # with open(Path(__file__).parent / "inputs" / "drac_member2.cache", "wb") as f:
        #    f.write(data)
        data_regression.check(data.decode(), basename="test_fetch_members")

    @pytest.mark.parametrize("raw_file", ["drac_member1.cache", "drac_member2.cache"])
    def test_parse_data(self, raw_file, data_regression):
        with open(Path(__file__).parent / "inputs" / raw_file, "rb") as f:
            raw_data = f.read()
        data = list(
            d.model_dump()
            for d in self.plugin.parse_user_data(
                raw_data, datetime(year=2024, month=1, day=1, tzinfo=UTC)
            )
        )
        data_regression.check(
            data, basename=f"test_parse_members_{raw_file.removesuffix('.cache')}"
        )
