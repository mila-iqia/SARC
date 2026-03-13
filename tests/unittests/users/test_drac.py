"""Tests for DRAC user scrapers."""

import json
from pathlib import Path

import pytest

from sarc.users.drac import (
    DRACMemberConfig,
    DRACMemberScraper,
    DRACRolesConfig,
    DRACRolesScraper,
)
from tests.unittests.core.test_users_scraping import UserPluginTester


class TestDRACRolesScraper(UserPluginTester):
    plugin = DRACRolesScraper()

    data = (Path(__file__).parent / "inputs" / "drac_roles.csv").read_text()
    raw_config = {"csv": data}
    parsed_config = DRACRolesConfig(csv=data)

    def test_fetch_data(self, data_regression):
        config = DRACMemberConfig(csv=self.data)
        data = self.plugin.get_user_data(config)

        data_regression.check(json.loads(data.decode()), basename="test_fetch_roles")

    @pytest.mark.parametrize(
        "raw_file",
        [
            "drac_role1.cache",
        ],
    )
    def test_parse_data(self, raw_file, data_regression):
        with open(Path(__file__).parent / "inputs" / raw_file, "rb") as f:
            raw_data = f.read()
        data = list(d.model_dump() for d in self.plugin.parse_user_data(raw_data))
        data_regression.check(data, basename="test_parse_roles")


class TestDRACMemberScraper(UserPluginTester):
    plugin = DRACMemberScraper()

    data = (Path(__file__).parent / "inputs" / "drac_members.csv").read_text()
    raw_config = {"csv": data}
    parsed_config = DRACMemberConfig(csv=data)

    def test_fetch_data(self, data_regression):
        config = DRACMemberConfig(csv=self.data)
        data = self.plugin.get_user_data(config)

        data_regression.check(json.loads(data.decode()), basename="test_fetch_members")

    @pytest.mark.parametrize(
        "raw_file",
        [
            "drac_member1.cache",
        ],
    )
    def test_parse_data(self, raw_file, data_regression):
        with open(Path(__file__).parent / "inputs" / raw_file, "rb") as f:
            raw_data = f.read()
        data = list(d.model_dump() for d in self.plugin.parse_user_data(raw_data))
        data_regression.check(data, basename="test_parse_members")
