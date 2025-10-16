"""Tests for MyMila user scrapers."""

import json
from pathlib import Path

import pytest

from sarc.users.mymila import MyMilaConfig, MyMilaScraper
from tests.common.sarc_mocks import fake_mymila_data
from tests.unittests.core.test_users_scraping import UserPluginTester


class TestMyMilaScraper(UserPluginTester):
    plugin = MyMilaScraper()

    raw_config = {
        "tenant_id": "test-tenant-id",
        "client_id": "test-client-id",
        "client_secret": "test-client-secret",
        "sql_endpoint": "test-sql-endpoint",
        "database": "test-database",
    }
    parsed_config = MyMilaConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        sql_endpoint="test-sql-endpoint",
        database="test-database",
    )

    @pytest.mark.freeze_time("2024-10-01")
    def test_fetch_data(self, data_regression, patch_return_values):
        patch_return_values(
            {
                "sarc.users.mymila._query_mymila": fake_mymila_data(10),
            }
        )
        data = self.plugin.get_user_data(self.parsed_config)
        # Uncomment to update the input data for the parse test
        # with open(Path(__file__).parent / "inputs" / "mymila1.cache", "wb") as f:
        #    f.write(data)
        data_regression.check(json.loads(data.decode()))

    @pytest.mark.freeze_time("2024-10-01")
    @pytest.mark.parametrize("raw_file", ["mymila1.cache"])
    def test_parse_data(self, raw_file, data_regression):
        with open(Path(__file__).parent / "inputs" / raw_file, "rb") as f:
            raw_data = f.read()

        data = list(
            d.model_dump(mode="json") for d in self.plugin.parse_user_data(raw_data)
        )
        data_regression.check(data)
