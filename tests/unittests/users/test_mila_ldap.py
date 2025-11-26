"""Tests for Mila LDAP user scrapers."""

import json
from pathlib import Path

import pytest

from sarc.users.mila_ldap import MilaLDAPConfig, MilaLDAPScraper
from tests.common.sarc_mocks import fake_raw_ldap_data
from tests.unittests.core.test_users_scraping import UserPluginTester


class TestMilaLDAPScraper(UserPluginTester):
    plugin = MilaLDAPScraper()

    raw_config = {
        "service_uri": "ldaps://ldap.example.com:636",
        "private_key_file": "/path/to/private.key",
        "certificate_file": "/path/to/certificate.crt",
    }
    parsed_config = MilaLDAPConfig(
        service_uri="ldaps://ldap.example.com:636",
        private_key_file=Path("/path/to/private.key"),
        certificate_file=Path("/path/to/certificate.crt"),
    )

    def test_fetch_data(self, data_regression, patch_return_values):
        patch_return_values(
            {
                "sarc.users.mila_ldap._query_ldap": fake_raw_ldap_data(
                    10, hardcoded_values_by_user={3: {"suspended": ["true"]}}
                ),
            }
        )
        data = self.plugin.get_user_data(self.parsed_config)
        # Uncomment to update the input data for the parse test
        # with open(Path(__file__).parent / "inputs" / "mila_ldap1.cache", "wb") as f:
        #    f.write(data)
        data_regression.check(json.loads(data.decode()))

    @pytest.mark.freeze_time("2023-01-01")
    @pytest.mark.parametrize("raw_file", ["mila_ldap1.cache"])
    def test_parse_data(self, raw_file, data_regression):
        with open(Path(__file__).parent / "inputs" / raw_file, "rb") as f:
            raw_data = f.read()

        data = list(
            d.model_dump(mode="json") for d in self.plugin.parse_user_data(raw_data)
        )
        data_regression.check(data)
