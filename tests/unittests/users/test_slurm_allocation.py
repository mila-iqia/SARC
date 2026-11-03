"""Tests for the Slurm allocation members scraper."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlmodel import select

from sarc.cache import Cache
from sarc.db.users import UserDB
from sarc.scraping.users import fetch_users, parse_ce, update_user
from sarc.users.slurm_allocation import (
    SlurmAllocationMembersConfig,
    SlurmAllocationMembersScraper,
)
from tests.unittests.core.test_users_scraping import UserPluginTester


class TestSlurmAllocationMembersScraper(UserPluginTester):
    plugin = SlurmAllocationMembersScraper()

    data = (
        Path(__file__).parent / "inputs" / "slurm_allocation_members.csv"
    ).read_text()
    raw_config = {"csv": data}
    parsed_config = SlurmAllocationMembersConfig(csv=data, domain="drac")

    def test_fetch_data(self, data_regression):
        data = self.plugin.get_user_data(self.parsed_config)
        data_regression.check(data.decode(), basename="test_fetch_members")

    @pytest.mark.usefixtures("read_only_db")
    def test_parse_data(self, data_regression):
        raw_data = self.plugin.get_user_data(self.parsed_config)
        data = list(
            d.model_dump(mode="json")
            for d in self.plugin.parse_user_data(
                raw_data, datetime(year=2024, month=1, day=1, tzinfo=UTC)
            )
        )
        data_regression.check(data, basename="test_parse_members")


@pytest.mark.usefixtures("enabled_cache")
def test_parse_with_drac_members(empty_read_write_db):
    """Members also known by drac_member are merged with them, others are created."""
    inputs = Path(__file__).parent / "inputs"
    members_csv = (
        "Slurm_Username,Nom,Email,Allocations,PI\n"
        "johndoe,John Doe,,aip-johndoe,oui\n"
        "newuser,New User,,aip-johndoe,\n"
    )
    # slurm_allocation_members first: its missing emails are filled by drac_member
    fetch_users(
        [
            ("slurm_allocation_members", {"csv": members_csv}),
            (
                "drac_member",
                {
                    "csv": (inputs / "drac_members.csv").read_text(),
                    "csv_date": "2025-12-31",
                },
            ),
        ]
    )
    for ce in Cache("users").read_from(datetime.now(UTC) - timedelta(hours=1)):
        for um in parse_ce(ce):
            update_user(empty_read_write_db, um)
    empty_read_write_db.flush()

    users = {u.display_name: u for u in empty_read_write_db.exec(select(UserDB)).all()}
    assert set(users) == {"John Doe", "Jane Smith", "New User"}

    john = users["John Doe"]
    assert john.email == "john.doe@example.com"
    assert dict(john.matching_ids) == {
        "drac_member": "abc-123",
        "slurm_allocation_members": "johndoe",
    }

    new = users["New User"]
    assert new.email == ""
    assert dict(new.matching_ids) == {"slurm_allocation_members": "newuser"}
    assert new.associated_accounts["drac"].get_value() == "newuser"
