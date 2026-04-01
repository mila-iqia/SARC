"""Tests for UserMap in sarc.core.scraping.jobs."""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import UUID

import pytest

from sarc.client.job import SlurmJob
from sarc.core.models.users import Credentials, UserData
from sarc.core.scraping.jobs import UserMap


def _make_user(uuid: str, accounts: dict[str, str] | None = None) -> UserData:
    """Create a minimal UserData with associated_accounts."""
    user = UserData(
        uuid=UUID(uuid),
        display_name="Test User",
        email="test@example.com",
        matching_ids={},
    )
    if accounts:
        for domain, username in accounts.items():
            creds = Credentials()
            creds.insert(username)
            user.associated_accounts[domain] = creds
    return user


def _make_user_with_expired_creds(uuid: str, domain: str, username: str) -> UserData:
    """Create a UserData with an expired credential (end date in the past)."""
    user = UserData(
        uuid=UUID(uuid),
        display_name="Expired User",
        email="expired@example.com",
        matching_ids={},
    )
    creds = Credentials()
    creds.insert(
        username,
        start=datetime(2020, 1, 1, tzinfo=UTC),
        end=datetime(2021, 1, 1, tzinfo=UTC),
    )
    user.associated_accounts[domain] = creds
    return user


def _make_job(cluster_name: str = "mila", user: str = "testuser", **kwargs) -> SlurmJob:
    """Create a minimal SlurmJob for testing."""
    now = datetime(2023, 6, 15, 12, 0, 0, tzinfo=UTC)
    defaults = {
        "cluster_name": cluster_name,
        "user": user,
        "account": "default",
        "job_id": 1,
        "name": "test_job",
        "group": "testgroup",
        "job_state": "COMPLETED",
        "exit_code": 0,
        "signal": None,
        "partition": "main",
        "nodes": ["node1"],
        "work_dir": "/tmp/test",
        "constraints": "",
        "priority": 100,
        "qos": "normal",
        "allocated": {"billing": 1, "cpu": 4, "gres_gpu": 1, "mem": 8192, "node": 1},
        "requested": {"billing": 1, "cpu": 4, "gres_gpu": 1, "mem": 8192, "node": 1},
        "submit_time": now,
        "start_time": now,
        "end_time": now,
        "elapsed_time": 3600,
        "time_limit": 7200,
    }
    defaults.update(kwargs)
    return SlurmJob(**defaults)


@dataclass
class FakeClusterConfig:
    name: str
    user_domain: str | None


@dataclass
class FakeConfig:
    clusters: dict[str, FakeClusterConfig]


def _patch_config_and_users(clusters: dict[str, str | None], users: list[UserData]):
    """Return stacked patches for config() and get_users().

    clusters: dict mapping cluster_name -> user_domain (or None)
    """
    fake_clusters = {
        name: FakeClusterConfig(name=name, user_domain=domain)
        for name, domain in clusters.items()
    }
    fake_cfg = FakeConfig(clusters=fake_clusters)

    return (
        patch("sarc.core.scraping.jobs.config", return_value=fake_cfg),
        patch("sarc.core.scraping.jobs.get_users", return_value=users),
    )


# ── Basic matching ──────────────────────────────────────────────────


class TestUserMapInit:
    def test_basic_mapping(self):
        user = _make_user("1f9b04e5-0ec4-4577-9196-2b03d254e344", {"mila": "jdoe"})
        p_cfg, p_users = _patch_config_and_users({"mila": "mila"}, [user])
        with p_cfg, p_users:
            um = UserMap()
        assert ("mila", "jdoe") in um._users
        assert um._cluster_domain == {"mila": "mila"}

    def test_multiple_domains(self):
        user = _make_user(
            "1f9b04e5-0ec4-4577-9196-2b03d254e344",
            {"mila": "jdoe_mila", "drac": "jdoe_drac"},
        )
        p_cfg, p_users = _patch_config_and_users(
            {"mila": "mila", "narval": "drac"}, [user]
        )
        with p_cfg, p_users:
            um = UserMap()
        assert ("mila", "jdoe_mila") in um._users
        assert ("drac", "jdoe_drac") in um._users
        assert um._cluster_domain == {"mila": "mila", "narval": "drac"}

    def test_no_user_domain_logs_warning(self, caplog):
        p_cfg, p_users = _patch_config_and_users({"raisin": None}, [])
        with p_cfg, p_users, caplog.at_level(logging.WARNING):
            um = UserMap()
        assert "raisin" not in um._cluster_domain
        assert any(
            "No user domain for cluster raisin" in r.message for r in caplog.records
        )

    def test_expired_credentials_still_indexed(self):
        """Expired credentials are indexed in UserMap (temporal check is in solve_user)."""
        user = _make_user_with_expired_creds(
            "1f9b04e5-0ec4-4577-9196-2b03d254e344", "mila", "expired_user"
        )
        p_cfg, p_users = _patch_config_and_users({"mila": "mila"}, [user])
        with p_cfg, p_users:
            um = UserMap()
        assert ("mila", "expired_user") in um._users

    def test_duplicate_credentials_excluded(self, caplog):
        user1 = _make_user(
            "1f9b04e5-0ec4-4577-9196-2b03d254e344", {"mila": "same_user"}
        )
        user2 = _make_user(
            "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4", {"mila": "same_user"}
        )
        p_cfg, p_users = _patch_config_and_users({"mila": "mila"}, [user1, user2])
        with p_cfg, p_users, caplog.at_level(logging.INFO):
            um = UserMap()
        assert ("mila", "same_user") not in um._users
        assert any(
            "Duplicated user for credential mila / same_user" in r.message
            for r in caplog.records
        )
        assert any("1 duplicated" in r.message for r in caplog.records)

    def test_no_users(self, caplog):
        p_cfg, p_users = _patch_config_and_users({"mila": "mila"}, [])
        with p_cfg, p_users, caplog.at_level(logging.INFO):
            um = UserMap()
        assert len(um._users) == 0
        assert any("0 user(s)" in r.message for r in caplog.records)

    def test_user_without_accounts(self):
        user = _make_user("1f9b04e5-0ec4-4577-9196-2b03d254e344", accounts=None)
        p_cfg, p_users = _patch_config_and_users({"mila": "mila"}, [user])
        with p_cfg, p_users:
            um = UserMap()
        assert len(um._users) == 0


# ── solve_user ──────────────────────────────────────────────────────


class TestSolveUser:
    @pytest.fixture
    def user_map(self):
        user = _make_user("1f9b04e5-0ec4-4577-9196-2b03d254e344", {"mila": "jdoe"})
        p_cfg, p_users = _patch_config_and_users(
            {"mila": "mila", "narval": "drac"}, [user]
        )
        with p_cfg, p_users:
            return UserMap()

    def test_links_matching_job(self, user_map):
        job = _make_job(cluster_name="mila", user="jdoe")
        assert user_map.solve_user(job) is True
        assert job.user_uuid == UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344")

    def test_already_set_uuid_not_overwritten(self, user_map):
        existing_uuid = UUID("5a8b9e7f-afcc-4ced-b596-44fcdb3a0cff")
        job = _make_job(cluster_name="mila", user="jdoe", user_uuid=existing_uuid)
        assert user_map.solve_user(job) is False
        assert job.user_uuid == existing_uuid

    def test_unknown_cluster(self, user_map):
        job = _make_job(cluster_name="unknown_cluster", user="jdoe")
        assert user_map.solve_user(job) is False
        assert job.user_uuid is None

    def test_cluster_without_domain(self):
        user = _make_user("1f9b04e5-0ec4-4577-9196-2b03d254e344", {"mila": "jdoe"})
        p_cfg, p_users = _patch_config_and_users({"raisin": None}, [user])
        with p_cfg, p_users:
            um = UserMap()
        job = _make_job(cluster_name="raisin", user="jdoe")
        assert um.solve_user(job) is False
        assert job.user_uuid is None

    def test_unknown_user(self, user_map):
        job = _make_job(cluster_name="mila", user="unknown_user")
        assert user_map.solve_user(job) is False
        assert job.user_uuid is None

    def test_wrong_domain(self, user_map):
        # User has mila credential, but job is on narval (drac domain)
        job = _make_job(cluster_name="narval", user="jdoe")
        assert user_map.solve_user(job) is False
        assert job.user_uuid is None

    def test_expired_credential_not_matched(self):
        """Job submitted outside credential validity period is not matched."""
        user = _make_user_with_expired_creds(
            "1f9b04e5-0ec4-4577-9196-2b03d254e344", "mila", "expired_user"
        )
        p_cfg, p_users = _patch_config_and_users({"mila": "mila"}, [user])
        with p_cfg, p_users:
            um = UserMap()
        # Job submitted in 2023, but credential expired in 2021
        job = _make_job(cluster_name="mila", user="expired_user")
        assert um.solve_user(job) is False
        assert job.user_uuid is None

    def test_multiple_jobs(self):
        user_mila = _make_user(
            "1f9b04e5-0ec4-4577-9196-2b03d254e344",
            {"mila": "alice", "drac": "alice_drac"},
        )
        user_drac = _make_user(
            "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4", {"drac": "bob_drac"}
        )
        p_cfg, p_users = _patch_config_and_users(
            {"mila": "mila", "narval": "drac"}, [user_mila, user_drac]
        )
        with p_cfg, p_users:
            um = UserMap()

        job_mila = _make_job(cluster_name="mila", user="alice", job_id=1)
        job_narval_alice = _make_job(cluster_name="narval", user="alice_drac", job_id=2)
        job_narval_bob = _make_job(cluster_name="narval", user="bob_drac", job_id=3)
        job_narval_unknown = _make_job(cluster_name="narval", user="nobody", job_id=4)

        assert um.solve_user(job_mila) is True
        assert job_mila.user_uuid == UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344")

        assert um.solve_user(job_narval_alice) is True
        assert job_narval_alice.user_uuid == UUID(
            "1f9b04e5-0ec4-4577-9196-2b03d254e344"
        )

        assert um.solve_user(job_narval_bob) is True
        assert job_narval_bob.user_uuid == UUID("7ecd3a8a-ab71-499e-b38a-ceacd91a99c4")

        assert um.solve_user(job_narval_unknown) is False
        assert job_narval_unknown.user_uuid is None
