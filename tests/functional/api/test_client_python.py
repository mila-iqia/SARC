from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import requests
import httpx
from pydantic_mongo import PydanticObjectId

from sarc.client.api import SarcApiClient, get_jobs
from sarc.client.job import SlurmJob, SlurmState
from sarc.config import ConfigurationError
from sarc.core.models.users import MemberType


@pytest.fixture
def sarc_client(client):
    """
    Returns a SarcApiClient that uses the FastAPI TestClient session.
    The 'client' fixture comes from tests/functional/api/conftest.py
    and is connected to the app with the database fixtures active.
    """
    return SarcApiClient(remote_url="http://testserver", session=client)


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_cluster_names(sarc_client):
    clusters = sarc_client.get_cluster_names()
    expected = {
        "mila",
        "local",
        "gerudo",
        "raisin",
        "patate",
        "raisin_no_prometheus",
        "hyrule",
        "fromage",
    }
    assert set(clusters) == expected


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_jobs(sarc_client):
    # Test getting jobs for a specific cluster
    job_ids = sarc_client.get_jobs(cluster="raisin")
    assert len(job_ids) > 0

    # Verify we can fetch one of them
    first_job_id = job_ids[0]
    job = sarc_client.get_job(first_job_id)
    assert job.cluster_name == "raisin"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_job_not_found(sarc_client):
    # Generate a random ObjectId that shouldn't exist
    random_oid = str(PydanticObjectId())

    # SarcApiClient uses requests, but TestClient uses httpx.
    # In production, requests.HTTPError is raised.
    # In tests with TestClient, httpx.HTTPStatusError is raised.
    with pytest.raises((requests.HTTPError, httpx.HTTPStatusError)) as excinfo:
        sarc_client.get_job(random_oid)

    # Expecting 404 Not Found for a valid formatted ObjectId that is missing
    # Handling differences between requests and httpx response objects in exception
    if isinstance(excinfo.value, requests.HTTPError):
        assert excinfo.value.response.status_code == 404
    else:
        assert excinfo.value.response.status_code == 404


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_jobs_filters(sarc_client):
    # Test with enum and datetime objects to ensure serialization works
    start_dt = datetime(2023, 1, 1, tzinfo=timezone.utc)

    job_ids = sarc_client.get_jobs(
        cluster="raisin", job_state=SlurmState.COMPLETED, start=start_dt
    )
    assert len(job_ids) > 0
    for jid in job_ids:
        job = sarc_client.get_job(jid)
        assert job.cluster_name == "raisin"
        assert job.job_state == SlurmState.COMPLETED


@pytest.mark.usefixtures("read_only_db_with_users")
def test_count_jobs(sarc_client):
    count = sarc_client.count_jobs(cluster="raisin")
    assert count == 20  # Based on factory data


@pytest.mark.usefixtures("read_only_db_with_users")
def test_query_users_and_get_details(sarc_client):
    # Query users by email
    email = "jdoe@example.com"
    user_ids = sarc_client.query_users(email=email)
    assert len(user_ids) == 1

    # Get user details by ID
    uid = user_ids[0]
    user = sarc_client.get_user_by_id(uid)
    assert user.email == email
    assert user.display_name == "Jane Doe"
    assert user.uuid == uid


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_email(sarc_client):
    email = "jsmith@example.com"
    user = sarc_client.get_user_by_email(email)
    assert user.email == email
    assert user.display_name == "John Smith"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_query_users_filters(sarc_client, freezer):
    # Set time to ensure member_type checks work as expected
    freezer.move_to(datetime(2022, 5, 1, tzinfo=timezone.utc))

    # Query staff members
    staff_ids = sarc_client.query_users(member_type=MemberType.STAFF)
    assert len(staff_ids) > 0

    # Verify retrieved user is indeed staff
    user = sarc_client.get_user_by_id(staff_ids[0])
    assert user.member_type.get_value() == MemberType.STAFF


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_rgu_value_per_gpu(sarc_client, monkeypatch):
    def _gen_fake_rgus():
        return {"A100": 3.21}

    monkeypatch.setattr("sarc.api.v0.get_rgus", _gen_fake_rgus)

    rgus = sarc_client.get_rgu_value_per_gpu()
    assert rgus == {"A100": 3.21}


# --- Initialization & Configuration Tests (migrated from unit tests) ---


def test_init_with_params():
    c = SarcApiClient(remote_url="http://example.com", timeout=60)
    assert c.remote_url == "http://example.com"
    assert c.timeout == 60

    c2 = SarcApiClient("http://example.com/")
    assert c2.remote_url == "http://example.com"


def test_init_from_config():
    # We patch sarc.client.api.config so we don't mess with the global app config
    with patch("sarc.client.api.config") as mock_config:
        mock_config.return_value.api.url = "http://config-url.com/"
        mock_config.return_value.api.timeout = 90

        c = SarcApiClient()
        assert c.remote_url == "http://config-url.com"
        assert c.timeout == 90


def test_init_no_config_raises_error():
    with patch("sarc.client.api.config") as mock_config:
        # Simulate missing api section in config
        mock_config.return_value.api = None
        with pytest.raises(ConfigurationError):
            SarcApiClient()


# --- New Tests for Pagination and Helper Functions ---


@pytest.mark.usefixtures("read_only_db_with_users")
def test_list_jobs_pagination(sarc_client, monkeypatch):
    # Monkeypatch PAGE_SIZE in the API module to test pagination with few items
    import sarc.api.v0

    monkeypatch.setattr(sarc.api.v0, "PAGE_SIZE", 5)

    # We expect 20 jobs in raisin cluster
    jobs_list = sarc_client.list_jobs(cluster="raisin", page=1)

    assert jobs_list.total == 20
    assert jobs_list.per_page == 5
    assert len(jobs_list.jobs) == 5
    assert jobs_list.page == 1

    # Test getting second page
    jobs_list_2 = sarc_client.list_jobs(cluster="raisin", page=2)
    assert jobs_list_2.page == 2
    assert len(jobs_list_2.jobs) == 5
    # Ensure different jobs (simple check)
    assert jobs_list.jobs[0].job_id != jobs_list_2.jobs[0].job_id


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_jobs_helper_pagination(client, monkeypatch):
    """
    Test the top-level get_jobs function which should iterate over pages.
    We need to patch SarcApiClient usage inside the function to use our test client session.
    """
    import sarc.client.api
    import sarc.api.v0

    # 1. Patch PAGE_SIZE to force pagination
    monkeypatch.setattr(sarc.api.v0, "PAGE_SIZE", 5)

    # 2. Patch SarcApiClient in the module to return our test-connected client
    # The get_jobs function instantiates SarcApiClient(), so we patch the class.
    # But we need it to behave like our 'sarc_client' fixture which has the session.

    # A cleaner way is to patch the session instantiation inside __init__ or pass the session somehow.
    # Since get_jobs instantiates SarcApiClient() with no args, it tries to read config.
    # We can patch SarcApiClient to return a mock or our configured instance.

    # Let's create a factory that returns a client with our test session
    def client_factory(*args, **kwargs):
        return SarcApiClient(remote_url="http://testserver", session=client)

    monkeypatch.setattr(sarc.client.api, "SarcApiClient", client_factory)

    # Call the helper function
    all_jobs = get_jobs(cluster="raisin")

    # Verify we got all 20 jobs despite PAGE_SIZE=5
    assert len(all_jobs) == 20
    assert isinstance(all_jobs[0], SlurmJob)


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_users_helper_pagination(client, monkeypatch):
    """
    Test the top-level get_users function which should iterate over pages.
    """
    import sarc.client.api
    import sarc.api.v0
    from sarc.client.api import get_users
    from sarc.core.models.users import UserData

    # 1. Patch PAGE_SIZE to force pagination (total users is 10)
    monkeypatch.setattr(sarc.api.v0, "PAGE_SIZE", 3)

    # 2. Patch SarcApiClient to use our test session
    def client_factory(*args, **kwargs):
        return SarcApiClient(remote_url="http://testserver", session=client)

    monkeypatch.setattr(sarc.client.api, "SarcApiClient", client_factory)

    # Call the helper function
    all_users = get_users()

    # Verify we got all 10 users despite PAGE_SIZE=3
    assert len(all_users) == 10
    assert isinstance(all_users[0], UserData)

    # Verify sorting by email (default)
    emails = [u.email for u in all_users]
    assert emails == sorted(emails)

    # Test with filter
    doe_users = get_users(display_name="Doe")
    assert len(doe_users) == 1
    assert doe_users[0].display_name == "Jane Doe"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_load_job_series_rest(client, monkeypatch):
    """
    Test load_job_series via REST API.
    """
    import pandas as pd
    from sarc.client.api import load_job_series
    import sarc.client.api

    # Patch SarcApiClient to use our test session
    def client_factory(*args, **kwargs):
        return SarcApiClient(remote_url="http://testserver", session=client)

    monkeypatch.setattr(sarc.client.api, "SarcApiClient", client_factory)

    # 1. Test basic loading
    df = load_job_series(cluster="raisin")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 20
    assert "job_id" in df.columns
    # Check user merging
    assert "user.email" in df.columns
    # Since we have users in DB, some should be matched
    # (assuming job user names match user accounts in fixtures)

    # 2. Test with filtering
    df_filtered = load_job_series(cluster="raisin", job_state="COMPLETED")
    assert len(df_filtered) > 0
    assert all(df_filtered["job_state"] == "COMPLETED")

    # 3. Test with fields selection/renaming
    # Note: load_job_series currently applies fields filtering BEFORE user merge,
    # so we cannot select user.* fields here if they are not in the job data.
    # Also, cluster_name MUST be present/renamed because it's used for the merge logic.
    df_fields = load_job_series(
        cluster="raisin",
        fields={"job_id": "ID", "user": "USERNAME", "cluster_name": "CLUSTER"},
    )
    assert "ID" in df_fields.columns
    assert "USERNAME" in df_fields.columns
    assert "CLUSTER" in df_fields.columns
    assert "job_id" not in df_fields.columns
