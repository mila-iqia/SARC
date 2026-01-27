from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch

import httpx
import pytest
import requests
from pydantic_mongo import PydanticObjectId

from sarc.client.api import SarcApiClient
from sarc.client.job import SlurmJob, SlurmState
from sarc.config import ConfigurationError, UTC
from sarc.core.models.users import MemberType


@pytest.fixture
def sarc_client(client):
    """
    Returns a SarcApiClient that uses the FastAPI TestClient session.
    The 'client' fixture comes from tests/functional/api/conftest.py
    and is connected to the app with the database fixtures active.
    """
    return SarcApiClient(remote_url="http://testserver", session=client)


class TestSarcApiClientInitialization:
    def test_init_with_params(self):
        c = SarcApiClient(remote_url="http://example.com", timeout=60)
        assert c.remote_url == "http://example.com"
        assert c.timeout == 60

        c2 = SarcApiClient("http://example.com/")
        assert c2.remote_url == "http://example.com"
        assert c2.timeout == 120  # default timeout

    def test_init_from_config(self):
        # We patch sarc.client.api.config so we don't mess with the global app config
        with patch("sarc.client.api.config") as mock_config:
            mock_config.return_value.api.url = "http://config-url.com/"
            mock_config.return_value.api.timeout = 90

            c = SarcApiClient()
            assert c.remote_url == "http://config-url.com"
            assert c.timeout == 90

    def test_init_no_config_raises_error(self):
        with patch("sarc.client.api.config") as mock_config:
            # Simulate missing api section in config
            mock_config.return_value.api = None
            with pytest.raises(ConfigurationError):
                SarcApiClient()


class TestSarcApiClient:
    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_job_not_found(self, sarc_client):
        # Generate a random ObjectId that shouldn't exist
        random_oid = str(PydanticObjectId())

        # SarcApiClient uses requests, but TestClient uses httpx.
        # In production, requests.HTTPError is raised.
        # In tests with TestClient, httpx.HTTPStatusError is raised.
        with pytest.raises((requests.HTTPError, httpx.HTTPStatusError)) as excinfo:
            sarc_client.job_by_id(random_oid)

        # Expecting 404 Not Found for a valid formatted ObjectId that is missing
        assert excinfo.value.response.status_code == 404
        assert "Job not found" in excinfo.value.response.json()["detail"]

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_by_cluster(self, sarc_client):
        job_idenrifiers = sarc_client.job_query(cluster="raisin")
        assert len(job_idenrifiers) > 0
        for job_oid in job_idenrifiers:
            job = sarc_client.job_by_id(job_oid)
            assert job.cluster_name == "raisin"

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_by_job_id(self, sarc_client):
        # Corresponds to test_get_jobs_by_job_id in test_v0
        # Check querying by job_id=10
        # The client accepts job_id as int or list[int].
        # The list_jobs method puts it in params.
        jobs_list = sarc_client.job_list(job_id=10)
        assert len(jobs_list.jobs) > 0
        for job in jobs_list.jobs:
            assert job.job_id == 10

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_by_user(self, sarc_client):
        # Corresponds to test_get_jobs_by_user in test_v0
        jobs_list = sarc_client.job_list(username="petitbonhomme")
        assert len(jobs_list.jobs) > 0
        for job in jobs_list.jobs:
            assert job.user == "petitbonhomme"

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_by_state(self, sarc_client):
        # Corresponds to test_get_jobs_by_state in test_v0
        jobs_list = sarc_client.job_list(job_state=SlurmState.COMPLETED)
        assert len(jobs_list.jobs) > 0
        for job in jobs_list.jobs:
            assert job.job_state == SlurmState.COMPLETED

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_empty_result(self, sarc_client):
        # Corresponds to test_get_jobs_empty_result in test_v0
        # Use a very high job ID that doesn't exist
        jobs_list = sarc_client.job_list(job_id=9999999999)
        assert len(jobs_list.jobs) == 0
        assert jobs_list.total == 0

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_invalid_cluster(self, sarc_client):
        # Corresponds to test_get_jobs_invalid_cluster in test_v0
        with pytest.raises((requests.HTTPError, httpx.HTTPStatusError)) as excinfo:
            sarc_client.job_list(cluster="invalid_cluster")
        assert excinfo.value.response.status_code == 404
        assert (
            "No such cluster 'invalid_cluster'"
            in excinfo.value.response.json()["detail"]
        )

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_invalid_job_state(self, sarc_client):
        # Corresponds to test_get_jobs_invalid_job_state in test_v0
        # Since the client enforces Enum, we can't easily pass 'INVALID'.
        # However, we can trick it or pass None and modify params? Not easily.
        # If we pass a value that is NOT in the enum to requests but as a string?
        # The client type hint says SlurmState | None.
        # But at runtime Python allows passing a string if we ignore type checker.
        # BUT: `job_state.value if job_state else None` line in `list_jobs` will crash if we pass "INVALID" string.
        # So the client protects us. We can test that client raises AttributeError.
        with pytest.raises(AttributeError):
            sarc_client.job_list(job_state="INVALID")

    def test_get_jobs_with_datetime_filters(self):
        # todo
        pass

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_multiple_filters(self, sarc_client):
        # Corresponds to test_get_jobs_multiple_filters
        start_dt = datetime(2023, 1, 1, tzinfo=UTC)
        jobs_list = sarc_client.job_list(
            cluster="raisin", job_state=SlurmState.COMPLETED, start=start_dt
        )
        assert len(jobs_list.jobs) > 0
        for job in jobs_list.jobs:
            assert job.cluster_name == "raisin"
            assert job.job_state == SlurmState.COMPLETED
            # Check implicit start time? The API handles it.

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_no_filters(self, sarc_client):
        # Corresponds to test_get_jobs_no_filters
        jobs_list = sarc_client.job_list()
        assert len(jobs_list.jobs) == 24
        assert jobs_list.total == 24

    def test_count_jobs_by_cluster(self):
        pass  # todo

    def test_count_jobs_by_job_id(self):
        pass  # todo

    def test_count_jobs_by_user(self):
        pass  # todo

    def test_count_jobs_by_state(self):
        pass  # todo

    def test_count_jobs_empty_result(self):
        pass  # todo

    def test_count_jobs_invalid_cluster(self):
        pass  # todo

    def test_count_jobs_invalid_job_state(self):
        pass  # todo

    def test_count_jobs_with_datetime_filters(self):
        pass

    def test_count_jobs_multiple_filters(self):
        pass

    def test_count_jobs_no_filters(self):
        pass

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_count_jobs_matches_query_length(self, sarc_client):
        # Corresponds to test_count_jobs_matches_query_length
        jobs_list = sarc_client.job_list(cluster="raisin")
        count = sarc_client.job_count(cluster="raisin")
        assert jobs_list.total == count
        assert len(jobs_list.jobs) == count

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_cluster_list(self, sarc_client):
        clusters = sarc_client.cluster_list()
        assert set(clusters) == {
            "mila",
            "local",
            "gerudo",
            "raisin",
            "patate",
            "raisin_no_prometheus",
            "hyrule",
            "fromage",
        }

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_gpu_rgu(self, sarc_client, monkeypatch):
        def _gen_fake_rgus():
            return {"A100": 3.21}

        monkeypatch.setattr("sarc.api.v0.get_rgus", _gen_fake_rgus)

        rgus = sarc_client.gpu_rgu()
        assert rgus == {"A100": 3.21}

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_user_query_by_display_name(self, sarc_client):
        # Corresponds to test_user_query_by_display_name in test_v0
        # Search for "janE"
        # query_users returns list of UUIDs
        uuids = sarc_client.user_query(display_name="janE")
        assert len(uuids) == 1

        user = sarc_client.user_by_id(uuids[0])
        assert user.display_name == "Jane Doe"
        assert user.email == "jdoe@example.com"

    def test_user_query_by_email(self):
        pass

    def test_user_query_by_member_type_professor_now(self):
        pass

    def test_user_query_by_member_type_other_now(self):
        pass

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_user_query_by_supervisor_start(self, sarc_client):
        # Corresponds to test_user_query_by_supervisor_start in test_v0
        # We test just one case to verify parameter passing
        supervisor = "1f9b04e5-0ec4-4577-9196-2b03d254e344"
        start = datetime(2019, 9, 1, tzinfo=UTC)
        uuids = sarc_client.user_query(supervisor=supervisor, supervisor_start=start)
        assert len(uuids) == 2
        # Verify uuids
        expected_uuids = {
            "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4",
            "7ee5849c-241e-4d84-a4d2-1f73e22784f9",
        }
        assert {str(u) for u in uuids} == expected_uuids

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_user_query_by_supervisor_end(self, sarc_client):
        # Corresponds to test_user_query_by_supervisor_end in test_v0
        supervisor = "1f9b04e5-0ec4-4577-9196-2b03d254e344"
        end = datetime(2022, 5, 1, tzinfo=UTC)
        uuids = sarc_client.user_query(supervisor=supervisor, supervisor_end=end)
        assert len(uuids) == 2

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_user_query_by_cosupervisor_start_end(self, sarc_client):
        # Corresponds to test_user_query_by_cosupervisor_start_end in test_v0
        cosupervisor = "1f9b04e5-0ec4-4577-9196-2b03d254e344"
        start = datetime(2022, 1, 1, tzinfo=UTC)
        end = datetime(2023, 1, 1, tzinfo=UTC)
        uuids = sarc_client.user_query(
            co_supervisor=cosupervisor, co_supervisor_start=start, co_supervisor_end=end
        )
        assert len(uuids) == 1
        assert str(uuids[0]) == "8b4fef2b-8f47-4eb6-9992-3e7e1133b42a"

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_user_query_by_cosupervisor_bad_start_end(self, sarc_client):
        # Corresponds to test_user_query_by_cosupervisor_bad_start_end in test_v0
        start = datetime(2022, 1, 1, tzinfo=UTC)
        end = start - timedelta(days=1)
        # Should raise 400 Bad Request
        with pytest.raises((requests.HTTPError, httpx.HTTPStatusError)) as excinfo:
            sarc_client.user_query(
                co_supervisor="1f9b04e5-0ec4-4577-9196-2b03d254e344",
                co_supervisor_start=start,
                co_supervisor_end=end,
            )
        assert excinfo.value.response.status_code == 400

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_user_query_empty_result(self, sarc_client):
        # Corresponds to test_user_query_empty_result in test_v0
        uuids = sarc_client.user_query(email="nothing@nothing.nothing")
        assert len(uuids) == 0

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_user_query_no_filters(self, sarc_client):
        # Corresponds to test_user_query_no_filters in test_v0
        uuids = sarc_client.user_query()
        assert len(uuids) == 10

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_user_query_multiple_filters(self, sarc_client):
        # Corresponds to test_user_query_multiple_filters in test_v0
        uuids = sarc_client.user_query(
            supervisor="1f9b04e5-0ec4-4577-9196-2b03d254e344",
            supervisor_start=datetime(2020, 1, 1, tzinfo=UTC),
            display_name="sMiTh",
        )
        assert len(uuids) == 1
        assert str(uuids[0]) == "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"

        user = sarc_client.user_by_id(uuids[0])
        assert user.display_name == "John Smith"

    def test_get_user_by_uuid(self):
        pass

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_user_by_uuid_invalid(self, sarc_client):
        # Corresponds to test_get_user_by_uuid_invalid in test_v0
        # If we pass invalid string as UUID to get_user_by_id
        # Client type hint is UUID4 | str.
        # But wait, if we pass "invalid", API returns 422.
        with pytest.raises((requests.HTTPError, httpx.HTTPStatusError)) as excinfo:
            sarc_client.user_by_id("invalid")
        assert excinfo.value.response.status_code == 422

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_user_by_uuid_unknown(self, sarc_client):
        # Corresponds to test_get_user_by_uuid_unknown in test_v0
        unknown_uuid = "70000000-a000-4000-b000-c00000000000"
        with pytest.raises((requests.HTTPError, httpx.HTTPStatusError)) as excinfo:
            sarc_client.user_by_id(unknown_uuid)
        assert excinfo.value.response.status_code == 404

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_user_by_email(self, sarc_client):
        email = "jsmith@example.com"
        user = sarc_client.user_by_email(email)
        assert user.email == email
        assert user.display_name == "John Smith"

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_user_by_email_unknown(self, sarc_client):
        # Corresponds to test_get_user_by_email_unknown in test_v0
        with pytest.raises((requests.HTTPError, httpx.HTTPStatusError)) as excinfo:
            sarc_client.user_by_email("unknown")
        assert excinfo.value.response.status_code == 404

    # old

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs(self, sarc_client):
        # Test getting jobs for a specific cluster
        job_ids = sarc_client.job_query(cluster="raisin")
        assert len(job_ids) > 0

        # Verify we can fetch one of them
        first_job_id = job_ids[0]
        job = sarc_client.job_by_id(first_job_id)
        assert job.cluster_name == "raisin"

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_filters(self, sarc_client):
        # Test with enum and datetime objects to ensure serialization works
        start_dt = datetime(2023, 1, 1, tzinfo=UTC)

        job_ids_all_dates = sarc_client.job_query(job_state=SlurmState.RUNNING)
        assert len(job_ids_all_dates) > 0

        job_ids = sarc_client.job_query(job_state=SlurmState.RUNNING, start=start_dt)
        assert len(job_ids) > 0
        # assert len(job_ids_all_dates) > len(job_ids)
        for jid in job_ids:
            job = sarc_client.job_by_id(jid)
            assert job.job_state == SlurmState.RUNNING
            assert job.end_time is None or job.end_time > start_dt

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_count_jobs(self, sarc_client):
        count = sarc_client.job_count(cluster="raisin")
        assert count == 20  # Based on factory data

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_query_users_and_get_details(self, sarc_client):
        # Query users by email
        email = "jdoe@example.com"
        user_ids = sarc_client.user_query(email=email)
        assert len(user_ids) == 1

        # Get user details by ID
        uid = user_ids[0]
        user = sarc_client.user_by_id(uid)
        assert user.email == email
        assert user.display_name == "Jane Doe"
        assert user.uuid == uid

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_query_users_filters(self, sarc_client, freezer):
        # Set time to ensure member_type checks work as expected
        freezer.move_to(datetime(2022, 5, 1, tzinfo=UTC))

        # Query staff members
        staff_ids = sarc_client.user_query(member_type=MemberType.STAFF)
        assert len(staff_ids) > 0

        # Verify retrieved user is indeed staff
        user = sarc_client.user_by_id(staff_ids[0])
        assert user.member_type.get_value() == MemberType.STAFF

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_list_jobs_pagination(self, sarc_client, monkeypatch):
        # Monkeypatch PAGE_SIZE in the API module to test pagination with few items
        import sarc.api.v0

        monkeypatch.setattr(sarc.api.v0, "PAGE_SIZE", 5)

        # We expect 20 jobs in raisin cluster
        jobs_list = sarc_client.job_list(cluster="raisin", page=1)

        assert jobs_list.total == 20
        assert jobs_list.per_page == 5
        assert len(jobs_list.jobs) == 5
        assert jobs_list.page == 1

        # Test getting second page
        jobs_list_2 = sarc_client.job_list(cluster="raisin", page=2)
        assert jobs_list_2.page == 2
        assert len(jobs_list_2.jobs) == 5
        # Ensure different jobs (simple check)
        assert jobs_list.jobs[0].job_id != jobs_list_2.jobs[0].job_id

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_count_jobs_variations(self, sarc_client):
        # Corresponds to count tests
        assert sarc_client.job_count(cluster="raisin") == 20
        assert sarc_client.job_count(job_id=10) == 1
        assert sarc_client.job_count(username="petitbonhomme") == 20
        assert sarc_client.job_count(job_state=SlurmState.COMPLETED) == 1
        assert sarc_client.job_count(job_id=9999999999) == 0

        # datetime
        start = datetime(2023, 1, 1, tzinfo=UTC)
        end = datetime(2023, 2, 15, 23, 59, 59, tzinfo=UTC)
        assert sarc_client.job_count(start=start, end=end) == 8

        # multiple
        assert (
            sarc_client.job_count(
                cluster="raisin", job_state=SlurmState.COMPLETED, start=start
            )
            > 0
        )

        # no filters
        assert sarc_client.job_count() == 24

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_count_jobs_invalid(self, sarc_client):
        with pytest.raises((requests.HTTPError, httpx.HTTPStatusError)) as excinfo:
            sarc_client.job_count(cluster="invalid_cluster")
        assert excinfo.value.response.status_code == 404


@pytest.fixture
def mock_client_class(client, monkeypatch):
    import sarc.client.api

    # Let's create a factory that returns a client with our test session
    def client_factory(*args, **kwargs):
        return SarcApiClient(remote_url="http://testserver", session=client)

    monkeypatch.setattr(sarc.client.api, "SarcApiClient", client_factory)


class TestHighLevelClientFunctions:
    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_jobs_helper_pagination(self, mock_client_class, monkeypatch):
        """
        Test the top-level get_jobs function which should iterate over pages.
        We need to patch SarcApiClient usage inside the function to use our test client session.
        """
        from sarc.client.api import get_jobs

        # 1. Patch PAGE_SIZE to force pagination
        monkeypatch.setattr("sarc.api.v0.PAGE_SIZE", 5)

        # 2. Patch SarcApiClient in the module to return our test-connected client
        # The get_jobs function instantiates SarcApiClient(), so we patch the class.
        # But we need it to behave like our 'sarc_client' fixture which has the session.

        # A cleaner way is to patch the session instantiation inside __init__ or pass the session somehow.
        # Since get_jobs instantiates SarcApiClient() with no args, it tries to read config.
        # We can patch SarcApiClient to return a mock or our configured instance.

        # Call the helper function
        all_jobs = get_jobs(cluster="raisin")

        # Verify we got all 20 jobs despite PAGE_SIZE=5
        assert len(all_jobs) == 20
        assert isinstance(all_jobs[0], SlurmJob)

    @pytest.mark.usefixtures("read_only_db_with_users")
    def test_get_users_helper_pagination(self, mock_client_class, monkeypatch):
        """
        Test the top-level get_users function which should iterate over pages.
        """
        from sarc.client.api import get_users
        from sarc.core.models.users import UserData

        # 1. Patch PAGE_SIZE to force pagination (total users is 10)
        monkeypatch.setattr("sarc.api.v0.PAGE_SIZE", 3)

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
    def test_load_job_series_rest(self, mock_client_class):
        """
        Test load_job_series via REST API.
        """
        import pandas as pd
        from sarc.client.api import load_job_series

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
