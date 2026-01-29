from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest
from pydantic_mongo import PydanticObjectId

from sarc.client.api import SarcApiClient
from sarc.client.job import SlurmJob, SlurmState
from sarc.config import ConfigurationError, UTC
from sarc.core.models.users import MemberType


# Test SarcApiClient Initialization


def test_init_with_params():
    c = SarcApiClient(remote_url="http://example.com", timeout=60)
    assert c.remote_url == "http://example.com"
    assert c.timeout == 60

    c2 = SarcApiClient("http://example.com/")
    assert c2.remote_url == "http://example.com"
    assert c2.timeout == 120  # default timeout


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


# Test SarcApiClient Methods


@pytest.fixture
def sarc_client(client):
    """
    Returns a SarcApiClient that uses the FastAPI TestClient session.
    The 'client' fixture comes from tests/functional/api/conftest.py
    and is connected to the app with the database fixtures active.
    """
    return SarcApiClient(remote_url="http://testserver", session=client)


@pytest.mark.usefixtures("read_only_db", "client_mode")
def test_get_job_not_found_pydantic_id(sarc_client):
    # Generate a random ObjectId that shouldn't exist
    oid = str(PydanticObjectId())

    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.job_by_id(oid)
    assert excinfo.value.response.status_code == 404

    # Expecting 404 Not Found for a valid formatted ObjectId that is missing
    assert excinfo.value.response.status_code == 404
    assert "Job not found" in excinfo.value.response.json()["detail"]


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_jobs_by_cluster(sarc_client):
    data = sarc_client.job_query(cluster="raisin")
    assert len(data) > 0
    for jid in data:
        job = sarc_client.job_by_id(jid)
        assert job.cluster_name == "raisin"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_job_id(sarc_client):
    data = sarc_client.job_query(job_id=10)
    assert len(data) > 0
    for jid in data:
        job = sarc_client.job_by_id(jid)
        assert job.job_id == 10


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_user(sarc_client):
    # Corresponds to test_get_jobs_by_user in test_v0
    data = sarc_client.job_query(username="petitbonhomme")
    assert len(data) > 0
    for jid in data:
        job = sarc_client.job_by_id(jid)
        assert job.user == "petitbonhomme"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_jobs_by_state(sarc_client):
    # Corresponds to test_get_jobs_by_state in test_v0
    data = sarc_client.job_query(job_state=SlurmState.COMPLETED)
    assert len(data) > 0
    for jid in data:
        job = sarc_client.job_by_id(jid)
        assert job.job_state == SlurmState.COMPLETED


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_empty_result(sarc_client):
    # Corresponds to test_get_jobs_empty_result in test_v0
    # Use a very high job ID that doesn't exist
    data = sarc_client.job_query(job_id=9999999999)
    assert len(data) == 0


def test_get_jobs_invalid_cluster(sarc_client):
    # Corresponds to test_get_jobs_invalid_cluster in test_v0
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.job_query(cluster="invalid_cluster")
    assert excinfo.value.response.status_code == 404
    assert (
        "No such cluster 'invalid_cluster'" in excinfo.value.response.json()["detail"]
    )


def test_get_jobs_invalid_job_state(sarc_client):
    with pytest.raises(AttributeError):
        sarc_client.job_query(job_state="INVALID")


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_with_datetime_filters(sarc_client):
    data = sarc_client.job_query(
        start=datetime.fromisoformat("2023-01-01T00:00:00"),
        end=datetime.fromisoformat("2023-12-31T23:59:59"),
    )
    assert len(data) > 0


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_jobs_multiple_filters(sarc_client):
    # Corresponds to test_get_jobs_multiple_filters
    data = sarc_client.job_query(
        cluster="raisin",
        job_state=SlurmState.COMPLETED,
        start=datetime(2023, 1, 1, tzinfo=UTC),
    )
    assert len(data) > 0
    for jid in data:
        job = sarc_client.job_by_id(jid)
        assert job.cluster_name == "raisin"
        assert job.job_state == SlurmState.COMPLETED


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_no_filters(sarc_client):
    # Corresponds to test_get_jobs_no_filters
    data = sarc_client.job_query()
    assert len(data) == 24


@pytest.mark.usefixtures("read_only_db_with_users")
def test_count_jobs_by_cluster(sarc_client):
    count = sarc_client.job_count(cluster="raisin")
    assert count == 20


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_job_id(sarc_client):
    count = sarc_client.job_count(job_id=10)
    assert count == 1


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_user(sarc_client):
    count = sarc_client.job_count(username="petitbonhomme")
    assert count == 20


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_state(sarc_client):
    count = sarc_client.job_count(job_state=SlurmState.COMPLETED)
    assert count == 1


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_empty_result(sarc_client):
    count = sarc_client.job_count(job_id=9999999999)
    assert count == 0


def test_count_jobs_invalid_cluster(sarc_client):
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.job_count(cluster="invalid_cluster")
    assert excinfo.value.response.status_code == 404
    assert (
        "No such cluster 'invalid_cluster'" in excinfo.value.response.json()["detail"]
    )


def test_count_jobs_invalid_job_state(sarc_client):
    with pytest.raises(AttributeError):
        sarc_client.job_count(job_state="INVALID")


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_with_datetime_filters(sarc_client):
    count = sarc_client.job_count(
        start=datetime.fromisoformat("2023-01-01T00:00:00"),
        end=datetime.fromisoformat("2023-02-15T23:59:59"),
    )
    assert count == 8


@pytest.mark.usefixtures("read_only_db_with_users")
def test_count_jobs_multiple_filters(sarc_client):
    count = sarc_client.job_count(
        cluster="raisin",
        job_state=SlurmState.COMPLETED,
        start=datetime.fromisoformat("2023-01-01T00:00:00"),
    )
    assert count > 0


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_no_filters(sarc_client):
    count = sarc_client.job_count()
    assert count == 24


@pytest.mark.usefixtures("read_only_db_with_users")
def test_count_jobs_matches_query_length(sarc_client):
    # Corresponds to test_count_jobs_matches_query_length
    response_query = sarc_client.job_query(cluster="raisin")
    response_list = sarc_client.job_list(cluster="raisin")
    count = sarc_client.job_count(cluster="raisin")
    assert len(response_query) == count
    assert len(response_list.jobs) == count


@pytest.mark.usefixtures("read_only_db_with_users")
def test_cluster_list(sarc_client):
    data = sarc_client.cluster_list()
    for cluster_name in ("raisin", "fromage", "patate"):
        assert cluster_name in data


def _gen_fake_rgus():
    return {"A100": 3.21}


@pytest.mark.usefixtures("read_only_db_with_users")
def test_gpu_rgu(sarc_client, monkeypatch):
    monkeypatch.setattr("sarc.api.v0.get_rgus", _gen_fake_rgus)

    rgus = sarc_client.gpu_rgu()
    assert rgus == _gen_fake_rgus()


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_display_name(sarc_client):
    data = sarc_client.user_query(display_name="janE")
    assert len(data) == 1
    assert str(data[0]) == "1f9b04e5-0ec4-4577-9196-2b03d254e344"

    user = sarc_client.user_by_id(data[0])
    assert user.display_name == "Jane Doe"
    assert user.uuid == data[0]
    assert user.email == "jdoe@example.com"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_email(sarc_client):
    data = sarc_client.user_query(email="bonhomme@mila.quebec")
    assert isinstance(data, list)
    assert len(data) == 1
    assert str(data[0]) == "5a8b9e7f-afcc-4ced-b596-44fcdb3a0cff"

    user = sarc_client.user_by_id(data[0])
    assert user.display_name == "M/Ms Bonhomme"
    assert user.uuid == data[0]
    assert user.email == "bonhomme@mila.quebec"


@pytest.mark.parametrize(
    "date,expected",
    [
        # After 2026/05/01 and until 2027/09/01, there should be only 1 match
        (datetime(2027, 9, 1, tzinfo=UTC), True),
        (datetime(2027, 8, 20, tzinfo=UTC), True),
        (datetime(2026, 5, 2, tzinfo=UTC), True),
        # Before 2020/09/01 and after 2027/09/01, there should be not match
        (datetime(2005, 8, 31, tzinfo=UTC), False),
        (datetime(2010, 8, 31, tzinfo=UTC), False),
        (datetime(2018, 8, 31, tzinfo=UTC), False),
        (datetime(2020, 8, 31, 23, 59, 59, 999999, tzinfo=UTC), False),
        (datetime(2027, 9, 1, 0, 0, 1, tzinfo=UTC), False),
        (datetime(2027, 12, 1, tzinfo=UTC), False),
        (datetime(2044, 7, 1, tzinfo=UTC), False),
    ],
)
@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_member_type_professor_now(sarc_client, freezer, date, expected):
    freezer.move_to(date)
    data = sarc_client.user_query(member_type=MemberType.PROFESSOR)
    assert isinstance(data, list)
    data = [str(uuid) for uuid in data]
    if expected:
        assert len(data) == 1
        assert sorted(data) == ["1f9b04e5-0ec4-4577-9196-2b03d254e344"]
    else:
        assert len(data) == 0


@pytest.mark.parametrize(
    "member_type,date,identifiers",
    [
        (
            MemberType.PHD_STUDENT,
            datetime(2020, 8, 31, 23, 59, 59, 999999, tzinfo=UTC),
            ["7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"],
        ),
        (
            MemberType.STAFF,
            datetime(2022, 5, 1, tzinfo=UTC),
            ["8b4fef2b-8f47-4eb6-9992-3e7e1133b42a"],
        ),
    ],
)
@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_member_type_other_now(
    sarc_client, freezer, member_type, date, identifiers
):
    freezer.move_to(date)
    data = sarc_client.user_query(member_type=member_type)
    assert isinstance(data, list)
    assert sorted(str(uuid) for uuid in data) == sorted(identifiers)


@pytest.mark.parametrize(
    "start,nb_expected",
    [
        # Start old enough to get all supervised users (2 users)
        (datetime(2005, 1, 1, tzinfo=UTC), 2),
        (datetime(2018, 8, 31, 23, 59, 59, 999999, tzinfo=UTC), 2),
        (datetime(2018, 9, 1, tzinfo=UTC), 2),
        (datetime(2019, 9, 1, tzinfo=UTC), 2),
        (datetime(2020, 9, 1, tzinfo=UTC), 2),
        # From these start dates, we get only 1 supervised user
        (datetime(2021, 5, 2, 0, 0, 0, 1, tzinfo=UTC), 1),
        (datetime(2022, 1, 1, tzinfo=UTC), 1),
        (datetime(2022, 10, 1, tzinfo=UTC), 1),
        (datetime(2022, 5, 1, tzinfo=UTC), 1),
        # From these start dates, there is no more user supervised by this prof.
        (datetime(2023, 1, 1, 0, 0, 1, tzinfo=UTC), 0),
        (datetime(2025, 1, 1, tzinfo=UTC), 0),
        (datetime(2035, 1, 1, tzinfo=UTC), 0),
    ],
)
@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_supervisor_start(sarc_client, start, nb_expected):
    supervisor = UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344")

    data = sarc_client.user_query(supervisor=supervisor, supervisor_start=start)
    assert isinstance(data, list)
    assert len(data) == nb_expected
    data = [str(uuid) for uuid in data]
    if nb_expected == 2:
        assert sorted(data) == [
            "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4",
            "7ee5849c-241e-4d84-a4d2-1f73e22784f9",
        ]
    elif nb_expected == 1:
        assert data == ["7ee5849c-241e-4d84-a4d2-1f73e22784f9"]
    else:
        assert not data


@pytest.mark.parametrize(
    "end,nb_expected",
    [
        # End new enough to get all supervised users (2 users)
        (datetime(2035, 1, 1, tzinfo=UTC), 2),
        (datetime(2025, 1, 1, tzinfo=UTC), 2),
        (datetime(2023, 1, 1, tzinfo=UTC), 2),
        (datetime(2022, 5, 1, tzinfo=UTC), 2),
        (datetime(2022, 1, 1, tzinfo=UTC), 2),
        # Until these end dates, we get only 1 supervised user
        (datetime(2021, 5, 1, tzinfo=UTC), 1),
        (datetime(2020, 1, 1, tzinfo=UTC), 1),
        (datetime(2019, 1, 1, tzinfo=UTC), 1),
        (datetime(2018, 9, 1, tzinfo=UTC), 1),
        # Until these end dates, there is no more user supervised by this prof.
        (datetime(2018, 8, 31, 23, 59, 59, 9999, tzinfo=UTC), 0),
        (datetime(2017, 1, 1, tzinfo=UTC), 0),
        (datetime(2005, 1, 1, tzinfo=UTC), 0),
    ],
)
@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_supervisor_end(sarc_client, end, nb_expected):
    supervisor = UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344")
    data = sarc_client.user_query(supervisor=supervisor, supervisor_end=end)
    assert isinstance(data, list)
    assert len(data) == nb_expected
    data = [str(uuid) for uuid in data]
    if nb_expected == 2:
        assert sorted(data) == [
            "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4",
            "7ee5849c-241e-4d84-a4d2-1f73e22784f9",
        ]
    elif nb_expected == 1:
        assert data == ["7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"]
    else:
        assert not data


@pytest.mark.parametrize(
    "start,end,expected",
    [
        (datetime(2022, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, tzinfo=UTC), True),
        (datetime(2022, 1, 1, tzinfo=UTC), datetime(2022, 5, 1, tzinfo=UTC), True),
        (datetime(2022, 5, 1, tzinfo=UTC), datetime(2023, 1, 1, tzinfo=UTC), True),
        (datetime(2022, 5, 1, tzinfo=UTC), datetime(2022, 8, 1, tzinfo=UTC), True),
        (datetime(2021, 1, 1, tzinfo=UTC), datetime(2022, 1, 1, tzinfo=UTC), True),
        (datetime(2023, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC), True),
        # outside
        (datetime(2021, 1, 1, tzinfo=UTC), datetime(2021, 9, 1, tzinfo=UTC), False),
        (datetime(2023, 1, 2, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC), False),
    ],
)
@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_cosupervisor_start_end(sarc_client, start, end, expected):
    cosupervisor = UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344")
    data = sarc_client.user_query(
        co_supervisor=cosupervisor, co_supervisor_start=start, co_supervisor_end=end
    )
    assert isinstance(data, list)
    if expected:
        assert len(data) == 1
        assert str(data[0]) == "8b4fef2b-8f47-4eb6-9992-3e7e1133b42a"
    else:
        assert not data


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_cosupervisor_bad_start_end(sarc_client):
    # Corresponds to test_user_query_by_cosupervisor_bad_start_end in test_v0
    start = datetime(2022, 1, 1, tzinfo=UTC)
    end = start - timedelta(days=1)
    # Should raise 400 Bad Request
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.user_query(
            co_supervisor=UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344"),
            co_supervisor_start=start,
            co_supervisor_end=end,
        )
    assert excinfo.value.response.status_code == 400


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_empty_result(sarc_client):
    # Corresponds to test_user_query_empty_result in test_v0
    uuids = sarc_client.user_query(email="nothing@nothing.nothing")
    assert len(uuids) == 0


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_no_filters(sarc_client):
    # Corresponds to test_user_query_no_filters in test_v0
    data = sarc_client.user_query()
    assert len(data) == 10


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_multiple_filters(sarc_client):
    # Corresponds to test_user_query_multiple_filters in test_v0
    data = sarc_client.user_query(
        supervisor=UUID("1f9b04e5-0ec4-4577-9196-2b03d254e344"),
        supervisor_start=datetime(2020, 1, 1, tzinfo=UTC),
        display_name="sMiTh",
    )
    assert len(data) == 1
    assert str(data[0]) == "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"

    user = sarc_client.user_by_id(data[0])
    assert user.display_name == "John Smith"
    assert user.uuid == data[0]


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_uuid(sarc_client):
    user = sarc_client.user_by_id("7ecd3a8a-ab71-499e-b38a-ceacd91a99c4")
    assert user.email == "jsmith@example.com"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_uuid_invalid(sarc_client):
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.user_by_id("invalid")
    assert excinfo.value.response.status_code == 422


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_uuid_unknown(sarc_client):
    # Corresponds to test_get_user_by_uuid_unknown in test_v0
    unknown_uuid = "70000000-a000-4000-b000-c00000000000"
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.user_by_id(unknown_uuid)
    assert excinfo.value.response.status_code == 404


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_email(sarc_client):
    email = "jsmith@example.com"
    user = sarc_client.user_by_email(email)
    assert user.email == email
    assert user.display_name == "John Smith"
    assert str(user.uuid) == "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_email_unknown(sarc_client):
    # Corresponds to test_get_user_by_email_unknown in test_v0
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.user_by_email("unknown")
    assert excinfo.value.response.status_code == 404


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_list(sarc_client):
    """Test user list with pagination."""
    # Default page 1
    data = sarc_client.user_list()
    assert data.page == 1
    assert data.total == 10
    assert len(data.users) == 10

    # Check sorting: email asc
    emails = [u.email for u in data.users]
    assert emails == sorted(emails)

    data = sarc_client.user_list(page=2)
    assert data.page == 2
    assert data.total == 10
    assert len(data.users) == 0

    # Test filtering with list endpoint
    data = sarc_client.user_list(display_name="janE")
    assert data.total == 1
    assert len(data.users) == 1
    assert data.users[0].display_name == "Jane Doe"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_job_list(sarc_client):
    # We expect 20 jobs in raisin cluster
    prev_jobs = []
    for page in range(1, 5):
        jobs_list = sarc_client.job_list(cluster="raisin", page=page, per_page=5)
        assert jobs_list.total == 20
        assert jobs_list.per_page == 5
        assert len(jobs_list.jobs) == 5
        assert jobs_list.page == page
        assert prev_jobs != jobs_list.jobs
        prev_jobs = jobs_list.jobs

    for page in range(6, 10):
        jobs_list = sarc_client.job_list(cluster="raisin", page=page, per_page=5)
        assert jobs_list.total == 20
        assert jobs_list.per_page == 5
        assert len(jobs_list.jobs) == 0
        assert jobs_list.page == page


# Test High-Level Client Functions


@pytest.fixture
def mock_client_class(client, monkeypatch):
    import sarc.client.api

    # Let's create a factory that returns a client with our test session
    def client_factory(*args, **kwargs):
        return SarcApiClient(remote_url="http://testserver", session=client)

    monkeypatch.setattr(sarc.client.api, "SarcApiClient", client_factory)


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_get_jobs(mock_client_class):
    """
    Test the top-level get_jobs function which should iterate over pages.
    """
    from sarc.client.api import get_jobs

    # Call the helper function
    all_jobs = list(get_jobs(cluster="raisin"))

    assert len(all_jobs) == 20
    assert isinstance(all_jobs[0], SlurmJob)

    # TODO Verify we got all 20 jobs despite PAGE_SIZE=5


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_get_users(mock_client_class):
    """
    Test the top-level get_users function which should iterate over pages.
    """
    from sarc.client.api import get_users
    from sarc.core.models.users import UserData

    # Call the helper function
    all_users = get_users()

    assert len(all_users) == 10
    assert isinstance(all_users[0], UserData)
    # TODO Verify we got all 10 users despite PAGE_SIZE=3

    # Verify sorting by email (default)
    emails = [u.email for u in all_users]
    assert emails == sorted(emails)

    # Test with filter
    doe_users = get_users(display_name="Doe")
    assert len(doe_users) == 1
    assert doe_users[0].display_name == "Jane Doe"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_load_job_series(mock_client_class):
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
