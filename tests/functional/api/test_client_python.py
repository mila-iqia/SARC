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


def test_init_missing_configuration_error():
    """Test that MissingConfigurationError is caught and handled."""
    from gifnoc.proxy import MissingConfigurationError

    with patch("sarc.client.api.config") as mock_config:
        # Simulate gifnoc raising MissingConfigurationError
        mock_config.side_effect = MissingConfigurationError("No config")
        # When remote_url is provided, it should still work
        c = SarcApiClient(remote_url="http://example.com")
        assert c.remote_url == "http://example.com"
        assert c.timeout == 120  # default when no config


def test_init_missing_configuration_error_no_url():
    """Test that MissingConfigurationError without URL raises ConfigurationError."""
    from gifnoc.proxy import MissingConfigurationError

    with patch("sarc.client.api.config") as mock_config:
        mock_config.side_effect = MissingConfigurationError("No config")
        with pytest.raises(ConfigurationError):
            SarcApiClient()  # No URL provided, should fail


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
    class MockSarcApiClient(SarcApiClient):
        def __init__(self, *args, **kwargs):
            super().__init__(remote_url="http://testserver", session=client)

    monkeypatch.setattr(sarc.client.api, "SarcApiClient", MockSarcApiClient)


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


# Test High-Level Client Functions: count_jobs


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_count_jobs(mock_client_class):
    """Test the top-level count_jobs function."""
    from sarc.client.api import count_jobs

    count = count_jobs(cluster="raisin")
    assert count == 20


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_count_jobs_with_filters(mock_client_class):
    """Test count_jobs with multiple filters."""
    from sarc.client.api import count_jobs

    count = count_jobs(cluster="raisin", job_state="COMPLETED")
    assert count > 0


def test_rest_count_jobs_invalid_cluster_returns_zero(mock_client_class):
    """Test that count_jobs returns 0 for invalid cluster (404 handling)."""
    from sarc.client.api import count_jobs

    # Invalid cluster should return 0 (not raise exception)
    count = count_jobs(cluster="nonexistent_cluster")
    assert count == 0


# Test High-Level Client Functions: get_job


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_get_job_found(mock_client_class):
    """Test get_job returns a single job when found."""
    from sarc.client.api import get_job

    job = get_job(cluster="raisin", job_id=10)
    assert job is not None
    assert isinstance(job, SlurmJob)
    assert job.job_id == 10
    assert job.cluster_name == "raisin"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_get_job_not_found(mock_client_class):
    """Test get_job returns None when no job matches."""
    from sarc.client.api import get_job

    # Use a combination that doesn't match any job
    job = get_job(cluster="raisin", job_id=777777777)
    assert job is None


def test_rest_get_job_invalid_cluster_returns_none(mock_client_class):
    """Test get_job returns None for invalid cluster."""
    from sarc.client.api import get_job

    job = get_job(cluster="nonexistent_cluster", job_id=10)
    assert job is None


# Test High-Level Client Functions: get_jobs pagination


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_get_jobs_pagination(mock_client_class):
    """
    Test that get_jobs correctly iterates over multiple pages.
    With 20 jobs in raisin and default page size 100, should get all in one request.
    """
    from sarc.client.api import get_jobs

    all_jobs = list(get_jobs(cluster="raisin"))
    assert len(all_jobs) == 20
    # Verify all jobs are from raisin cluster
    assert all(job.cluster_name == "raisin" for job in all_jobs)


# Test High-Level Client Functions: get_users pagination


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_get_users_pagination(mock_client_class):
    """Test that get_users correctly iterates over multiple pages."""
    from sarc.client.api import get_users

    all_users = get_users()
    assert len(all_users) == 10

    # Test with filters
    filtered_users = get_users(display_name="Smith")
    assert len(filtered_users) == 1
    assert filtered_users[0].display_name == "John Smith"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_get_jobs_multi_page_iteration(mock_client_class):
    """
    Test that get_jobs correctly iterates through multiple pages.
    """
    from sarc.client.api import get_jobs

    with patch(
        "sarc.client.api.SarcApiClient.job_list",
        autospec=True,
        side_effect=SarcApiClient.job_list,
    ) as mock_func:
        all_jobs = list(get_jobs(cluster="raisin", per_page=3))
        # Should still get all 20 jobs across multiple pages
        assert len(all_jobs) == 20

        assert mock_func.call_count == 7


@pytest.mark.usefixtures("read_only_db_with_users")
def test_rest_get_users_multi_page_iteration(mock_client_class):
    """
    Test that get_users correctly iterates through multiple pages.
    """
    from sarc.client.api import get_users

    with patch(
        "sarc.client.api.SarcApiClient.job_list",
        autospec=True,
        side_effect=SarcApiClient.job_list,
    ) as mock_func:
        all_users = get_users(per_page=3)
        # Should still get all 10 users across multiple pages
        assert len(all_users) == 10

        assert mock_func.call_count == 4


def test_rest_get_jobs_invalid_cluster_yields_nothing(mock_client_class):
    """Test get_jobs yields nothing for invalid cluster (404 handling)."""
    from sarc.client.api import get_jobs

    # Invalid cluster should yield no jobs (not raise exception)
    all_jobs = list(get_jobs(cluster="nonexistent_cluster"))
    assert len(all_jobs) == 0


# Test job_id list via SarcApiClient


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_job_id_list(sarc_client):
    """Test job_query with a list of job IDs."""
    data = sarc_client.job_query(job_id=[8, 9])
    assert len(data) == 2
    for jid in data:
        job = sarc_client.job_by_id(jid)
        assert job.job_id in [8, 9]


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_job_id_list(sarc_client):
    """Test job_count with a list of job IDs."""
    count = sarc_client.job_count(job_id=[8, 9])
    assert count == 2


@pytest.mark.usefixtures("read_only_db")
def test_job_list_by_job_id_list(sarc_client):
    """Test job_list with a list of job IDs."""
    result = sarc_client.job_list(job_id=[8, 9])
    assert result.total == 2
    assert len(result.jobs) == 2
    job_ids = [job.job_id for job in result.jobs]
    assert 8 in job_ids
    assert 9 in job_ids


# Test per_page parameter


@pytest.mark.usefixtures("read_only_db_with_users")
def test_job_list_per_page(sarc_client):
    """Test job_list with custom per_page."""
    # Get first 3 jobs
    result = sarc_client.job_list(cluster="raisin", per_page=3)
    assert result.per_page == 3
    assert len(result.jobs) == 3
    assert result.total == 20

    # Get second page
    result2 = sarc_client.job_list(cluster="raisin", page=2, per_page=3)
    assert result2.page == 2
    assert len(result2.jobs) == 3

    # Verify different jobs on different pages
    page1_ids = {job.job_id for job in result.jobs}
    page2_ids = {job.job_id for job in result2.jobs}
    assert page1_ids.isdisjoint(page2_ids)


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_list_per_page(sarc_client):
    """Test user_list with custom per_page."""
    # Get first 3 users
    result = sarc_client.user_list(per_page=3)
    assert result.per_page == 3
    assert len(result.users) == 3
    assert result.total == 10

    # Get second page
    result2 = sarc_client.user_list(page=2, per_page=3)
    assert result2.page == 2
    assert len(result2.users) == 3

    # Verify different users on different pages
    page1_emails = {user.email for user in result.users}
    page2_emails = {user.email for user in result2.users}
    assert page1_emails.isdisjoint(page2_emails)


# Test get_rgus version handling


def test_rest_get_rgus_unsupported_version(mock_client_class):
    """Test get_rgus raises NotImplementedError for unsupported version."""
    from sarc.client.api import get_rgus

    with pytest.raises(NotImplementedError, match="rgu_version != 1.0"):
        get_rgus(rgu_version="2.0")


# Test _parse_common_args


def test_parse_common_args_int_job_id():
    """Test _parse_common_args converts int job_id to list."""
    from sarc.client.api import _parse_common_args

    job_id, _, _, _ = _parse_common_args(job_id=123)
    assert job_id == [123]

    # List should be unchanged
    job_id2, _, _, _ = _parse_common_args(job_id=[1, 2, 3])
    assert job_id2 == [1, 2, 3]


def test_parse_common_args_str_job_state():
    """Test _parse_common_args converts string job_state to enum."""
    from sarc.client.api import _parse_common_args

    _, job_state, _, _ = _parse_common_args(job_state="COMPLETED")
    assert job_state == SlurmState.COMPLETED

    # Enum should be unchanged
    _, job_state2, _, _ = _parse_common_args(job_state=SlurmState.RUNNING)
    assert job_state2 == SlurmState.RUNNING


def test_parse_common_args_str_dates():
    """Test _parse_common_args converts string dates to datetime."""
    from sarc.client.api import _parse_common_args

    _, _, start, end = _parse_common_args(start="2023-01-15", end="2023-02-20")

    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert start.year == 2023
    assert start.month == 1
    assert start.day == 15
    assert end.year == 2023
    assert end.month == 2
    assert end.day == 20

    # datetime should be unchanged
    dt = datetime(2024, 6, 15, 12, 30, tzinfo=UTC)
    _, _, start2, _ = _parse_common_args(start=dt)
    assert start2 == dt


# Test pagination validation errors


@pytest.mark.usefixtures("read_only_db")
def test_job_list_page_less_than_one(sarc_client):
    """Test job_list with page < 1 raises 400 error."""
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.job_list(page=0)
    assert excinfo.value.response.status_code == 400
    assert "Page must be >= 1" in excinfo.value.response.json()["detail"]


@pytest.mark.usefixtures("read_only_db")
def test_job_list_per_page_less_than_one(sarc_client):
    """Test job_list with per_page < 1 raises 400 error."""
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.job_list(per_page=0)
    assert excinfo.value.response.status_code == 400
    assert "Page size must be >= 1" in excinfo.value.response.json()["detail"]


@pytest.mark.usefixtures("read_only_db")
def test_job_list_per_page_exceeds_max(sarc_client):
    """Test job_list with per_page > MAX_PAGE_SIZE raises 400 error."""
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.job_list(per_page=500)  # MAX_PAGE_SIZE is 200
    assert excinfo.value.response.status_code == 400
    assert "Page size must be <=" in excinfo.value.response.json()["detail"]


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_list_page_less_than_one(sarc_client):
    """Test user_list with page < 1 raises 400 error."""
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.user_list(page=0)
    assert excinfo.value.response.status_code == 400
    assert "Page must be >= 1" in excinfo.value.response.json()["detail"]


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_list_per_page_less_than_one(sarc_client):
    """Test user_list with per_page < 1 raises 400 error."""
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.user_list(per_page=0)
    assert excinfo.value.response.status_code == 400
    assert "Page size must be >= 1" in excinfo.value.response.json()["detail"]


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_list_per_page_exceeds_max(sarc_client):
    """Test user_list with per_page > MAX_PAGE_SIZE raises 400 error."""
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        sarc_client.user_list(per_page=500)  # MAX_PAGE_SIZE is 200
    assert excinfo.value.response.status_code == 400
    assert "Page size must be <=" in excinfo.value.response.json()["detail"]
