from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from sarc.api.v0 import build
from sarc.config import UTC


@pytest.fixture
def app():
    """Create a FastAPI test app with the v0 router."""
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(build())
    return app


@pytest.fixture
def client(app):
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.mark.usefixtures("read_only_db", "client_mode")
def test_get_job_not_found(client):
    """Test job not found returns 404."""
    response = client.get("/v0/job/id/not_found")

    assert response.status_code == 422


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_jobs_by_cluster(client):
    """Test successful jobs query by cluster."""
    response = client.get("/v0/job/query?cluster=raisin")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for jid in data:
        r = client.get(f"/v0/job/id/{jid}")
        assert r.status_code == 200
        job = r.json()
        assert job["cluster_name"] == "raisin"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_job_id(client):
    """Test jobs query by job ID."""
    response = client.get("/v0/job/query?job_id=10")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for jid in data:
        r = client.get(f"/v0/job/id/{jid}")
        assert r.status_code == 200
        job = r.json()
        assert job["job_id"] == 10


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_user(client):
    """Test jobs query by username."""
    response = client.get("/v0/job/query?username=petitbonhomme")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for jid in data:
        r = client.get(f"/v0/job/id/{jid}")
        assert r.status_code == 200
        job = r.json()
        assert job["user"] == "petitbonhomme"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_state(client):
    """Test jobs query by job state."""
    response = client.get("/v0/job/query?job_state=COMPLETED")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for jid in data:
        r = client.get(f"/v0/job/id/{jid}")
        assert r.status_code == 200
        job = r.json()
        assert job["job_state"] == "COMPLETED"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_empty_result(client):
    """Test jobs query with no results."""
    # Use a very high job ID that doesn't exist
    response = client.get("/v0/job/query?job_id=9999999999")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 0


def test_get_jobs_invalid_cluster(client):
    """Test jobs query with invalid cluster."""

    response = client.get("/v0/job/query?cluster=invalid_cluster")

    assert response.status_code == 404
    data = response.json()
    assert "No such cluster 'invalid_cluster'" in data["detail"]


def test_get_jobs_invalid_job_state(client):
    """Test jobs query with invalid job state."""
    response = client.get("/v0/job/query?job_state=INVALID")

    assert response.status_code == 422


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_with_datetime_filters(client):
    """Test jobs query with start and end datetime filters."""
    params = {
        "start": "2023-01-01T00:00:00",
        "end": "2023-12-31T23:59:59",
    }

    response = client.get("/v0/job/query", params=params)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_jobs_multiple_filters(client):
    """Test jobs query with multiple filters."""
    params = {
        "cluster": "raisin",
        "job_state": "COMPLETED",
        "start": "2023-01-01T00:00:00",
    }

    response = client.get("/v0/job/query", params=params)

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for jid in data:
        r = client.get(f"/v0/job/id/{jid}")
        assert r.status_code == 200
        job = r.json()
        assert job["cluster_name"] == "raisin"
        assert job["job_state"] == "COMPLETED"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_no_filters(client):
    """Test jobs query without any filters."""
    response = client.get("/v0/job/query")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 24


@pytest.mark.usefixtures("read_only_db_with_users")
def test_count_jobs_by_cluster(client):
    """Test jobs count by cluster."""
    response = client.get("/v0/job/count?cluster=raisin")

    assert response.status_code == 200
    count = response.json()
    assert isinstance(count, int)
    assert count == 20


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_job_id(client):
    """Test jobs count by job ID."""
    response = client.get("/v0/job/count?job_id=10")

    assert response.status_code == 200
    count = response.json()
    assert isinstance(count, int)
    assert count == 1


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_user(client):
    """Test jobs count by username."""
    response = client.get("/v0/job/count?username=petitbonhomme")

    assert response.status_code == 200
    count = response.json()
    assert isinstance(count, int)
    assert count == 20


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_by_state(client):
    """Test jobs count by job state."""
    response = client.get("/v0/job/count?job_state=COMPLETED")

    assert response.status_code == 200
    count = response.json()
    assert isinstance(count, int)
    assert count == 1


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_empty_result(client):
    """Test jobs count with no results."""
    # Use a very high job ID that doesn't exist
    response = client.get("/v0/job/count?job_id=9999999999")

    assert response.status_code == 200
    count = response.json()
    assert count == 0


def test_count_jobs_invalid_cluster(client):
    """Test jobs count with invalid cluster."""
    response = client.get("/v0/job/count?cluster=invalid_cluster")

    assert response.status_code == 404
    data = response.json()
    assert "No such cluster 'invalid_cluster'" in data["detail"]


def test_count_jobs_invalid_job_state(client):
    """Test jobs count with invalid job state."""
    response = client.get("/v0/job/count?job_state=INVALID")

    assert response.status_code == 422


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_with_datetime_filters(client):
    """Test jobs count with start and end datetime filters."""
    params = {
        "start": "2023-01-01T00:00:00",
        "end": "2023-02-15T23:59:59",
    }

    response = client.get("/v0/job/count", params=params)

    assert response.status_code == 200
    count = response.json()
    assert isinstance(count, int)
    assert count == 8


@pytest.mark.usefixtures("read_only_db_with_users")
def test_count_jobs_multiple_filters(client):
    """Test jobs count with multiple filters."""
    params = {
        "cluster": "raisin",
        "job_state": "COMPLETED",
        "start": "2023-01-01T00:00:00",
    }

    response = client.get("/v0/job/count", params=params)

    assert response.status_code == 200
    count = response.json()
    assert isinstance(count, int)
    assert count > 0


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_no_filters(client):
    """Test jobs count without any filters."""
    response = client.get("/v0/job/count")

    assert response.status_code == 200
    count = response.json()
    assert isinstance(count, int)
    assert count == 24


@pytest.mark.usefixtures("read_only_db_with_users")
def test_count_jobs_matches_query_length(client):
    """Test that jobs count matches the length of jobs query results."""
    # Test with a specific filter
    response_query = client.get("/v0/job/query?cluster=raisin")
    response_count = client.get("/v0/job/count?cluster=raisin")

    assert response_query.status_code == 200
    assert response_count.status_code == 200

    jobs = response_query.json()
    count = response_count.json()

    assert len(jobs) == count


@pytest.mark.usefixtures("read_only_db_with_users")
def test_cluster_list(client):
    """Test cluster list."""
    response = client.get("/v0/cluster/list")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for cluster_name in ("raisin", "fromage", "patate"):
        assert cluster_name in data


def _gen_fake_rgus():
    """Mock for sarc.client.gpumetrics.get_rgus()"""
    return {
        "A100": 3.21,
        "gpu1": 1.5,
        "gpu_2": 4.5,
        "GPU 3": 4 * 7,
    }


@pytest.mark.usefixtures("read_only_db_with_users")
def test_gpu_rgu(client, monkeypatch):
    monkeypatch.setattr("sarc.api.v0.get_rgus", _gen_fake_rgus)

    response = client.get("/v0/gpu/rgu")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert data == _gen_fake_rgus()


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_display_name(client):
    response = client.get("/v0/user/query?display_name=janE")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0] == "1f9b04e5-0ec4-4577-9196-2b03d254e344"

    r = client.get(f"/v0/user/id/{data[0]}")
    assert r.status_code == 200
    user = r.json()
    assert isinstance(user, dict)
    assert user["display_name"] == "Jane Doe"
    assert user["uuid"] == data[0]
    assert user["email"] == "jdoe@example.com"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_email(client):
    response = client.get("/v0/user/query?email=bonhomme@mila.quebec")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0] == "5a8b9e7f-afcc-4ced-b596-44fcdb3a0cff"

    r = client.get(f"/v0/user/id/{data[0]}")
    assert r.status_code == 200
    user = r.json()
    assert isinstance(user, dict)
    assert user["display_name"] == "M/Ms Bonhomme"
    assert user["uuid"] == data[0]
    assert user["email"] == "bonhomme@mila.quebec"


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
def test_user_query_by_member_type_professor_now(client, freezer, date, expected):
    freezer.move_to(date)
    response = client.get("/v0/user/query?member_type=professor")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if expected:
        assert len(data) == 1
        assert sorted(data) == ["1f9b04e5-0ec4-4577-9196-2b03d254e344"]
    else:
        assert len(data) == 0


@pytest.mark.parametrize(
    "member_type,date,identifiers",
    [
        (
            "phd",
            datetime(2020, 8, 31, 23, 59, 59, 999999, tzinfo=UTC),
            ["7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"],
        ),
        (
            "staff",
            datetime(2022, 5, 1, tzinfo=UTC),
            ["8b4fef2b-8f47-4eb6-9992-3e7e1133b42a"],
        ),
    ],
)
@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_member_type_other_now(
    client, freezer, member_type, date, identifiers
):
    freezer.move_to(date)
    response = client.get(f"/v0/user/query?member_type={member_type}")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert sorted(data) == sorted(identifiers)


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
def test_user_query_by_supervisor_start(client, start, nb_expected):
    supervisor = "1f9b04e5-0ec4-4577-9196-2b03d254e344"
    response = client.get(
        "/v0/user/query",
        params={"supervisor": supervisor, "supervisor_start": start.isoformat()},
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == nb_expected
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
def test_user_query_by_supervisor_end(client, end, nb_expected):
    supervisor = "1f9b04e5-0ec4-4577-9196-2b03d254e344"
    response = client.get(
        "/v0/user/query",
        params={"supervisor": supervisor, "supervisor_end": end.isoformat()},
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == nb_expected
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
def test_user_query_by_cosupervisor_start_end(client, start, end, expected):
    cosupervisor = "1f9b04e5-0ec4-4577-9196-2b03d254e344"
    response = client.get(
        "/v0/user/query",
        params={
            "co_supervisor": cosupervisor,
            "co_supervisor_start": start.isoformat(),
            "co_supervisor_end": end.isoformat(),
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if expected:
        assert len(data) == 1
        assert data == ["8b4fef2b-8f47-4eb6-9992-3e7e1133b42a"]
    else:
        assert not data


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_by_cosupervisor_bad_start_end(client):
    start = datetime(2022, 1, 1, tzinfo=UTC)
    end = start - timedelta(days=1)
    response = client.get(
        "/v0/user/query",
        params={
            "co_supervisor": "1f9b04e5-0ec4-4577-9196-2b03d254e344",
            "co_supervisor_start": start.isoformat(),
            "co_supervisor_end": end.isoformat(),
        },
    )
    assert response.status_code == 400


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_empty_result(client):
    response = client.get("/v0/user/query?email=nothing@nothing.nothing")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 0


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_no_filters(client):
    response = client.get("/v0/user/query")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 10


@pytest.mark.usefixtures("read_only_db_with_users")
def test_user_query_multiple_filters(client):
    response = client.get(
        "/v0/user/query",
        params={
            "supervisor": "1f9b04e5-0ec4-4577-9196-2b03d254e344",
            "supervisor_start": datetime(2020, 1, 1, tzinfo=UTC),
            "display_name": "sMiTh",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0] == "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"

    r = client.get(f"/v0/user/id/{data[0]}")
    assert r.status_code == 200
    user = r.json()
    assert isinstance(user, dict)
    assert user["display_name"] == "John Smith"
    assert user["uuid"] == data[0]


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_uuid(client):
    response = client.get("/v0/user/id/7ecd3a8a-ab71-499e-b38a-ceacd91a99c4")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert data["email"] == "jsmith@example.com"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_uuid_invalid(client):
    response = client.get("/v0/user/id/invalid")
    assert response.status_code == 422


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_uuid_unknown(client):
    response = client.get("/v0/user/id/70000000-a000-4000-b000-c00000000000")
    assert response.status_code == 404


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_email(client):
    response = client.get("/v0/user/email/jsmith@example.com")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert data["uuid"] == "7ecd3a8a-ab71-499e-b38a-ceacd91a99c4"


@pytest.mark.usefixtures("read_only_db_with_users")
def test_get_user_by_email_unknown(client):
    response = client.get("/v0/user/email/unknown")
    assert response.status_code == 404
