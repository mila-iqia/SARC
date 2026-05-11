from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import DataError

from sarc.alerts.common import HealthCheck
from sarc.alerts.healthcheck_state import HealthCheckState, HealthCheckStateRepository
from sarc.config import UTC, config
from sarc.models.api import SlurmJobList, UserList
from sarc.models.cluster import SlurmCluster
from tests.common.dateutils import _iso_mtl
from tests.unittests.alerts.definitions import BeanCheck


@pytest.mark.usefixtures("read_only_db")
def test_get_job_not_found(client):
    """Test job not found (string, bad ID format) returns 422."""
    client.get("/v0/job/id/not_found", expect_status=422)


@pytest.mark.usefixtures("read_only_db")
def test_get_job_not_found_id(client):
    """Test job not found (int, good format) returns 404."""
    oid = 999_999
    client.get(f"/v0/job/id/{oid}", expect_status=404)


@pytest.fixture
def paginator(client):
    def query(endpoint, limit, sort_field, **params):
        entries = []
        cursor = None
        for i in range(50):
            print(cursor)
            q = {**params, "limit": limit, "cursor": cursor}
            response = client.get(endpoint, params=q, expect_status=200)
            data = response.json()
            cursor = data["cursor"]
            if cursor is False:
                break
            results = data["results"]
            assert len(results) <= limit
            entries.extend(results)
        else:
            raise Exception(
                "There's a problem, tests should not try to fetch more than 50 pages"
            )

        reverse = sort_field.startswith("-")
        sort_field = sort_field.lstrip("-")
        assert sorted(entries, key=lambda x: x[sort_field], reverse=reverse) == entries
        ids = [x["id"] for x in entries]
        assert len(set(ids)) == len(ids), "Duplicated results in paginated queries"
        return ids

    return query


def query_fixture(t, type_name, endpoint):
    def fixture(client):
        def query(*, n=True, query="", expect_status=200, **params):
            response = client.get(
                f"{endpoint}{query}", expect_status=expect_status, params=params or None
            )
            if expect_status != 200:
                return response.json()
            data = t.model_validate(response.json())
            if n is True:
                assert len(data.results) > 0, f"Expected at least one {type_name}"
            elif n is not False:
                assert len(data.results) == n, f"Expected exactly {n} {type_name}s"
            return data.results

        return query

    return pytest.fixture(fixture)


jobq = query_fixture(t=SlurmJobList, type_name="job", endpoint="/v0/job/query")
userq = query_fixture(t=UserList, type_name="user", endpoint="/v0/user/query")


def _ids(jobs):
    return sorted([j.id for j in jobs])


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_cluster(jobq):
    """Test successful jobs query by cluster."""
    jobs = jobq(cluster_name="raisin")
    assert all(j.cluster_id == 7 for j in jobs)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_job_id(jobq):
    """Test jobs query by job ID."""
    jobs = jobq(job_id="10", n=1)
    assert _ids(jobs) == [10]


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_user(jobq):
    """Test jobs query by cluster_user."""
    jobs = jobq(cluster_user="beaubonhomme")
    assert [j.cluster_user == "beaubonhomme" for j in jobs]
    assert _ids(jobs) == [18]


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_email(jobq):
    """Test jobs query by user email."""
    jobs = jobq(email="petitbonhomme@mila.quebec")
    assert len(jobs) == 21
    assert all(j.cluster_user == "petitbonhomme" for j in jobs)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_sarc_user_id(jobq):
    """Test jobs query by sarc_user_id."""
    jobs = jobq(sarc_user_id="9")
    assert len(jobs) == 21
    assert all(j.cluster_user == "petitbonhomme" for j in jobs)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_extra_fields(jobq):
    """Test jobs query returns all requested extra fields."""
    (job,) = jobq(
        cluster_user="beaubonhomme", extra_fields="cluster_name,sarc_user,statistics"
    )
    assert job.cluster_name == "raisin"
    assert job.sarc_user is not None
    assert job.sarc_user.id == 10
    assert job.sarc_user.email == "beaubonhomme@mila.quebec"
    # TODO: factory should generate some statistics
    assert isinstance(job.statistics, dict)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_state(jobq):
    """Test jobs query by job state."""
    jobs = jobq(job_state="COMPLETED")
    assert [j.job_state == "COMPLETED" for j in jobs]


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_multiple_job_ids(jobq):
    """Test jobs query with multiple job IDs."""
    jobs = jobq(query="?job_id=10&job_id=15", n=2)
    assert _ids(jobs) == [10, 15]


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_empty_job_id_list(jobq):
    """Test jobs query with an empty job_id list.
    SarcApiClient sends job_id= for an empty list.
    """
    jobq(query="?job_id=", n=0)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_invalid_job_id(jobq):
    """Test jobs query with invalid job ID."""
    jobq(job_id="not_an_int", expect_status=422)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_empty_result(jobq):
    """Test jobs query with no results."""
    # Use a job ID that doesn't exist
    jobq(job_id="999999", n=0)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_resubmitted(jobq):
    """Test resubmitted job."""
    jobq(job_id="1_000_000", n=2)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_invalid_cluster(jobq):
    """Test jobs query with invalid cluster."""
    err = jobq(cluster_name="invalid_cluster", expect_status=404)
    assert "No such cluster 'invalid_cluster'" in err["detail"]


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_invalid_job_state(jobq):
    """Test jobs query with invalid job state."""
    jobq(job_state="CHLORINE", expect_status=422)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_with_naive_datetime_filters(client):
    """Test jobs query with start and end datetime filters."""
    params = {"start": "2023-01-01T00:00:00", "end": "2023-12-31T23:59:59"}

    response = client.get("/v0/job/query", params=params, expect_status=422)
    assert (
        "Time-aware datetime required. E.g: 2025-01-01T10:00Z (UTC), 2025-01-01T05:00-05:00 (UTC-5 hours)"
        in response.text
    )


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_with_datetime_filters(jobq):
    """Test jobs query with start and end datetime filters."""
    assert jobq(
        start=_iso_mtl("2023-01-01T00:00:00"), end=_iso_mtl("2023-12-31T23:59:59")
    )


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_multiple_filters(jobq):
    """Test jobs query with multiple filters."""
    start = _iso_mtl("2023-01-01T00:00:00")
    jobs = jobq(cluster_name="raisin", job_state="COMPLETED", start=start)
    assert all(
        j.cluster_id == 7 and j.job_state == "COMPLETED" and str(j.start_time) >= start
        for j in jobs
    )


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_no_filters(jobq):
    """Test jobs query without any filters."""
    jobq(n=22)


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_pagination(paginator):
    """Test jobs query with pagination."""
    results = paginator("/v0/job/query", limit=2, sort_field="-submit_time")
    assert len(results) == 22


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_invalid_pagination(client):
    """Test jobs query with invalid pagination parameters."""
    # Limit < 1
    with pytest.raises(ValidationError):
        client.get("/v0/job/query?limit=0")
    # Limit > MAX
    with pytest.raises(ValidationError):
        client.get("/v0/job/query?limit=101")


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "params,expected",
    [
        ({}, 22),
        ({"cluster_name": "raisin"}, 19),
        ({"job_id": "10"}, 1),
        ({"job_id": "999999"}, 0),
        ({"cluster_user": "petitbonhomme"}, 21),
        ({"cluster_user": "beaubonhomme"}, 1),
        ({"job_state": "COMPLETED"}, 1),
        (
            {
                "start": _iso_mtl("2023-01-01T00:00:00"),
                "end": _iso_mtl("2023-02-15T23:59:59"),
            },
            8,
        ),
        (
            {
                "cluster_name": "raisin",
                "job_state": "COMPLETED",
                "start": _iso_mtl("2023-01-01T00:00:00"),
            },
            1,
        ),
    ],
)
def test_count_jobs(client, params, expected):
    response = client.get("/v0/job/count", params=params, expect_status=200)
    count = response.json()
    assert count == expected, params


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_invalid_cluster(client):
    """Test jobs count with invalid cluster."""
    response = client.get(
        "/v0/job/count?cluster_name=invalid_cluster", expect_status=404
    )

    data = response.json()
    assert "No such cluster 'invalid_cluster'" in data["detail"]


@pytest.mark.usefixtures("read_only_db")
def test_count_jobs_invalid_job_state(client):
    """Test jobs count with invalid job state."""
    client.get("/v0/job/count?job_state=BICARBONATE", expect_status=422)


@pytest.mark.usefixtures("read_only_db")
def test_cluster_list(client):
    """Test cluster list."""
    response = client.get("/v0/cluster/list", expect_status=200)

    data = [SlurmCluster.model_validate(cl) for cl in response.json()]
    assert {cl.name for cl in data} == {
        "fromage",
        "gerudo",
        "hyrule",
        "local",
        "mila",
        "patate",
        "raisin",
        "raisin_no_prometheus",
    }


def _gen_fake_rgus():
    """Mock for sarc.client.gpumetrics.get_rgus()"""
    return {"A100": 3.21, "gpu1": 1.5, "gpu_2": 4.5, "GPU 3": 4 * 7}


@pytest.mark.usefixtures("read_only_db")
def test_gpu_rgu(client, monkeypatch):
    monkeypatch.setattr("sarc.api.v0.get_rgus", _gen_fake_rgus)

    response = client.get("/v0/gpu/rgu", expect_status=200)
    data = response.json()
    assert isinstance(data, dict)
    assert data == _gen_fake_rgus()


@pytest.mark.usefixtures("read_only_db")
def test_user_query_by_display_name(userq):
    users = userq(display_name="janE")
    assert _ids(users) == [1]
    (user,) = users
    assert user.display_name == "Jane Doe"
    assert user.email == "jdoe@example.com"


@pytest.mark.usefixtures("read_only_db")
def test_user_query_by_email(userq):
    users = userq(email="bonhomme@mila.quebec")
    assert _ids(users) == [8]
    (user,) = users
    assert user.display_name == "M/Ms Bonhomme"
    assert user.email == "bonhomme@mila.quebec"


@pytest.mark.parametrize(
    "member_type,date,expected",
    [
        # After 2026/05/01 and until 2027/09/01, there should be only 1 match
        ("professor", datetime(2027, 8, 31, tzinfo=UTC), [1]),
        ("professor", datetime(2027, 8, 20, tzinfo=UTC), [1]),
        ("professor", datetime(2026, 5, 2, tzinfo=UTC), [1]),
        # Before 2020/09/01 and after 2027/09/01, there should be not match
        ("professor", datetime(2005, 8, 31, tzinfo=UTC), []),
        ("professor", datetime(2010, 8, 31, tzinfo=UTC), []),
        ("professor", datetime(2018, 8, 31, tzinfo=UTC), []),
        ("professor", datetime(2020, 8, 31, 23, 59, 59, tzinfo=UTC), []),
        ("professor", datetime(2027, 9, 1, 0, 0, 1, tzinfo=UTC), []),
        ("professor", datetime(2027, 12, 1, tzinfo=UTC), []),
        ("professor", datetime(2044, 7, 1, tzinfo=UTC), []),
        # Others
        ("phd", datetime(2020, 8, 31, 23, 59, 59, 999999, tzinfo=UTC), [2]),
        ("staff", datetime(2022, 5, 1, tzinfo=UTC), [7]),
    ],
)
@pytest.mark.usefixtures("read_only_db")
def test_user_query_by_member_type_now(userq, freezer, member_type, date, expected):
    freezer.move_to(date)
    users = userq(member_type=member_type, n=False)
    assert _ids(users) == expected


@pytest.mark.parametrize(
    "start,supervisor,expected",
    [
        # Start old enough to get all supervised users (3 users)
        (datetime(2005, 1, 1, tzinfo=UTC), 1, [2, 5, 7]),
        (datetime(2018, 8, 31, 23, 59, 59, 999999, tzinfo=UTC), 1, [2, 5, 7]),
        (datetime(2018, 9, 1, tzinfo=UTC), 1, [2, 5, 7]),
        (datetime(2019, 9, 1, tzinfo=UTC), 1, [2, 5, 7]),
        (datetime(2020, 9, 1, tzinfo=UTC), 1, [2, 5, 7]),
        # From these start dates, we get only 2 supervised users
        (datetime(2021, 5, 2, 0, 0, 0, 1, tzinfo=UTC), 1, [5, 7]),
        (datetime(2022, 1, 1, tzinfo=UTC), 1, [5, 7]),
        (datetime(2022, 10, 1, tzinfo=UTC), 1, [5, 7]),
        (datetime(2022, 5, 1, tzinfo=UTC), 1, [5, 7]),
        # From these start dates, there is no more user supervised by this prof.
        (datetime(2023, 1, 1, 0, 0, 1, tzinfo=UTC), 1, []),
        (datetime(2025, 1, 1, tzinfo=UTC), 1, []),
        (datetime(2035, 1, 1, tzinfo=UTC), 1, []),
    ],
)
@pytest.mark.usefixtures("read_only_db")
def test_user_query_by_supervisor_start(userq, start, supervisor, expected):
    users = userq(supervisor=supervisor, start=start.isoformat(), n=False)
    assert _ids(users) == expected


@pytest.mark.parametrize(
    "end,supervisor,expected",
    [
        # End new enough to get all supervised users (2 users)
        (datetime(2035, 1, 1, tzinfo=UTC), 1, [2, 5, 7]),
        (datetime(2025, 1, 1, tzinfo=UTC), 1, [2, 5, 7]),
        (datetime(2023, 1, 1, tzinfo=UTC), 1, [2, 5, 7]),
        (datetime(2022, 5, 1, tzinfo=UTC), 1, [2, 5, 7]),
        (datetime(2022, 1, 2, tzinfo=UTC), 1, [2, 5, 7]),
        # Until these end dates, we get only 1 supervised user
        (datetime(2021, 4, 1, tzinfo=UTC), 1, [2]),
        (datetime(2020, 1, 1, tzinfo=UTC), 1, [2]),
        (datetime(2019, 1, 1, tzinfo=UTC), 1, [2]),
        (datetime(2018, 9, 2, tzinfo=UTC), 1, [2]),
        # Until these end dates, there is no more user supervised by this prof.
        (datetime(2018, 8, 31, 23, 59, 59, 9999, tzinfo=UTC), 1, []),
        (datetime(2017, 1, 1, tzinfo=UTC), 1, []),
        (datetime(2005, 1, 1, tzinfo=UTC), 1, []),
    ],
)
@pytest.mark.usefixtures("read_only_db")
def test_user_query_by_supervisor_end(userq, end, supervisor, expected):
    users = userq(supervisor=supervisor, end=end.isoformat(), n=False)
    assert _ids(users) == expected


@pytest.mark.usefixtures("read_only_db")
def test_user_query_by_supervisor_bad_start_end(userq):
    start = datetime(2022, 1, 1, tzinfo=UTC)
    end = start - timedelta(days=1)
    with pytest.raises(DataError):
        userq(supervisor=1, start=start.isoformat(), end=end.isoformat())


@pytest.mark.usefixtures("read_only_db")
def test_user_query_empty_result(userq):
    userq(email="nothing@nothing.nothing", n=0)


@pytest.mark.usefixtures("read_only_db")
def test_user_query_no_filters(userq):
    users = userq()
    assert len(users) == 10


@pytest.mark.usefixtures("read_only_db")
def test_user_query_multiple_filters(userq):
    users = userq(
        supervisor=1, start=datetime(2020, 1, 1, tzinfo=UTC), display_name="sMiTh"
    )
    assert _ids(users) == [2]
    (user,) = users
    assert user.display_name == "John Smith"


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_uuid(client):
    response = client.get("/v0/user/id/2", expect_status=200)
    data = response.json()
    assert isinstance(data, dict)
    assert data["email"] == "jsmith@example.com"


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_uuid_invalid(client):
    client.get("/v0/user/id/invalid", expect_status=422)


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_uuid_unknown(client):
    client.get("/v0/user/id/999999", expect_status=404)


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_email(client):
    response = client.get("/v0/user/email/jsmith@example.com", expect_status=200)
    data = response.json()
    assert isinstance(data, dict)
    assert data["id"] == 2


@pytest.mark.usefixtures("read_only_db")
def test_get_user_by_email_unknown(client):
    client.get("/v0/user/email/unknown", expect_status=404)


@pytest.mark.usefixtures("read_only_db")
def test_user_pagination(paginator):
    """Test user list with pagination."""
    results = paginator("/v0/user/query", limit=2, sort_field="id")
    assert len(results) == 10


@pytest.mark.usefixtures("read_only_db")
def test_user_pagination_offset(userq):
    """Test user list with pagination."""
    users = userq(limit=5)
    assert len(users) == 5

    users = userq(limit=3, cursor=3)
    assert len(users) == 3

    users = userq(limit=3, cursor=9)
    assert len(users) == 1

    users = userq(limit=10, cursor=1000, n=False)
    assert len(users) == 0

    # Invalid parameters
    with pytest.raises(ValidationError):
        userq(limit=0)


@pytest.mark.usefixtures("empty_read_write_db")
def test_health_list_empty(client):
    response = client.get("/v0/health/list")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.usefixtures("empty_read_write_db")
def test_health_list_with_states(client):
    db = config().mongo.database_instance
    repo = HealthCheckStateRepository(db)

    hc_a = HealthCheck(name="alpha_check", active=True)
    hc_b = BeanCheck(name="bravo_check", active=True, beans=14)

    repo.save(HealthCheckState(check=hc_b, last_result=hc_b.ok(), last_message="OK"))
    repo.save(HealthCheckState(check=hc_a, last_result=hc_a.fail()))

    response = client.get("/v0/health/list")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    # Sorted by check name
    assert data[0]["check"]["name"] == "alpha_check"
    assert data[1]["check"]["name"] == "bravo_check"
    assert "beans" in data[1]["check"]
    assert data[1]["check"]["beans"] == 14
    # Verify result details
    assert data[0]["last_result"]["status"] == "failure"
    assert data[0]["last_message"] is None
    assert data[1]["last_result"]["status"] == "ok"
    assert data[1]["last_message"] == "OK"


@pytest.mark.usefixtures("empty_read_write_db")
def test_health_list_with_error_trace(client):
    db = config().mongo.database_instance
    repo = HealthCheckStateRepository(db)

    hc = BeanCheck(name="evil_check", active=True, beans=666)
    result = hc()
    repo.save(HealthCheckState(check=hc, last_result=result, last_message="error"))

    response = client.get("/v0/health/list")
    assert response.status_code == 200
    (state,) = response.json()
    assert state["last_result"]["status"] == "error"
    exc = state["last_result"]["exception"]
    assert exc["type"] == "ValueError"
    assert exc["message"] == "What a beastly number"
    assert len(exc["trace"]) > 0
    frame = exc["trace"][-1]
    assert frame["filename"].endswith("definitions.py")
    assert frame["code"] == 'raise ValueError("What a beastly number")'
    assert isinstance(frame["line"], int)
