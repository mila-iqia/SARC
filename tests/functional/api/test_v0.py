import pytest
from fastapi.testclient import TestClient

from sarc.api.v0 import router


@pytest.fixture
def app():
    """Create a FastAPI test app with the v0 router."""
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(router)
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


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_cluster(client):
    """Test successful jobs query by cluster."""
    response = client.get("/v0/job/query?cluster=raisin")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for job in data:
        assert job["cluster_name"] == "raisin"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_job_id(client):
    """Test jobs query by job ID."""
    response = client.get("/v0/job/query?job_id=10")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for job in data:
        assert job["job_id"] == 10


@pytest.mark.usefixtures("read_only_db")
def test_get_job(client):
    """Test jobs query by job ID."""
    response = client.get("/v0/job/query?job_id=1")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    db_id = data[0]["id"]
    response = client.get(f"/v0/job/id/{db_id}")

    assert response.status_code == 200
    data = response.json()

    assert data["job_id"] == 1


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_user(client):
    """Test jobs query by username."""
    response = client.get("/v0/job/query?username=petitbonhomme")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for job in data:
        assert job["user"] == "petitbonhomme"


@pytest.mark.usefixtures("read_only_db")
def test_get_jobs_by_state(client):
    """Test jobs query by job state."""
    response = client.get("/v0/job/query?job_state=COMPLETED")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    for job in data:
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


@pytest.mark.usefixtures("read_only_db")
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
    for job in data:
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
