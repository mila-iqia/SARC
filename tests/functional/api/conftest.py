import pytest
from fastapi.testclient import TestClient

from sarc.api.v0 import router
from sarc.rest.client import SarcApiClient


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


@pytest.fixture
def sarc_client(client):
    """
    Returns a SarcApiClient that uses the FastAPI TestClient session.
    The 'client' fixture comes from tests/functional/api/conftest.py
    and is connected to the app with the database fixtures active.

    Used to test low-level SarcApiClient methods.
    """
    return SarcApiClient(remote_url="http://testserver", session=client)


@pytest.fixture
def mock_client_class(client, monkeypatch):
    """
    Replace SarcApiClient class with a mock class
    that uses the FastAPI TestClient session.

    Used to test high-level REST client functions.
    """

    class MockSarcApiClient(SarcApiClient):
        def __init__(self, *args, **kwargs):
            super().__init__(remote_url="http://testserver", session=client)

    monkeypatch.setattr("sarc.rest.client.SarcApiClient", MockSarcApiClient)
