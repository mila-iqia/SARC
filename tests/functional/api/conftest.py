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
