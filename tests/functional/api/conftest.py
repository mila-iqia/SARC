import socket

import gifnoc
import httpx
import pytest
from easy_oauth.testing.utils import OAuthMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sarc.config import config
from sarc.rest.client import SarcApiClient


@pytest.fixture(scope="session")
def oauth_port():
    """Return a random free TCP port for OAuth mock server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    return port


@pytest.fixture(scope="session")
def oauth_mock(oauth_port):
    with OAuthMock(port=oauth_port) as oauth:
        yield oauth


@pytest.fixture(scope="function")
def app(oauth_mock):
    from sarc.api.v0 import router

    def client(email=None):
        """Create a test client for the FastAPI app."""
        mc = ModifiedTestClient(app, oauth_mock)
        if email is not None:
            mc.set_email(email)
        return mc

    oauth_overrides = {
        "sarc.api.auth.server_metadata_url": f"http://127.0.0.1:{oauth_port}/.well-known/openid-configuration"
    }

    with gifnoc.overlay(oauth_overrides):
        app = FastAPI()
        app.client = client
        app.include_router(router)
        api_config = config().api
        assert api_config is not None
        if api_config.auth is not None:
            api_config.auth.install(app)

        yield app


class ModifiedTestClient(TestClient):
    def __init__(self, app, oauth_mock):
        self.normal = httpx.Client()
        self.oauth_mock = oauth_mock
        self.token = None
        super().__init__(app, follow_redirects=False)

    def get_token(self, email: str):
        self.oauth_mock.set_email(email)
        response = self.get("/token")
        assert response.status_code == 200
        return response.json()["refresh_token"]

    def set_email(self, email: str):
        self.token = self.get_token(email)
        self.headers["Authorization"] = f"Bearer {self.token}"

    def request(self, method: str, url: httpx.URL | str, **kwargs):
        url = httpx.URL(url)
        if url.scheme:
            result = self.normal.request(method, url, **kwargs)
        else:
            result = super().request(method, url, **kwargs)

        # If the result is a redirect, follow once
        if hasattr(result, "is_redirect") and result.is_redirect:
            location = result.headers.get("location")
            if location.startswith("http://testserver"):
                location = location[len("http://testserver") :]
            if location:
                # Should follow with the same method according to RFC for 303 (should convert POST to GET) and for 302/301 often treated as GET.
                # But we will re-request with GET as most clients do for non-307 (if method != 'GET')
                if result.status_code in (301, 302, 303):
                    return self.request("GET", location, **kwargs)
                else:
                    return self.request(method, location, **kwargs)
        return result

    def expect(self, response, expected=None):
        if expected is None:
            return response
        if response.status_code != expected:
            raise AssertionError(
                f"Expected status {expected}, got {response.status_code}: {response.text}"
            )
        return response

    def get(self, *args, expect_status=None, **kwargs):
        response = super().get(*args, **kwargs)
        return self.expect(response, expect_status)

    def post(self, *args, expect_status=None, **kwargs):
        response = super().post(*args, **kwargs)
        return self.expect(response, expect_status)

    def delete(self, *args, expect_status=None, **kwargs):
        response = super().delete(*args, **kwargs)
        return self.expect(response, expect_status)


@pytest.fixture
def client(app, oauth_mock):
    """Create a test client for the FastAPI app."""
    mc = ModifiedTestClient(app, oauth_mock)
    mc.set_email("admin@admin.admin")
    return mc


@pytest.fixture
def sarc_client(app, oauth_mock):
    """
    Returns a SarcApiClient that uses the FastAPI TestClient session.
    The 'client' fixture comes from tests/functional/api/conftest.py
    and is connected to the app with the database fixtures active.

    Used to test low-level SarcApiClient methods.
    """
    mc = ModifiedTestClient(app, oauth_mock)
    return SarcApiClient(
        remote_url="", session=mc, oauth2_token=mc.get_token("admin@admin.admin")
    )


@pytest.fixture
def enable_caps(app):
    from sarc.api.v0 import can_query, is_admin

    def yes():
        return True

    def fake_user():
        return "doej@mila.quebec"

    app.dependency_overrides[is_admin] = yes
    app.dependency_overrides[can_query] = fake_user

    try:
        yield
    finally:
        del app.dependency_overrides[can_query]
        del app.dependency_overrides[is_admin]


@pytest.fixture
def mock_client_class(app, oauth_mock, monkeypatch):
    """
    Replace SarcApiClient class with a mock class
    that uses the FastAPI TestClient session.

    Used to test high-level REST client functions.
    """
    mc = ModifiedTestClient(app, oauth_mock)

    class MockSarcApiClient(SarcApiClient):
        def __init__(self, *args, **kwargs):
            super().__init__(
                remote_url="",
                session=mc,
                oauth2_token=mc.get_token("admin@admin.admin"),
            )

    monkeypatch.setattr("sarc.rest.client.SarcApiClient", MockSarcApiClient)
    monkeypatch.setattr("sarc.rest.client.SarcApiClient", MockSarcApiClient)
