"""Functional tests for the public ``POST /upload-report`` endpoint."""

import gifnoc
import pytest
from fastapi.testclient import TestClient

from sarc.api import reports


@pytest.fixture(autouse=True)
def fresh_limiter():
    """The endpoint's limiter is module-level state: isolate tests."""
    reports.limiter.reset()
    yield
    reports.limiter.reset()


@pytest.fixture
def reports_dir(tmp_path):
    """Point the storage directory at ``tmp_path`` and return the report
    directory (``<storage>/reports/user-reports``, where uploads land)."""
    with gifnoc.overlay({"sarc.storage": str(tmp_path)}):
        yield tmp_path / "reports" / "user-reports"


@pytest.fixture
def anon_client(app):
    """A client with no OAuth token: the endpoint must be public."""
    return TestClient(app)


class FakeClock:
    """Stand-in for ``reports._now`` so tests control the limiter clock."""

    def __init__(self, start=0.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def fake_clock(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(reports, "_now", clock)
    return clock


def post_report(
    client, filename="report.md", content=b"# Report\n", category="general"
):
    params = {} if category is None else {"category": category}
    return client.post(
        "/upload-report",
        params=params,
        files={"file": (filename, content, "text/markdown")},
    )


def test_upload_ok(anon_client, reports_dir):
    response = post_report(anon_client, content=b"# My report\n")
    assert response.status_code == 201
    body = response.json()
    assert body["size"] == len(b"# My report\n")
    assert body["category"] == "general"
    assert body["filename"].startswith("report-")
    assert body["filename"].endswith(".md")
    saved = reports_dir / "general" / body["filename"]
    assert saved.read_bytes() == b"# My report\n"


def test_category_is_required(anon_client, reports_dir):
    response = post_report(anon_client, category=None)
    assert response.status_code == 422
    assert not reports_dir.exists()
    # FastAPI rejects the malformed request before the endpoint runs, so no
    # slot was spent and a proper upload still goes through.
    assert post_report(anon_client).status_code == 201


def test_rejects_non_md(anon_client, reports_dir):
    response = post_report(anon_client, filename="report.txt")
    assert response.status_code == 400
    assert not reports_dir.exists()


def test_sanitizes_filename(anon_client, reports_dir):
    response = post_report(anon_client, filename="../../..//etc/passwd.md")
    assert response.status_code == 201
    filename = response.json()["filename"]
    assert "/" not in filename
    assert (reports_dir / "general" / filename).exists()


def test_category_subdirectory(anon_client, reports_dir):
    with gifnoc.overlay(
        {"sarc.server.report_upload": {"categories": ["daily", "weekly"]}}
    ):
        response = post_report(anon_client, category="daily")
    assert response.status_code == 201
    body = response.json()
    assert body["category"] == "daily"
    assert (reports_dir / "daily" / body["filename"]).read_bytes() == b"# Report\n"


def test_unknown_category_rejected(anon_client, reports_dir):
    response = post_report(anon_client, category="secret")
    assert response.status_code == 400
    assert not reports_dir.exists()
    # A bad category is charged like any failed upload: slot spent.
    response = post_report(anon_client)
    assert response.status_code == 429


def test_category_traversal_rejected(anon_client, reports_dir):
    # Not on the whitelist (and not a plain name): refused either way.
    response = post_report(anon_client, category="../../etc")
    assert response.status_code == 400
    assert not reports_dir.exists()


def test_categories_are_configurable(anon_client, reports_dir):
    with gifnoc.overlay(
        {
            "sarc.server.report_upload": {
                "categories": ["gpu", "storage"],
                "max_per_ip": 10,
            }
        }
    ):
        assert post_report(anon_client, category="gpu").status_code == 201
        assert post_report(anon_client, category="storage").status_code == 201
        # The previously-default category is off-whitelist now.
        assert post_report(anon_client, category="general").status_code == 400


def test_category_config_validation():
    from sarc.config import ConfigurationError, ReportUploadConfig

    for bad in ["../escape", "a/b", "/abs", ".", ".."]:
        with pytest.raises(ConfigurationError, match="category"):
            ReportUploadConfig(categories=[bad])


def test_rejects_oversize(anon_client, reports_dir):
    with gifnoc.overlay({"sarc.server.report_upload": {"max_size": 10}}):
        response = post_report(anon_client, content=b"x" * 11)
    assert response.status_code == 413
    assert not reports_dir.exists()
    # A failed upload still counts: the caller's slot is spent.
    response = post_report(anon_client)
    assert response.status_code == 429


def test_rejects_when_storage_disabled(anon_client):
    with gifnoc.overlay({"sarc.storage": None}):
        response = post_report(anon_client)
    assert response.status_code == 503


def test_per_ip_rate_limit(anon_client, reports_dir, fake_clock):
    assert post_report(anon_client).status_code == 201
    response = post_report(anon_client)
    assert response.status_code == 429
    assert int(response.headers["retry-after"]) >= 1

    fake_clock.advance(59)
    assert post_report(anon_client).status_code == 429
    fake_clock.advance(2)  # past the 60s window
    assert post_report(anon_client).status_code == 201


def test_global_rate_limit(anon_client, reports_dir, fake_clock):
    with gifnoc.overlay(
        {"sarc.server.report_upload": {"max_per_ip": 10, "max_total": 2}}
    ):
        assert post_report(anon_client).status_code == 201
        assert post_report(anon_client).status_code == 201
        assert post_report(anon_client).status_code == 429

        fake_clock.advance(61)  # the whole window expires
        assert post_report(anon_client).status_code == 201


def test_limits_are_configurable(anon_client, reports_dir, fake_clock):
    with gifnoc.overlay({"sarc.server.report_upload": {"max_per_ip": 3}}):
        for _ in range(3):
            assert post_report(anon_client).status_code == 201
        assert post_report(anon_client).status_code == 429


def test_limiter_isolates_keys():
    limiter = reports.RateLimiter()
    assert limiter.try_acquire("a", 0.0, 1, 100, 60.0) is None
    assert limiter.try_acquire("a", 1.0, 1, 100, 60.0) is not None
    assert limiter.try_acquire("b", 1.0, 1, 100, 60.0) is None


def test_limiter_global_budget_shared():
    limiter = reports.RateLimiter()
    assert limiter.try_acquire("a", 0.0, 10, 2, 60.0) is None
    assert limiter.try_acquire("b", 0.0, 10, 2, 60.0) is None
    assert limiter.try_acquire("c", 0.0, 10, 2, 60.0) is not None


def test_limiter_sweeps_idle_keys():
    limiter = reports.RateLimiter()
    limiter.try_acquire("a", 0.0, 5, 100, 60.0)
    limiter.try_acquire("b", 10.0, 5, 100, 60.0)
    # Accepts trigger the periodic sweep; "a" has been idle past the window.
    limiter.try_acquire("b", 120.0, 5, 100, 60.0)
    assert "a" not in limiter._per_key
    assert "b" in limiter._per_key
