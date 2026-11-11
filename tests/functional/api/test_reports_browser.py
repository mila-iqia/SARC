"""Functional tests for the admin-only ``/reports`` browse interface.

Reports are written straight into the storage overlay (the upload endpoint
has its own tests). Access control mirrors test_metrics: guests get a 307 to
``/login``, signed-in non-admins a 403, and admins get the pages.
"""

import os

import gifnoc
import pytest
from fastapi.testclient import TestClient

_ADMIN = "admin@admin.admin"
_USER = "petitbonhomme@mila.quebec"
_NOT_IN_DB = "unknown-user@mila.quebec"

# Every browser endpoint with the params it needs; reused by the access
# matrix. The content does not matter there: the gate answers first.
_PAGES = [
    ("home", "/reports", {}),
    ("category", "/reports", {"category": "general"}),
    ("view", "/reports", {"category": "general", "file": "a.md"}),
    ("download", "/reports/download", {"category": "general", "file": "a.md"}),
]


@pytest.fixture
def storage(tmp_path):
    """Point the storage directory at ``tmp_path`` and return the reports
    root uploads land in (``<storage>/<report_upload.subdirectory>``; derived
    from the config so a default change cannot silently desync this fixture)."""
    from sarc.config import config

    with gifnoc.overlay({"sarc.storage": str(tmp_path)}):
        yield tmp_path / config.server.report_upload.subdirectory


@pytest.fixture
def admin(app, storage):
    return app.client(_ADMIN)


def add_report(root, category, name, content=b"# Report\n"):
    """Store a report the way the upload endpoint would."""
    directory = root / category
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_bytes(content)


# === Access control =========================================================


@pytest.mark.parametrize(
    "path,params", [(p, q) for _, p, q in _PAGES], ids=[n for n, _, _ in _PAGES]
)
def test_guest_redirected_to_login(app, path, params):
    """The login redirect runs before the admin gate, so a guest browser is
    sent to OAuth rather than getting a bare 403. Needs no DB."""
    resp = TestClient(app, follow_redirects=False).get(path, params=params)
    assert resp.status_code == 307
    assert resp.headers["location"].endswith("/login")


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "path,params", [(p, q) for _, p, q in _PAGES], ids=[n for n, _, _ in _PAGES]
)
@pytest.mark.parametrize(
    "email", [pytest.param(_USER, id="user"), pytest.param(_NOT_IN_DB, id="not_in_db")]
)
def test_non_admin_forbidden(app, path, params, email):
    """Signed in but without the admin capability: 403, on every page."""
    app.client(email).get(path, params=params, expect_status=403)


def test_503_without_storage(app):
    """No storage directory configured (the test-config default): 503, like
    the upload endpoint."""
    app.client(_ADMIN).get("/reports", expect_status=503)


# === Browsing ===============================================================


def test_home_lists_categories(admin, storage):
    add_report(storage, "general", "a.md")
    add_report(storage, "daily", "b.md")
    add_report(storage, "daily", "b.log")  # not a report
    add_report(storage, ".hidden", "sneaky.md")  # not a browsable category

    body = admin.get("/reports", expect_status=200).text
    assert "?category=general" in body
    assert "?category=daily" in body
    assert ".hidden" not in body
    assert "sneaky.md" not in body


def test_home_empty(admin, storage):
    """Storage configured but nothing uploaded yet (no directory at all):
    the page says so rather than failing."""
    body = admin.get("/reports", expect_status=200).text
    assert "No reports uploaded yet" in body


def test_category_lists_reports_newest_first(admin, storage):
    add_report(storage, "general", "old.md")
    os.utime(storage / "general" / "old.md", (1_600_000_000, 1_600_000_000))
    add_report(storage, "general", "new.md")
    add_report(storage, "general", "notes.txt")  # not a report

    body = admin.get("/reports", params={"category": "general"}, expect_status=200).text
    assert body.index("new.md") < body.index("old.md")
    assert "notes.txt" not in body


def test_file_without_category_rejected(admin, storage):
    admin.get("/reports", params={"file": "a.md"}, expect_status=400)


# === Viewing ================================================================


def test_view_renders_markdown_safely(admin, storage):
    """Reports are public uploads read on an admin page: raw HTML is
    escaped and dangerous link targets neutralized; real Markdown (headings,
    tables) is rendered."""
    add_report(
        storage,
        "general",
        "a.md",
        b"# Title\n\n"
        b"<script>alert(1)</script>\n\n"
        b"[click](javascript:alert(1))\n\n"
        b"| a | b |\n| - | - |\n| 1 | 2 |\n",
    )
    body = admin.get(
        "/reports", params={"category": "general", "file": "a.md"}, expect_status=200
    ).text
    assert "<h1>Title</h1>" in body
    assert "<table>" in body
    assert "<script>alert" not in body
    assert "javascript:" not in body


# === Downloading ============================================================


def test_download(admin, storage):
    add_report(storage, "general", "a.md", b"# Hi\n")
    resp = admin.get(
        "/reports/download", params={"category": "general", "file": "a.md"}
    )
    assert resp.status_code == 200
    assert resp.content == b"# Hi\n"
    assert "attachment" in resp.headers["content-disposition"]
    assert "text/markdown" in resp.headers["content-type"]


# === Unknown and hostile paths ==============================================


@pytest.mark.parametrize(
    "params",
    [
        pytest.param({"category": "nope"}, id="unknown"),
        pytest.param({"category": ""}, id="empty"),
        pytest.param({"category": ".."}, id="dotdot"),
        pytest.param({"category": "../../etc"}, id="escapes_root"),
    ],
)
def test_bad_category_404(admin, storage, params):
    """A category that is not a directory inside the reports root — unknown,
    empty, or a traversal — is a plain 404, never a listing of anything
    outside it."""
    add_report(storage, "general", "a.md")
    (storage.parent / "outside.md").write_bytes(b"SECRET CONTENT")

    resp = admin.get("/reports", params=params)
    assert resp.status_code == 404
    assert "SECRET CONTENT" not in resp.text


_FILE_CASES = [
    pytest.param({"category": "general", "file": "nope.md"}, id="unknown_file"),
    pytest.param({"category": "general", "file": "../outside.md"}, id="escaping_file"),
    pytest.param({"category": "general", "file": "outside.txt"}, id="non_md_file"),
    pytest.param({"category": "..", "file": "outside.md"}, id="dotdot_category"),
    pytest.param({"category": "general", "file": "..\\..\\outside.md"}, id="backslash"),
]


@pytest.mark.parametrize(
    "path", ["/reports", "/reports/download"], ids=["view", "download"]
)
@pytest.mark.parametrize("params", _FILE_CASES)
def test_unknown_or_hostile_report_404(admin, storage, path, params):
    """Nothing outside the reports root (``<storage>/<subdirectory>``) is
    reachable, and a
    report that does not exist is a plain 404 — never a read of another
    file: ``outside.md`` is planted where a traversal would land and must
    stay unseen. Applies to viewing and to downloading alike."""
    add_report(storage, "general", "a.md")
    (storage.parent / "outside.md").write_bytes(b"SECRET CONTENT")

    resp = admin.get(path, params=params)
    assert resp.status_code == 404
    assert "SECRET CONTENT" not in resp.text
