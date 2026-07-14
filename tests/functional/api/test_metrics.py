"""Tests for the ``/dash`` dashboard endpoints in ``sarc.api.metrics``.

The endpoints are checked along three axes:

- **Access control.** Every ``/dash`` route shares one gate
  (``APIRouter(..., dependencies=[Depends(_dash_login_redirect),
  Depends(requestor)])``): a guest (no token) is 307-redirected to the OAuth
  ``/login`` route (``_dash_login_redirect``, which runs before ``requestor``),
  an authenticated email absent from the DB gets 403, and both a regular user
  and an admin get 200.
- **Per-user scoping.** A non-admin only sees their own jobs (``_scope`` filters
  every query on ``sarc_user_id``); an admin sees everything. Proven on every
  aggregating endpoint: the admin total partitions exactly into the two
  user-scoped totals, the averaging endpoint (metric_trend) tells the scopes
  apart by value, and a user who owns no job sees nothing at all.
- **Functional values.** The RGU/metric outputs are exact on enriched data, run
  as admin (full visibility). Plus branch tests (period bucketing, validation,
  filters, sort).

The ``app`` fixture (conftest) mounts the router under the OAuth mock, so
``app.client(email)`` yields a client authenticated as that email (``None`` =
guest). The guest -> login redirect is checked by
``test_guest_redirected_to_login``.
"""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlmodel import col, func, select

from sarc.config import config
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.support import GpuRguDB

# Covers every factory-seeded job (submitted from 2023-02-14, +6h each).
WINDOW = {"start": "2023-02-01", "end": "2023-03-01"}

# Roles, mapped to the factory's seeded identities (tests/db/factory.py):
#   admin@admin.admin   -> admin capability (sarc-test.yaml user_overrides), no
#                          UserDB row needed; sees everything.
#   petitbonhomme       -> regular user owning the large majority of jobs.
#   beaubonhomme        -> regular user owning exactly one job.
#   smithj@mila.quebec  -> valid user (mila_ldap) but owns no job (empty scope).
#   unknown-user        -> authenticates, but no UserDB row -> 403.
_ADMIN = "admin@admin.admin"
_USER = "petitbonhomme@mila.quebec"
_OTHER_USER = "beaubonhomme@mila.quebec"
_USER_NO_JOBS = "smithj@mila.quebec"
_NOT_IN_DB = "unknown-user@mila.quebec"

# GPU-job enrichment, shared by the scoping fixture and the value-test fixture.
# Constants make the RGU/metric outputs exact: physical RGU =
# allocated_gres_gpu * drac_rgu; rgu_hours = rgu * elapsed / 3600.
_GPU = "DASH-TEST-GPU"
_DRAC_RGU = 8.0
_GRES = 2
_BASE_ELAPSED = 43200.0  # factory default elapsed_time: 12h, in seconds
_RGU_PER_JOB = _GRES * _DRAC_RGU  # = 16
_RGU_HOURS_PER_JOB = _RGU_PER_JOB * _BASE_ELAPSED / 3600.0  # = 192
_WEIGHT_PER_JOB = _RGU_PER_JOB * _BASE_ELAPSED  # = 691200, the distribution weight

# Per-job statistics as (mean, max), all in [0, 1]; gpu_sm_occupancy is the default.
_STATS = {
    "gpu_sm_occupancy": (0.5, 0.5),
    "gpu_utilization": (0.4, 0.8),
    "gpu_memory": (0.6, 0.9),
    "system_memory": (0.3, 0.5),
}
_SM_OCC = _STATS["gpu_sm_occupancy"][0]


@pytest.fixture
def dash_client(app):
    """Admin client (full visibility): the functional value tests run as admin."""
    return app.client(_ADMIN)


# Every /dash endpoint, with the minimal params it needs. Reused by the access
# matrix and the empty-data tests. The homepage takes no params.
_ENDPOINTS = [
    ("homepage", "/dash/metrics", {}),
    ("job_counts", "/dash/metrics/job_counts", WINDOW),
    ("job_times", "/dash/metrics/job_times_vs_limit", WINDOW),
    ("metric_distribution", "/dash/metrics/metric_distribution", WINDOW),
    ("metric_comparison", "/dash/metrics/metric_comparison", WINDOW),
    ("rgu_usage", "/dash/metrics/rgu_usage", WINDOW),
    ("rgu_by_cluster", "/dash/metrics/rgu_by_cluster", WINDOW),
    ("metric_trend", "/dash/metrics/metric_trend", WINDOW),
    ("rgu_by_user", "/dash/metrics/rgu_by_user", WINDOW),
    ("jobs", "/dash/metrics/jobs", WINDOW),
]


# === Access control =========================================================


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "email,expected",
    [
        pytest.param(_NOT_IN_DB, 403, id="not_in_db"),
        pytest.param(_USER, 200, id="user"),
        pytest.param(_ADMIN, 200, id="admin"),
    ],
)
@pytest.mark.parametrize(
    "path,params",
    [(path, params) for _, path, params in _ENDPOINTS],
    ids=[name for name, _, _ in _ENDPOINTS],
)
def test_access_control(app, path, params, email, expected):
    """Authenticated access to every /dash endpoint: absent-from-DB 403, user
    and admin 200. Guests are redirected instead — see
    test_guest_redirected_to_login."""
    app.client(email).get(path, params=params, expect_status=expected)


@pytest.mark.parametrize(
    "path,params",
    [(path, params) for _, path, params in _ENDPOINTS],
    ids=[name for name, _, _ in _ENDPOINTS],
)
def test_guest_redirected_to_login(app, path, params):
    """An unauthenticated request to any /dash endpoint is 307-redirected to the
    OAuth ``/login`` route instead of getting a 401 (the /v0 behaviour). A raw,
    non-following client observes the 307 before it is chased into the OAuth
    flow. Needs no DB: the redirect happens in the auth gate, before any
    query."""
    client = TestClient(app, follow_redirects=False)
    resp = client.get(path, params=params)
    assert resp.status_code == 307
    assert resp.headers["location"].endswith("/login")


# === Per-user scoping =======================================================


# Distinct gpu_sm_occupancy means for the two scoped users, so the averaging
# endpoint (metric_trend) can tell their scopes apart by value, not just count.
_OCC_PETIT = 0.3
_OCC_BEAU = 0.7


@pytest.fixture
def scoped_db(read_write_db):
    """Turn one petitbonhomme job and one beaubonhomme job into GPU jobs (+ stats).

    With exactly one enriched job per user, every aggregating endpoint must
    partition: admin sees both, each user sees only their own, and the two
    user-scoped totals sum to the admin's. The two jobs get distinct
    gpu_sm_occupancy means (_OCC_PETIT / _OCC_BEAU) so metric_trend (an average,
    not a sum) can also distinguish the scopes by value.
    """
    sess = read_write_db
    # GpuRguDB first: harmonized_gpu_type is an FK to it.
    sess.add(GpuRguDB(name=_GPU, rgu=10.0, drac_rgu=_DRAC_RGU))
    sess.flush()

    for cluster_user, sm_occ in (
        ("petitbonhomme", _OCC_PETIT),
        ("beaubonhomme", _OCC_BEAU),
    ):
        job = sess.exec(
            select(SlurmJobDB)
            .where(
                col(SlurmJobDB.cluster_user) == cluster_user,
                col(SlurmJobDB.elapsed_time) == _BASE_ELAPSED,
            )
            .order_by(col(SlurmJobDB.id))
        ).first()
        assert job is not None, f"expected a seeded job for {cluster_user}"
        job.harmonized_gpu_type = _GPU
        job.allocated_gpu_type = _GPU
        job.allocated_gres_gpu = _GRES
        sess.add(job)
        stats = {**_STATS, "gpu_sm_occupancy": (sm_occ, sm_occ)}
        for name, (mean, mx) in stats.items():
            sess.add(
                JobStatisticDB(
                    job_id=job.id,
                    name=name,
                    mean=mean,
                    std=0.0,
                    q05=mean,
                    q25=mean,
                    median=mean,
                    q75=mean,
                    max=mx,
                    unused=0.0,
                )
            )
    sess.commit()


# Every aggregating endpoint, with a function reducing its payload to one
# additive scalar. metric_trend (an average) is checked separately; the homepage
# carries no job data (only the cosmetic is_admin flag).
_SCOPE_TOTALS = [
    ("job_counts", "/dash/metrics/job_counts", lambda d: sum(r["count"] for r in d)),
    ("job_times", "/dash/metrics/job_times_vs_limit", lambda d: d["total_jobs"]),
    (
        "metric_distribution",
        "/dash/metrics/metric_distribution",
        lambda d: sum(d["primary"]["weights"]),
    ),
    (
        "metric_comparison",
        "/dash/metrics/metric_comparison",
        lambda d: sum(sum(row) for row in d["z"]),
    ),
    (
        "rgu_usage",
        "/dash/metrics/rgu_usage",
        lambda d: sum(r["rgu_requested"] for r in d),
    ),
    (
        "rgu_by_cluster",
        "/dash/metrics/rgu_by_cluster",
        lambda d: sum(sum(s["rgu"]) for s in d["series"]),
    ),
    (
        "rgu_by_user",
        "/dash/metrics/rgu_by_user",
        lambda d: sum(u["rgu_requested"] for u in d),
    ),
    ("jobs", "/dash/metrics/jobs", lambda d: d["total"]),
]


@pytest.mark.parametrize(
    "path,total",
    [(path, total) for _, path, total in _SCOPE_TOTALS],
    ids=[name for name, _, _ in _SCOPE_TOTALS],
)
def test_scope_partitions_per_endpoint(app, scoped_db, path, total):
    """Every aggregating endpoint scopes to the requestor: admin sees all, each
    user only their own, and the two user views partition the admin's exactly.

    The strict ``user + other == admin`` is what proves the scope keys on the
    right ``sarc_user_id`` — not merely that a non-admin gets *some* subset.
    """
    admin = total(app.client(_ADMIN).get(path, params=WINDOW).json())
    user = total(app.client(_USER).get(path, params=WINDOW).json())
    other = total(app.client(_OTHER_USER).get(path, params=WINDOW).json())

    assert admin > 0
    assert 0 < user < admin
    assert 0 < other < admin
    assert user + other == pytest.approx(admin)


def _trend_means(client) -> list[float]:
    """Non-null per-bucket gpu_sm_occupancy means from /metrics/metric_trend."""
    data = client.get("/dash/metrics/metric_trend", params=WINDOW).json()
    series = {s["metric"]: s for s in data["series"]}["gpu_sm_occupancy"]
    return [m for m in series["mean"] if m is not None]


def test_metric_trend_scoped(app, scoped_db):
    """metric_trend averages, so scoping shows up as the value, not the count:
    each user sees only their own occupancy, while the admin's view mixes both."""
    petit = _trend_means(app.client(_USER))
    beau = _trend_means(app.client(_OTHER_USER))
    admin = _trend_means(app.client(_ADMIN))

    assert petit and all(m == pytest.approx(_OCC_PETIT) for m in petit)
    assert beau and all(m == pytest.approx(_OCC_BEAU) for m in beau)
    # The admin sees beau's job too, so at least one bucket isn't petit-only.
    assert any(m != pytest.approx(_OCC_PETIT) for m in admin)


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "path,total",
    [(path, total) for _, path, total in _SCOPE_TOTALS],
    ids=[name for name, _, _ in _SCOPE_TOTALS],
)
def test_user_without_jobs_sees_nothing(app, path, total):
    """A valid, authenticated user who owns no job is scoped down to empty —
    scoping keys on identity, not "show everything when you own nothing"."""
    assert total(app.client(_USER_NO_JOBS).get(path, params=WINDOW).json()) == 0


# === Empty-data (admin) ===============================================
# Each endpoint on the default window (no jobs): assert the right empty
# container. Run as admin so the emptiness is the window, not the scope.
EMPTY_ENDPOINTS = [
    (
        "job_counts",
        "/dash/metrics/job_counts",
        lambda d: isinstance(d, list) and all(r["count"] == 0 for r in d),
    ),
    (
        "job_times",
        "/dash/metrics/job_times_vs_limit",
        lambda d: d["total_jobs"] == 0 and d["elapsed_vs_limit"] is None,
    ),
    (
        "metric_distribution",
        "/dash/metrics/metric_distribution",
        lambda d: d["primary"]["values"] == [],
    ),
    (
        "metric_comparison",
        "/dash/metrics/metric_comparison",
        lambda d: all(v == 0 for row in d["z"] for v in row),
    ),
    (
        "rgu_usage",
        "/dash/metrics/rgu_usage",
        lambda d: isinstance(d, list) and all(r["rgu_requested"] == 0 for r in d),
    ),
    ("rgu_by_cluster", "/dash/metrics/rgu_by_cluster", lambda d: d["series"] == []),
    (
        "metric_trend",
        "/dash/metrics/metric_trend",
        lambda d: all(v is None for s in d["series"] for v in s["mean"]),
    ),
    ("rgu_by_user", "/dash/metrics/rgu_by_user", lambda d: d == []),
    ("jobs", "/dash/metrics/jobs", lambda d: d["total"] == 0 and d["jobs"] == []),
]


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "path,is_empty",
    [(path, check) for _, path, check in EMPTY_ENDPOINTS],
    ids=[name for name, _, _ in EMPTY_ENDPOINTS],
)
def test_endpoint_empty(dash_client, path, is_empty):
    """Default window (no dates) holds no jobs: each endpoint returns empty."""
    data = dash_client.get(path).raise_for_status().json()
    assert is_empty(data), f"unexpected payload from {path}: {data!r}"


# === Value tests (enriched data, admin) =====================================
# Uses the GPU-job enrichment constants defined near the top of this module.


@pytest.fixture
def dash_db(read_write_db):
    """Writable DB with a few jobs turned into GPU jobs (+ statistics).

    The enriched jobs all belong to petitbonhomme (the factory's default
    cluster_user, so the first jobs are theirs). Returns the facts the value
    tests assert against.
    """
    sess = read_write_db
    # GpuRguDB first: harmonized_gpu_type is an FK to it.
    sess.add(GpuRguDB(name=_GPU, rgu=10.0, drac_rgu=_DRAC_RGU))
    sess.flush()

    # The factory seeds one job with a harmonized, RGU-computable GPU type;
    # detach it so the value tests below cover exactly the enriched jobs.
    for job in sess.exec(
        select(SlurmJobDB).where(col(SlurmJobDB.harmonized_gpu_type).is_not(None))
    ).all():
        job.harmonized_gpu_type = None
        sess.add(job)

    jobs = sess.exec(
        select(SlurmJobDB)
        .where(col(SlurmJobDB.elapsed_time) == _BASE_ELAPSED)
        .order_by(col(SlurmJobDB.id))
    ).all()[:4]
    assert jobs, "expected seeded jobs to enrich"

    for job in jobs:
        job.harmonized_gpu_type = _GPU
        job.allocated_gpu_type = _GPU
        job.allocated_gres_gpu = _GRES
        sess.add(job)
        for name, (mean, mx) in _STATS.items():
            sess.add(
                JobStatisticDB(
                    job_id=job.id,
                    name=name,
                    mean=mean,
                    std=0.0,
                    q05=mean,
                    q25=mean,
                    median=mean,
                    q75=mean,
                    max=mx,
                    unused=0.0,
                )
            )
    sess.commit()

    n = len(jobs)
    return SimpleNamespace(
        n=n,
        gpu=_GPU,
        total_requested=_RGU_HOURS_PER_JOB * n,
        total_used=_RGU_HOURS_PER_JOB * _SM_OCC * n,
        total_weight=_WEIGHT_PER_JOB * n,
    )


@pytest.mark.usefixtures("read_only_db")
def test_job_counts_with_data(dash_client):
    """Bucketed counts sum to the jobs in the window (admin sees all of them)."""
    data = dash_client.get("/dash/metrics/job_counts", params=WINDOW).json()
    with config.db.session() as sess:
        n_jobs = sess.exec(select(func.count()).select_from(SlurmJobDB)).one()
    assert sum(row["count"] for row in data) == n_jobs


@pytest.mark.usefixtures("read_only_db")
def test_job_times_with_data(dash_client):
    """Both heatmaps are populated from the seeded jobs."""
    data = dash_client.get("/dash/metrics/job_times_vs_limit", params=WINDOW).json()
    assert data["total_jobs"] > 0
    for grid in (data["elapsed_vs_limit"], data["wait_vs_limit"]):
        assert grid.keys() >= {"x", "y", "z", "total"}


def test_jobs_table_with_data(dash_client, dash_db):
    """Only the enriched GPU jobs pass the RGU filter; their columns are exact."""
    data = dash_client.get("/dash/metrics/jobs", params=WINDOW).json()
    assert data["total"] == dash_db.n
    assert len(data["jobs"]) == dash_db.n
    for job in data["jobs"]:
        assert job["gpu_type"] == _GPU
        assert job["rgu"] == pytest.approx(_RGU_PER_JOB)
        assert job["rgu_hours"] == pytest.approx(_RGU_HOURS_PER_JOB)
        assert job["gpu_sm_occupancy_mean"] == pytest.approx(_SM_OCC)
        assert job["gpu_utilization_mean"] == pytest.approx(0.4)
        assert job["gpu_memory_max"] == pytest.approx(0.9)


def test_rgu_usage_with_data(dash_client, dash_db):
    data = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    assert sum(r["rgu_requested"] for r in data) == pytest.approx(
        dash_db.total_requested
    )
    assert sum(r["rgu_used"] for r in data) == pytest.approx(dash_db.total_used)


def test_rgu_by_cluster_with_data(dash_client, dash_db):
    data = dash_client.get("/dash/metrics/rgu_by_cluster", params=WINDOW).json()
    assert data["series"], "expected at least one cluster series"
    total = sum(sum(s["rgu"]) for s in data["series"])
    assert total == pytest.approx(dash_db.total_requested)


def test_rgu_by_user_with_data(dash_client, dash_db):
    data = dash_client.get("/dash/metrics/rgu_by_user", params=WINDOW).json()
    assert data, "expected at least one user row"
    assert sum(u["rgu_requested"] for u in data) == pytest.approx(
        dash_db.total_requested
    )
    assert sum(u["rgu_used"] for u in data) == pytest.approx(dash_db.total_used)


def test_metric_trend_with_data(dash_client, dash_db):
    """Every job has gpu_sm_occupancy 0.5, so each bucket averages 0.5."""
    data = dash_client.get("/dash/metrics/metric_trend", params=WINDOW).json()
    series = {s["metric"]: s for s in data["series"]}
    means = [m for m in series["gpu_sm_occupancy"]["mean"] if m is not None]
    assert means, "expected at least one non-empty bucket"
    assert all(m == pytest.approx(_SM_OCC) for m in means)


def test_metric_distribution_with_data(dash_client, dash_db):
    """All jobs share value 0.5 -> single density bin (centre 0.51)."""
    data = dash_client.get("/dash/metrics/metric_distribution", params=WINDOW).json()
    primary = data["primary"]
    assert primary["values"] == pytest.approx([0.51])
    assert sum(primary["weights"]) == pytest.approx(dash_db.total_weight)


def test_metric_comparison_with_data(dash_client, dash_db):
    """metric vs metric2 paired heatmap (its own endpoint now).

    All jobs at gpu_utilization 0.4 / gpu_memory 0.6 -> one cell (bx=40, by=60)
    in the 100x100 grid.
    """
    data = dash_client.get(
        "/dash/metrics/metric_comparison",
        params={**WINDOW, "metric": "gpu_utilization", "metric2": "gpu_memory"},
    ).json()
    z = data["z"]
    assert z[60][40] == dash_db.n
    assert sum(sum(row) for row in z) == dash_db.n


# === Period bucketing =======================================================


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize("period", ["1d", "h", "d", "m"])
def test_period_bucketing(dash_client, period):
    """Fixed (1d) and calendar (h/d/m) periods; default 'w' covers the week arm."""
    params = {"start": "2023-02-14", "end": "2023-02-16", "period": period}
    r = dash_client.get("/dash/metrics/job_counts", params=params)
    assert r.status_code == 200


# === Input validation =======================================================


@pytest.mark.usefixtures("read_only_db")
def test_invalid_period_returns_400(dash_client):
    r = dash_client.get(
        "/dash/metrics/job_counts", params={**WINDOW, "period": "bogus"}
    )
    assert r.status_code == 400


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "path,bad_param",
    [
        ("/dash/metrics/metric_distribution", "metric"),
        ("/dash/metrics/metric_comparison", "metric"),
        ("/dash/metrics/metric_comparison", "metric2"),
        ("/dash/metrics/metric_trend", "metric"),
    ],
)
def test_unknown_metric_returns_400(dash_client, path, bad_param):
    r = dash_client.get(path, params={**WINDOW, bad_param: "not_a_metric"})
    assert r.status_code == 400


@pytest.mark.usefixtures("read_only_db")
def test_unknown_cluster_returns_404(dash_client):
    r = dash_client.get(
        "/dash/metrics/job_counts", params={**WINDOW, "clusters": ["no_such_cluster"]}
    )
    assert r.status_code == 404


# === Filters ================================================================

# Filters matching seeded jobs (raisin / petitbonhomme / a COMPLETED job).
_FILTERS = {
    "clusters": ["raisin"],
    "cluster_user": "petitbonhomme",
    "job_states": ["COMPLETED"],
}


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "path", ["/dash/metrics/job_counts", "/dash/metrics/rgu_usage"]
)
def test_filters_accepted(dash_client, path):
    """Cluster + user + state filters are accepted on both the direct-SlurmJobDB
    (job_counts) and the RGU base (_apply_rgu_base) query paths."""
    r = dash_client.get(path, params={**WINDOW, **_FILTERS})
    assert r.status_code == 200


@pytest.mark.usefixtures("read_only_db")
def test_focus_narrows_window(dash_client):
    """focus_start/end clip the window."""
    r = dash_client.get(
        "/dash/metrics/job_times_vs_limit",
        params={
            **WINDOW,
            **_FILTERS,
            "focus_start": "2023-02-14T00:00:00Z",
            "focus_end": "2023-02-20T00:00:00Z",
        },
    )
    assert r.status_code == 200


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "sort_by",
    [
        "cluster",  # joins clusters into the page subquery
        "waste",  # joins a stat alias (sort_needs_stat)
        "gpu_utilization_mean",
        "gpu_sm_occupancy_mean",
        "gpu_memory_max",
        "job_id",  # ranks on slurm_jobs(+gpurgudb) alone — no extra join
        "submit_time",
        "user",
        "job_state",
        "elapsed",
        "requested_gpu",
        "allocated_gpu",
        "billing",
        "gpu_type",
        "gpu_type_rgu",
        "rgu",
        "rgu_hours",
    ],
)
def test_jobs_sort_columns(dash_client, sort_by):
    """Every sortable column yields a valid page query, whichever join branch the
    sort needs: clusters ("cluster"), a stat alias (sort_needs_stat), or the
    source alone. A missing join would surface as a SQL error (500), not a 200."""
    r = dash_client.get("/dash/metrics/jobs", params={**WINDOW, "sort_by": sort_by})
    assert r.status_code == 200
