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
- **Pro-rating.** A run is charged to the periods it covers, in proportion:
  clipping at the window and bucket edges, runs that never happened, and the
  invariants that follow (every view agrees, splitting a window conserves).

The ``app`` fixture (conftest) mounts the router under the OAuth mock, so
``app.client(email)`` yields a client authenticated as that email (``None`` =
guest). The guest -> login redirect is checked by
``test_guest_redirected_to_login``.
"""

import math
import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlmodel import col, select

from sarc.config import config
from sarc.db.job import JobStatisticDB, SlurmJobDB, SlurmState
from sarc.db.job_series import JobSeriesDB
from sarc.db.support import GpuRguDB

# Covers every factory-seeded job (submitted from 2023-02-14, +6h each).
WINDOW = {"start": "2023-02-01", "end": "2023-03-01"}

# QUIET is a window no seeded job touches (the factory runs 2023-02-14 05:01 to
# 2023-02-20 05:01), so one job placed inside it accounts for everything the
# endpoints report.
QUIET = {"start": "2023-02-01", "end": "2023-02-11"}
_QUIET_DAYS = 10

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
        lambda d: sum(r["rgu_allocated"] for r in d),
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
        lambda d: isinstance(d, list) and all(r["rgu_allocated"] == 0 for r in d),
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
    """Counts read as occupancy: how many jobs were running in each bucket.

    A job is therefore counted in every bucket it spans, so the buckets sum to
    *at least* the number of jobs -- summing them is not a way to count jobs.
    Over a single bucket covering the whole window, each running job is counted
    exactly once, which is what pins the number down.
    """
    begin = datetime(2023, 2, 1, tzinfo=timezone.utc)
    finish = datetime(2023, 3, 1, tzinfo=timezone.utc)
    ran_in_window = [
        job
        for job in _dashboard_jobs()
        if job.start_time is not None
        and job.start_time < finish
        and job.start_time + timedelta(seconds=job.elapsed_time) > begin
    ]
    assert ran_in_window, "expected seeded jobs running in the window"

    whole = dash_client.get(
        "/dash/metrics/job_counts", params={**WINDOW, "period": "m"}
    ).json()
    assert len(whole) == 1, "February is one calendar-month bucket"
    assert whole[0]["count"] == len(ran_in_window)

    daily = dash_client.get(
        "/dash/metrics/job_counts", params={**WINDOW, "period": "d"}
    ).json()
    assert sum(row["count"] for row in daily) >= len(ran_in_window)

    # The Submitted view answers the other question: each job lands in exactly one
    # bucket, so there its buckets do add up -- and to its own population.
    submitted = [job for job in _dashboard_jobs() if begin <= job.submit_time < finish]
    submitted_daily = dash_client.get(
        "/dash/metrics/job_counts",
        params={**WINDOW, "period": "d", "submitted": "true"},
    ).json()
    assert sum(row["count"] for row in submitted_daily) == len(submitted)


# Every bucketed endpoint, with the empty payload it owes an empty window.
_BUCKETED = [
    ("job_counts", []),
    ("rgu_usage", []),
    ("rgu_by_cluster", {"periods": [], "series": []}),
    (
        "metric_trend",
        {
            "periods": [],
            "series": [{"metric": "gpu_sm_occupancy", "mean": [], "max": []}],
        },
    ),
]


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize("period", ["d", "w", "m", "h", "14d"])
@pytest.mark.parametrize(("endpoint", "empty"), _BUCKETED)
def test_bucketed_endpoints_on_an_empty_window(dash_client, endpoint, empty, period):
    """start == end asks about a window of no length: the answer is nothing.

    End being exclusive, that is what the UI sends whenever someone fills both
    date inputs with the same day -- neither is validated. There is no bucket to
    charge anything to, so each endpoint owes its own empty shape, not a 200 with
    a bucket axis it cannot build.
    """
    resp = dash_client.get(
        f"/dash/metrics/{endpoint}",
        params={"start": "2023-02-15", "end": "2023-02-15", "period": period},
    )
    assert resp.status_code == 200
    assert resp.json() == empty


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
        # Default metric is gpu_sm_occupancy, so metric_mean mirrors it.
        assert job["metric_mean"] == pytest.approx(_SM_OCC)
        assert job["gpu_utilization_mean"] == pytest.approx(0.4)
        assert job["gpu_memory_max"] == pytest.approx(0.9)


def test_job_times_vs_limit_follows_the_submission(dash_client, dash_db):
    """The one endpoint selected on submit_time: a job submitted before the
    window and running well into it is charged to the window everywhere else,
    and counted nowhere here."""
    start = datetime(2023, 1, 30, tzinfo=timezone.utc)  # 2 days before the window
    with config.db.session() as sess:
        _run_single_job(sess, start, hours=5 * 24)

    # It did run inside the window: the RGU views charge it 3 of its 5 days.
    usage = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    assert sum(r["rgu_allocated"] for r in usage) == pytest.approx(
        _RGU_PER_JOB * 3 * 24
    )

    assert _jobs_submitted() == 0, "the one job left was submitted before the window"
    data = dash_client.get("/dash/metrics/job_times_vs_limit", params=WINDOW).json()
    assert data["total_jobs"] == 0

    # And it is not that the plot counts nothing: over a window holding the
    # submission, the same job shows up.
    january = {"start": "2023-01-01", "end": "2023-02-01"}
    data = dash_client.get("/dash/metrics/job_times_vs_limit", params=january).json()
    assert data["total_jobs"] == 1


def _dashboard_jobs() -> list[JobSeriesDB]:
    """The seeded jobs every plot draws from: GPU jobs with a known RGU.

    Read off the view the endpoints read, so the population is stated once here
    and the tests never restate the SQL that selects it.
    """
    with config.db.session() as sess:
        return list(
            sess.exec(
                select(JobSeriesDB).where(
                    col(JobSeriesDB.allocated_gres_gpu) > 0,
                    col(JobSeriesDB.harmonized_gpu_type).is_not(None),
                )
            ).all()
        )


def _jobs_submitted() -> int:
    """Seeded jobs submitted inside WINDOW that started and asked for a
    time_limit -- the population of /job_times_vs_limit, counted in Python for
    the same reason as _jobs_that_ran()."""
    begin = datetime(2023, 2, 1, tzinfo=timezone.utc)
    finish = datetime(2023, 3, 1, tzinfo=timezone.utc)
    return sum(
        1
        for job in _dashboard_jobs()
        if job.start_time is not None
        and job.time_limit is not None
        and begin <= job.submit_time < finish
    )


def test_rgu_usage_with_data(dash_client, dash_db):
    data = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    assert sum(r["rgu_allocated"] for r in data) == pytest.approx(
        dash_db.total_requested
    )
    assert sum(r["rgu_used"] for r in data) == pytest.approx(dash_db.total_used)
    # Every enriched job runs at 50 % >= the 15 % default min_usage: no shortfall.
    assert sum(r["rgu_wasted"] for r in data) == 0.0


def test_rgu_usage_wasted_is_per_job(dash_client, dash_db):
    """``rgu_wasted`` sums each job's own shortfall below ``min_usage``: a job
    above the threshold contributes zero and never offsets a job below it.

    With one job dropped to 2 % and the rest at 50 %, the bucket-aggregated
    shortfall would be 0 (mean usage is well above 15 %); the per-job one is
    exactly the low job's deficit.
    """
    low_mean = 0.02
    with config.db.session() as sess:
        job = sess.exec(
            select(SlurmJobDB)
            .where(col(SlurmJobDB.harmonized_gpu_type) == _GPU)
            .order_by(col(SlurmJobDB.id))
        ).first()
        stat = sess.exec(
            select(JobStatisticDB).where(
                col(JobStatisticDB.job_id) == job.id,
                col(JobStatisticDB.name) == "gpu_sm_occupancy",
            )
        ).one()
        stat.mean = low_mean
        sess.add(stat)
        sess.commit()

    data = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    assert sum(r["rgu_wasted"] for r in data) == pytest.approx(
        _RGU_HOURS_PER_JOB * (0.15 - low_mean)
    )
    # The shortfall stays within the unused share: no negative bar segment.
    for r in data:
        assert (
            r["rgu_wasted"]
            <= r["rgu_allocated"] - r["rgu_used"] - r["rgu_unmeasured"] + 1e-9
        )

    # min_usage is a request parameter: at 60 % every job falls short.
    data = dash_client.get(
        "/dash/metrics/rgu_usage", params={**WINDOW, "min_usage": 0.6}
    ).json()
    expected = _RGU_HOURS_PER_JOB * (
        (0.6 - low_mean) + (dash_db.n - 1) * (0.6 - _SM_OCC)
    )
    assert sum(r["rgu_wasted"] for r in data) == pytest.approx(expected)


def test_rgu_usage_metric_means(dash_client, dash_db):
    """``metric_means`` gives the plain per-job mean of each trend metric. A
    bucket holding no job reports a null mean -- an average of nothing, not a
    0 % that would read as measured."""
    data = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    for name, expected in (("gpu_sm_occupancy", _SM_OCC), ("gpu_utilization", 0.4)):
        means = [r["metric_means"][name]["mean"] for r in data]
        assert any(m is not None for m in means), "expected a charged bucket"
        for mean in means:
            assert mean is None or mean == pytest.approx(expected)


def test_rgu_usage_whole_counts_each_job_once(dash_client, dash_db):
    """``whole=true`` averages over the jobs, which is why it is a query of its
    own rather than the per-period rows added up.

    A job is counted in every bucket it runs through, so no recombination of the
    per-bucket means can recover a per-job average: it would weigh each job by
    how long it ran, and the figure would move with the Period selector. Over a
    single bucket each job is averaged once, and the question goes away.

    Three jobs at 20 % inside one week, and one at 80 % running across two. The
    per-job mean is 35 %; recombining the buckets by their job counts -- what the
    frontend used to do -- reads 44 %, and averaging the bucket means 57.5 %.

    The sums are checked too: those *are* additive, so both views must agree on
    them, which is what lets the reader switch between them without the totals
    moving.
    """
    low, high = 0.2, 0.8
    short = datetime(2023, 2, 14, 12, tzinfo=timezone.utc)  # inside 02-13..02-20
    crossing = datetime(2023, 2, 18, 12, tzinfo=timezone.utc)  # +4d -> 02-22 12:00
    with config.db.session() as sess:
        jobs = sess.exec(
            select(SlurmJobDB)
            .where(col(SlurmJobDB.harmonized_gpu_type) == _GPU)
            .order_by(col(SlurmJobDB.id))
        ).all()
        assert len(jobs) == 4, "the fixture enriches exactly 4 jobs"
        for i, job in enumerate(jobs):
            long_one = i == 3
            # Buckets follow when a job ran, so these set start_time; submit_time
            # goes with it to keep the row coherent (no start before submission).
            job.submit_time = job.start_time = crossing if long_one else short
            job.elapsed_time = 4 * 24 * 3600.0 if long_one else 12 * 3600.0
            job.end_time = job.start_time + timedelta(seconds=job.elapsed_time)
            sess.add(job)
            stat = sess.exec(
                select(JobStatisticDB).where(
                    col(JobStatisticDB.job_id) == job.id,
                    col(JobStatisticDB.name) == "gpu_sm_occupancy",
                )
            ).one()
            stat.mean = high if long_one else low
            sess.add(stat)
        sess.commit()

    def fetch(**extra):
        return dash_client.get(
            "/dash/metrics/rgu_usage", params={**WINDOW, "period": "w", **extra}
        ).json()

    whole = fetch(whole="true")
    assert len(whole) == 1, "the whole range is one bucket"
    assert whole[0]["metric_means"]["gpu_sm_occupancy"]["mean"] == pytest.approx(
        (3 * low + high) / 4
    )

    # The long job does show up in two weekly buckets -- that is what makes the
    # per-bucket means unrecombinable, and what whole=true steps around.
    weekly = fetch()
    charged = [
        r["metric_means"]["gpu_sm_occupancy"]["mean"]
        for r in weekly
        if r["metric_means"]["gpu_sm_occupancy"]["mean"] is not None
    ]
    assert charged == pytest.approx([(3 * low + high) / 4, high])

    # Sums are additive, so the two views agree on them whatever the bucketing.
    for key in ("rgu_allocated", "rgu_used", "rgu_unmeasured", "rgu_wasted"):
        assert whole[0][key] == pytest.approx(sum(r[key] for r in weekly)), key

    # period is ignored under whole=true: a different one gives the same answer.
    assert fetch(whole="true", period="d") == whole


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


# === Pro-rating =============================================================


def _run_single_job(
    sess, start: datetime, hours: float, submit: datetime | None = None
):
    """Leave exactly one enriched GPU job, running for ``hours`` from ``start``.

    Pro-rating is about one job meeting several buckets, so these tests need a
    job whose span they choose; the other three are detached rather than moved,
    so whatever lands in a bucket comes from this one alone. Detaching drops the
    RGU weight, which every plot now filters on, so the isolation is total.

    ``submit`` defaults to ``start``; pass it to pull the submission away from the
    run, which is what separates the two job-count views.
    """
    jobs = sess.exec(
        select(SlurmJobDB)
        .where(col(SlurmJobDB.harmonized_gpu_type) == _GPU)
        .order_by(col(SlurmJobDB.id))
    ).all()
    for job in jobs[1:]:
        job.harmonized_gpu_type = None
        sess.add(job)
    job = jobs[0]
    job.start_time = start
    job.submit_time = submit or start
    job.elapsed_time = hours * 3600.0
    job.end_time = start + timedelta(hours=hours)
    sess.add(job)
    sess.commit()
    return _RGU_PER_JOB * hours  # RGU.h the job is worth in total


def _jobs_that_ran() -> int:
    """Seeded jobs whose run overlaps WINDOW, counted in Python so the test does
    not restate the SQL it is checking."""
    begin = datetime(2023, 2, 1, tzinfo=timezone.utc)
    finish = datetime(2023, 3, 1, tzinfo=timezone.utc)
    return sum(
        1
        for job in _dashboard_jobs()
        if job.start_time is not None
        and job.elapsed_time > 0
        and job.start_time < finish
        and job.start_time + timedelta(seconds=job.elapsed_time) > begin
    )


def _utc(month: int, day: int) -> datetime:
    return datetime(2023, month, day, tzinfo=timezone.utc)


# (label, run start, run length, hours the window is owed). WINDOW is the 28 days
# of [02-01, 03-01).
_WINDOW_SHARES = [
    ("inside", _utc(2, 10), 24, 24),
    ("clipped at the start", _utc(1, 30), 5 * 24, 3 * 24),
    ("clipped at the end", _utc(2, 27), 5 * 24, 2 * 24),
    ("spanning the whole window", _utc(1, 25), 40 * 24, 28 * 24),
    ("opening on the window's first instant", _utc(2, 1), 24, 24),
    ("closing on the window's first instant", _utc(1, 31), 24, 0),
    ("opening on the window's last instant", _utc(3, 1), 24, 0),
    ("entirely before", _utc(1, 20), 24, 0),
    ("entirely after", _utc(3, 5), 24, 0),
    ("of no length", _utc(2, 10), 0, 0),
]


@pytest.mark.parametrize(
    ("start", "hours", "owed"),
    [case[1:] for case in _WINDOW_SHARES],
    ids=[case[0] for case in _WINDOW_SHARES],
)
def test_a_run_owes_the_window_its_hours_inside_it(
    dash_client, dash_db, start, hours, owed
):
    """A run is charged for the hours it spent inside the window, and only those.

    The boundary rows are the point: bounds are half-open on both sides, so a run
    closing exactly on the window's first instant owes nothing, and one opening on
    its last instant owes nothing either. Under submit_time none of the clipped
    rows counted at all, which made the first bar of any range read low.

    Whether the table lists the job is asserted too, and it is the half that
    bites: a boundary touched inclusively is worth zero hours either way, so the
    sum alone would accept `[]` bounds while the table would list a job that never
    reached the window.
    """
    with config.db.session() as sess:
        _run_single_job(sess, start, hours)

    data = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    assert sum(r["rgu_allocated"] for r in data) == pytest.approx(_RGU_PER_JOB * owed)

    table = dash_client.get("/dash/metrics/jobs", params=WINDOW).json()
    assert table["total"] == (1 if owed else 0)


def test_the_two_job_count_views_select_on_different_columns(dash_client, dash_db):
    """One job queued on 01-15 and running 01-30 to 02-04, so the views disagree.

    A January window holds its submission and not its run; WINDOW holds its run
    and not its submission. Each view counts it in exactly one of the two, which
    is the whole reason for offering both.
    """
    with config.db.session() as sess:
        _run_single_job(sess, _utc(1, 30), hours=5 * 24, submit=_utc(1, 15))

    def counted(window, **extra):
        rows = dash_client.get(
            "/dash/metrics/job_counts", params={**window, "period": "m", **extra}
        ).json()
        return sum(row["count"] for row in rows)

    january = {"start": "2023-01-01", "end": "2023-01-20"}
    assert counted(january) == 0
    assert counted(january, submitted="true") == 1
    assert counted(WINDOW) == 1
    assert counted(WINDOW, submitted="true") == 0

    # And it lands in the bucket of its submission, not merely somewhere.
    daily = dash_client.get(
        "/dash/metrics/job_counts",
        params={**january, "period": "d", "submitted": "true"},
    ).json()
    assert [row["period_start"] for row in daily if row["count"]] == ["2023-01-15"]


def test_a_running_job_is_charged_from_its_elapsed_not_its_end(dash_client, dash_db):
    """A job still running carries no end_time, and is charged all the same.

    The measure is [start_time, start_time + elapsed_time); end_time never enters
    it. That is what freezes a past bucket once one scrape has run after it:
    elapsed only ever grows, so the interval only ever extends to the right.
    """
    with config.db.session() as sess:
        _run_single_job(sess, _utc(2, 10), hours=24)
        job = sess.exec(
            select(SlurmJobDB).where(col(SlurmJobDB.harmonized_gpu_type) == _GPU)
        ).first()
        job.job_state = SlurmState.RUNNING
        job.end_time = None
        sess.add(job)
        sess.commit()

    data = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    assert sum(r["rgu_allocated"] for r in data) == pytest.approx(_RGU_PER_JOB * 24)
    table = dash_client.get("/dash/metrics/jobs", params=WINDOW).json()
    assert table["total"] == 1


def test_a_job_that_never_ran_appears_nowhere(dash_client, dash_db):
    """Every plot answers the same question -- which jobs were running then --
    so a job that ran for no time answers it the same way everywhere: not at all.

    It spans the empty interval, and the empty interval meets no bucket. Before
    the `elapsed_time > 0` guard it still matched the bucket its start_time fell
    in: worth 0 RGU.h, but inflating the job counts. Production carries 258 674
    of these.
    """
    with config.db.session() as sess:
        job = sess.exec(
            select(SlurmJobDB)
            .where(col(SlurmJobDB.harmonized_gpu_type) == _GPU)
            .order_by(col(SlurmJobDB.id))
        ).first()
        job.elapsed_time = 0.0
        job.end_time = job.start_time
        sess.add(job)
        sess.commit()

    # The RGU views drop it (it was worth 0 anyway), and so does the job table.
    usage = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    assert sum(r["rgu_allocated"] for r in usage) == pytest.approx(
        _RGU_HOURS_PER_JOB * (dash_db.n - 1)
    )
    table = dash_client.get("/dash/metrics/jobs", params=WINDOW).json()
    assert table["total"] == dash_db.n - 1

    # And the counts, which is where it used to show up despite weighing nothing.
    # _jobs_that_ran already applies the rule, so it drops the job too: the point
    # is that the endpoint agrees with it.
    counts = dash_client.get(
        "/dash/metrics/job_counts", params={**WINDOW, "period": "m"}
    ).json()
    assert sum(r["count"] for r in counts) == _jobs_that_ran()


def test_a_job_that_never_started_appears_nowhere(dash_client, dash_db):
    """A job still queued has no span, so it meets no bucket -- whatever its
    elapsed_time says.

    Separate from the zero-elapsed case above: that one has no end because it
    lasted nothing, this one because it never began, and ``_overlap_hours``
    alone would hand either the full width of every bucket. Only
    ``slurm_job_end`` returning NULL keeps them out.
    """
    with config.db.session() as sess:
        job = sess.exec(
            select(SlurmJobDB)
            .where(col(SlurmJobDB.harmonized_gpu_type) == _GPU)
            .order_by(col(SlurmJobDB.id))
        ).first()
        job.start_time = None
        job.end_time = None
        sess.add(job)
        sess.commit()

    usage = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    assert sum(r["rgu_allocated"] for r in usage) == pytest.approx(
        _RGU_HOURS_PER_JOB * (dash_db.n - 1)
    )
    table = dash_client.get("/dash/metrics/jobs", params=WINDOW).json()
    assert table["total"] == dash_db.n - 1
    counts = dash_client.get(
        "/dash/metrics/job_counts", params={**WINDOW, "period": "m"}
    ).json()
    assert sum(r["count"] for r in counts) == _jobs_that_ran()


def test_rgu_usage_prorates_a_job_over_the_weeks_it_ran(dash_client, dash_db):
    """A job is charged to the weeks it ran in, in proportion to the time it
    spent in each -- not wholly to the week it was submitted in.

    Nine days from Thursday noon: 3.5 days in the first calendar week (up to
    Monday 00:00 UTC), 5.5 in the second. The two shares add back up to the
    job's whole cost, which is what makes the bars still sum to something
    meaningful.
    """
    start = datetime(2023, 2, 16, 12, tzinfo=timezone.utc)  # Thursday
    with config.db.session() as sess:
        total = _run_single_job(sess, start, hours=9 * 24)

    data = dash_client.get(
        "/dash/metrics/rgu_usage", params={**WINDOW, "period": "w"}
    ).json()
    charged = [r["rgu_allocated"] for r in data if r["rgu_allocated"] > 0]
    assert len(charged) == 2, "the job spans two calendar weeks"
    assert charged[0] == pytest.approx(_RGU_PER_JOB * 3.5 * 24)
    assert charged[1] == pytest.approx(_RGU_PER_JOB * 5.5 * 24)
    assert sum(charged) == pytest.approx(total)


def test_rgu_usage_prorates_across_a_month_boundary(dash_client, dash_db):
    """Monthly buckets split a run on the 1st, like any other period.

    Months are the one period whose buckets are not a fixed number of seconds, so
    a position in the axis is counted off the calendar rather than divided out of
    a length. This checks that arithmetic where it can actually be wrong: the
    charged buckets are the 2nd and 3rd of four, not the 1st, and the window opens
    mid-month so bucket 0 is clipped.
    """
    start = datetime(2023, 2, 27, tzinfo=timezone.utc)  # 2 days of February left
    with config.db.session() as sess:
        total = _run_single_job(sess, start, hours=5 * 24)

    data = dash_client.get(
        "/dash/metrics/rgu_usage",
        params={"start": "2023-01-15", "end": "2023-04-10", "period": "m"},
    ).json()
    assert [r["period_start"] for r in data] == [
        "2023-01-15",
        "2023-02-01",
        "2023-03-01",
        "2023-04-01",
    ]
    assert [r["rgu_allocated"] for r in data] == pytest.approx(
        [0.0, _RGU_PER_JOB * 2 * 24, _RGU_PER_JOB * 3 * 24, 0.0]
    )
    assert sum(r["rgu_allocated"] for r in data) == pytest.approx(total)


def test_empty_buckets_are_reported_as_empty(dash_client, dash_db):
    """A bucket no job ran in is present and empty -- not missing, and not folded
    into its neighbour.

    One job, 36h from 02-04 12:00, so exactly two of the ten daily buckets are
    charged (12h on the 4th, 24h on the 5th) and the eight others must come back
    at zero. Each endpoint says "empty" in its own way: 0 for a count or a sum,
    null for metric_trend (an average has no value without a job, and a null is
    a gap in the curve rather than a misleading 0).
    """
    with config.db.session() as sess:
        total = _run_single_job(sess, datetime(2023, 2, 4, 12, tzinfo=timezone.utc), 36)
    params = {**QUIET, "period": "d"}
    charged = {3, 4}  # 0-based: the 4th and the 5th of February

    counts = dash_client.get("/dash/metrics/job_counts", params=params).json()
    assert [r["count"] for r in counts] == [
        1 if i in charged else 0 for i in range(_QUIET_DAYS)
    ]

    expected_rgu = [0.0] * _QUIET_DAYS
    expected_rgu[3] = _RGU_PER_JOB * 12
    expected_rgu[4] = _RGU_PER_JOB * 24
    usage = dash_client.get("/dash/metrics/rgu_usage", params=params).json()
    assert [r["rgu_allocated"] for r in usage] == pytest.approx(expected_rgu)
    assert sum(r["rgu_allocated"] for r in usage) == pytest.approx(total)

    by_cluster = dash_client.get("/dash/metrics/rgu_by_cluster", params=params).json()
    per_bucket = by_cluster["series"][0]["rgu"]
    assert len(per_bucket) == _QUIET_DAYS
    assert [i for i, v in enumerate(per_bucket) if v > 0] == sorted(charged)

    trend = dash_client.get("/dash/metrics/metric_trend", params=params).json()
    means = trend["series"][0]["mean"]
    assert len(means) == _QUIET_DAYS
    assert [i for i, v in enumerate(means) if v is not None] == sorted(charged)


def test_a_run_ending_on_a_bucket_edge_charges_nothing_to_the_next(
    dash_client, dash_db
):
    """A run that stops exactly on a bucket boundary leaves the next bucket empty.

    Bounds are half-open on both sides, so the instant a run ends belongs to the
    next bucket without reaching into it. Both natural mistakes get this wrong --
    an inclusive stop bound, or `[]` bounds -- and the damage is invisible in a
    sum (zero hours) but not in job_counts, which would report the job running on
    a day it had already finished.
    """
    # 02-03 00:00 + 48h = 02-05 00:00, exactly where the 5th's bucket opens.
    with config.db.session() as sess:
        _run_single_job(sess, datetime(2023, 2, 3, tzinfo=timezone.utc), 48)
    params = {**QUIET, "period": "d"}

    counts = dash_client.get("/dash/metrics/job_counts", params=params).json()
    assert [r["count"] for r in counts] == [0, 0, 1, 1, 0, 0, 0, 0, 0, 0]
    assert counts[4]["period_start"] == "2023-02-05"

    usage = dash_client.get("/dash/metrics/rgu_usage", params=params).json()
    assert usage[4]["rgu_allocated"] == 0.0
    assert [r["rgu_allocated"] for r in usage[:4]] == pytest.approx(
        [0.0, 0.0, _RGU_PER_JOB * 24, _RGU_PER_JOB * 24]
    )


def test_rgu_usage_is_additive_over_a_window_split(dash_client, dash_db):
    """Cutting the window in two conserves the total -- the property submit_time
    semantics did not have.

    Under the old rule a job belonged wholly to the half its submission fell in,
    so a cut either double-counted it or lost it. Charging the time itself splits
    the hours instead. The cut falls inside a run here (02-10 12:00 -> 02-12
    00:00, window cut at 02-11), where the property used to break.
    """
    start = datetime(2023, 2, 10, 12, tzinfo=timezone.utc)
    with config.db.session() as sess:
        total = _run_single_job(sess, start, hours=36)

    halves = (
        {"start": "2023-02-01", "end": "2023-02-11"},
        {"start": "2023-02-11", "end": "2023-03-01"},
    )

    def charged(window):
        data = dash_client.get(
            "/dash/metrics/rgu_usage", params={**window, "period": "d"}
        ).json()
        return sum(r["rgu_allocated"] for r in data)

    whole, first, second = charged(WINDOW), charged(halves[0]), charged(halves[1])
    assert whole == pytest.approx(total), "the whole run falls inside WINDOW"
    assert first + second == pytest.approx(whole)
    # 12h before the cut, 24h after it.
    assert first == pytest.approx(_RGU_PER_JOB * 12)
    assert second == pytest.approx(_RGU_PER_JOB * 24)


def test_rgu_by_cluster_prorates_like_rgu_usage(dash_client, dash_db):
    """The two plots are drawn side by side and must agree bucket by bucket."""
    start = datetime(2023, 2, 16, 12, tzinfo=timezone.utc)
    with config.db.session() as sess:
        _run_single_job(sess, start, hours=9 * 24)

    params = {**WINDOW, "period": "w"}
    usage = dash_client.get("/dash/metrics/rgu_usage", params=params).json()
    by_cluster = dash_client.get("/dash/metrics/rgu_by_cluster", params=params).json()

    assert len(by_cluster["series"]) == 1, "the enriched job is on one cluster"
    per_bucket = by_cluster["series"][0]["rgu"]
    assert len(per_bucket) == len(usage)
    for cluster_value, usage_row in zip(per_bucket, usage, strict=True):
        assert cluster_value == pytest.approx(usage_row["rgu_allocated"])


def test_every_view_agrees_on_the_windows_rgu_hours(dash_client, dash_db):
    """The bars, the per-user breakdown and the job table have to add up to the
    same number, or the dashboard contradicts itself.

    A job running from two days before the window closes until three days after
    owes the window exactly those two days, wherever it is displayed.
    """
    start = datetime(2023, 2, 27, tzinfo=timezone.utc)
    with config.db.session() as sess:
        _run_single_job(sess, start, hours=5 * 24)
    expected = _RGU_PER_JOB * 2 * 24  # WINDOW ends 2023-03-01

    usage = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    by_user = dash_client.get("/dash/metrics/rgu_by_user", params=WINDOW).json()
    table = dash_client.get("/dash/metrics/jobs", params=WINDOW).json()
    dist = dash_client.get("/dash/metrics/metric_distribution", params=WINDOW).json()

    assert sum(r["rgu_allocated"] for r in usage) == pytest.approx(expected)
    assert sum(u["rgu_requested"] for u in by_user) == pytest.approx(expected)
    # The table rounds each row to two decimals.
    assert sum(j["rgu_hours"] for j in table["jobs"]) == pytest.approx(
        expected, abs=0.01
    )
    assert table["total"] == 1, "the job crossing the window edge is listed, once"
    # The histogram weighs in RGU-seconds -- the same measure in another unit,
    # and the only place the pro-rating shows up in a distribution.
    assert sum(dist["primary"]["weights"]) / 3600.0 == pytest.approx(expected)


# === NaN / missing-stat handling (migration guardrail) ======================
# Prometheus gaps leave some jobs with a NaN metric mean, and some
# RGU-computable jobs have no stat row at all (LEFT-join NULL). On Postgres
# NaN = NaN is TRUE and NaN poisons SUM/AVG, so the endpoints gate every
# mean-based term on `_is_real` (NOT NULL AND != 'NaN'). This pins that
# behavior by value so the slurm_jobs -> job_series migration must reproduce
# it exactly: such jobs count wherever their RGU is used, but never enter (nor
# poison) a mean-based aggregate.

_NAN = float("nan")


@pytest.fixture
def dash_db_nan(read_write_db):
    """Four RGU-computable GPU jobs, differing only in gpu_sm_occupancy: two
    "good" (mean 0.5), one with a NaN mean, one with the stat missing entirely.

    All four have a valid RGU (GPU type + gres), so all four count in the jobs
    table and in the allocated-RGU totals; only the two good ones may enter a
    mean-based aggregate. Returns the expected totals.
    """
    sess = read_write_db
    sess.add(GpuRguDB(name=_GPU, rgu=10.0, drac_rgu=_DRAC_RGU))
    sess.flush()

    # Detach the factory's own RGU-computable job so only ours are counted.
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
    assert len(jobs) == 4, "need 4 seeded jobs to enrich"

    # gpu_sm_occupancy mean per job: 2 good, 1 NaN, 1 missing (no stat row).
    occupancies = [_SM_OCC, _SM_OCC, _NAN, None]
    for job, sm in zip(jobs, occupancies):
        job.harmonized_gpu_type = _GPU
        job.allocated_gpu_type = _GPU
        job.allocated_gres_gpu = _GRES
        sess.add(job)
        stats = dict(_STATS)
        if sm is None:
            del stats["gpu_sm_occupancy"]  # no row -> LEFT join yields NULL
        elif math.isnan(sm):
            stats["gpu_sm_occupancy"] = (sm, sm)  # NaN mean (Prometheus gap)
        # else: keep the finite default (_SM_OCC)
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
                )
            )
    sess.commit()

    n_total, n_good = 4, 2
    return SimpleNamespace(
        n_total=n_total,
        n_good=n_good,
        total_requested=_RGU_HOURS_PER_JOB * n_total,
        total_used=_RGU_HOURS_PER_JOB * _SM_OCC * n_good,
        total_unmeasured=_RGU_HOURS_PER_JOB * (n_total - n_good),
        total_weight=_WEIGHT_PER_JOB * n_good,
    )


def test_nan_and_missing_means_never_poison_aggregates(dash_client, dash_db_nan):
    """A NaN or missing metric mean must count where RGU is used but stay out of
    every mean-based aggregate, which must remain finite (never NaN)."""
    facts = dash_db_nan

    # jobs table: every RGU-computable job is listed; NaN/NULL means don't drop it.
    jobs = dash_client.get("/dash/metrics/jobs", params=WINDOW).json()
    assert jobs["total"] == facts.n_total
    assert len(jobs["jobs"]) == facts.n_total

    # rgu_usage: allocated counts all jobs; used sums only the real means and
    # stays finite; the NaN/NULL jobs land in rgu_unmeasured, not rgu_used.
    usage = dash_client.get("/dash/metrics/rgu_usage", params=WINDOW).json()
    used = sum(r["rgu_used"] for r in usage)
    assert math.isfinite(used)
    assert sum(r["rgu_allocated"] for r in usage) == pytest.approx(
        facts.total_requested
    )
    assert used == pytest.approx(facts.total_used)
    assert sum(r["rgu_unmeasured"] for r in usage) == pytest.approx(
        facts.total_unmeasured
    )
    # NaN/missing means never count as shortfall (nor do the 50 % good jobs).
    assert sum(r["rgu_wasted"] for r in usage) == 0.0

    # rgu_by_user carries its own copy of the same NaN gate.
    by_user = dash_client.get("/dash/metrics/rgu_by_user", params=WINDOW).json()
    used_by_user = sum(u["rgu_used"] for u in by_user)
    assert math.isfinite(used_by_user)
    assert used_by_user == pytest.approx(facts.total_used)

    # metric_trend: the average skips NaN/NULL, so every non-empty bucket is 0.5.
    trend = dash_client.get("/dash/metrics/metric_trend", params=WINDOW).json()
    series = {s["metric"]: s for s in trend["series"]}["gpu_sm_occupancy"]
    means = [m for m in series["mean"] if m is not None]
    assert means, "expected at least one non-empty bucket"
    assert all(math.isfinite(m) and m == pytest.approx(_SM_OCC) for m in means)

    # metric_distribution: only the two good jobs are binned; weights stay finite.
    dist = dash_client.get("/dash/metrics/metric_distribution", params=WINDOW).json()
    weights = dist["primary"]["weights"]
    assert all(math.isfinite(w) for w in weights)
    assert sum(weights) == pytest.approx(facts.total_weight)


# === Period bucketing =======================================================


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize("period", ["1d", "h", "d", "m"])
def test_period_bucketing(dash_client, period):
    """Fixed (1d) and calendar (h/d/m) periods; default 'w' covers the week arm."""
    params = {"start": "2023-02-14", "end": "2023-02-16", "period": period}
    r = dash_client.get("/dash/metrics/job_counts", params=params)
    assert r.status_code == 200


# The bucket axis is the x-axis every time plot shares, so the four bucketed
# endpoints have to agree on it. These tests pin the axis down independently of
# the data, then which buckets the data lands in.


def _period_axis(payload) -> list[tuple[str, str]]:
    """The (period_start, period_end) axis, whichever shape the endpoint returns:
    a bare list of bucket rows, or {periods, series}."""
    rows = payload if isinstance(payload, list) else payload["periods"]
    return [(r["period_start"], r["period_end"]) for r in rows]


# One case per arm of the bucket arithmetic: a fixed step, a calendar unit of
# fixed length (hour/day/week), and months, which have none. Each also has a
# first or last bucket clipped by the window, what a naive enumeration misses.
_AXIS_CASES = [
    pytest.param(
        QUIET,
        "d",
        [(f"2023-02-{d:02d}", f"2023-02-{d + 1:02d}") for d in range(1, 11)],
        id="calendar_day",
    ),
    pytest.param(
        QUIET,
        "2d",
        [
            ("2023-02-01", "2023-02-03"),
            ("2023-02-03", "2023-02-05"),
            ("2023-02-05", "2023-02-07"),
            ("2023-02-07", "2023-02-09"),
            ("2023-02-09", "2023-02-11"),
        ],
        id="fixed_2d",
    ),
    # 2023-02-01 is a Wednesday, so bucket 0 opens on the Monday before the window
    # and is clipped back to it; the last one is cut short by the end.
    pytest.param(
        QUIET,
        "w",
        [("2023-02-01", "2023-02-06"), ("2023-02-06", "2023-02-11")],
        id="calendar_week_clipped_both_ends",
    ),
    # Months, over enough of them that a position is not always 0: this is the one
    # arm that counts calendar months instead of dividing a length.
    pytest.param(
        {"start": "2023-01-15", "end": "2023-04-10"},
        "m",
        [
            ("2023-01-15", "2023-02-01"),
            ("2023-02-01", "2023-03-01"),
            ("2023-03-01", "2023-04-01"),
            ("2023-04-01", "2023-04-10"),
        ],
        id="calendar_month_multi",
    ),
    # Sub-daily labels carry the time too, so the axis format changes with it.
    pytest.param(
        {"start": "2023-02-01", "end": "2023-02-02"},
        "h",
        [
            (
                f"2023-02-01 {h:02d}:00",
                f"2023-02-0{1 if h < 23 else 2} {(h + 1) % 24:02d}:00",
            )
            for h in range(24)
        ],
        id="calendar_hour",
    ),
]


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(("window", "period", "expected"), _AXIS_CASES)
@pytest.mark.parametrize(
    "endpoint", ["job_counts", "rgu_usage", "rgu_by_cluster", "metric_trend"]
)
def test_bucket_axis_tiles_the_window(dash_client, endpoint, window, period, expected):
    """The axis tiles [start, end) exactly: every bucket present, contiguous, none
    past the end, and identical across the four bucketed endpoints.

    An endpoint returning only the buckets it found data in -- or one too many
    past the end -- would misalign the whole dashboard. Mostly checked on a
    window holding no job at all, the case that tells the two apart.
    """
    payload = dash_client.get(
        f"/dash/metrics/{endpoint}", params={**window, "period": period}
    ).json()
    axis = _period_axis(payload)
    assert axis == expected
    # Contiguous: each bucket resumes where the previous one stopped.
    for (_, end), (start, _) in zip(axis, axis[1:], strict=False):
        assert end == start


# === Input validation =======================================================


@pytest.mark.usefixtures("read_only_db")
def test_invalid_period_returns_400(dash_client):
    r = dash_client.get(
        "/dash/metrics/job_counts", params={**WINDOW, "period": "bogus"}
    )
    assert r.status_code == 400


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize("period", ["0", "0d", "0h", "0.0w", "0.0000000000001d"])
@pytest.mark.parametrize("endpoint", [name for name, _ in _BUCKETED])
def test_zero_length_period_returns_400(dash_client, endpoint, period):
    """A bucket of no length never advances, so enumerating the axis would step
    in place for ever.

    The last case is not written as zero: a timedelta counts microseconds, and
    1e-13 day rounds down to none. Hence a check on the parsed step rather than
    on the number typed -- which is also what makes this list exhaustive, every
    other period being a positive step that reaches the end of the window.
    """
    r = dash_client.get(
        f"/dash/metrics/{endpoint}", params={**WINDOW, "period": period}
    )
    assert r.status_code == 400


@pytest.mark.usefixtures("read_only_db")
def test_invalid_period_returns_400_even_when_ignored(dash_client):
    """``whole=true`` buckets the range itself and never looks at ``period`` --
    but a parameter the endpoint ignores is still one it accepts or refuses, and
    a typo must not be swallowed by the view the caller happens to ask for."""
    r = dash_client.get(
        "/dash/metrics/rgu_usage", params={**WINDOW, "period": "bogus", "whole": "true"}
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


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize("endpoint", [name for name, _ in _BUCKETED])
def test_unknown_cluster_is_checked_before_an_empty_window_shortcut(
    dash_client, endpoint
):
    """Same rule as ``as_user``: a request that would be refused is refused
    whatever the window, so the cluster names are resolved before the empty
    window returns its empty shape."""
    r = dash_client.get(
        f"/dash/metrics/{endpoint}",
        params={
            "start": "2023-02-15",
            "end": "2023-02-15",
            "clusters": ["no_such_cluster"],
        },
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


# Every endpoint taking a focus, with how many jobs its payload accounts for.
_FOCUSED = [
    ("jobs", lambda d: d["total"]),
    ("rgu_by_user", len),
    ("metric_distribution", lambda d: len(d["primary"]["values"])),
    ("metric_comparison", lambda d: sum(sum(row) for row in d["z"])),
    ("job_times_vs_limit", lambda d: d["total_jobs"]),
]


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(("endpoint", "jobs_seen"), _FOCUSED)
def test_focus_outside_the_range_is_an_empty_window(dash_client, endpoint, jobs_seen):
    """A focus clipping past the range asks about no time at all: no jobs.

    The dashboard keeps a focus across a range change, so any range moved off the
    focused slice sends exactly this. It has to collapse to the empty window:
    left inverted, it reaches ``_ran_between`` as a tstzrange whose lower bound
    is above its upper, which Postgres refuses outright.
    """
    r = dash_client.get(
        f"/dash/metrics/{endpoint}",
        params={
            **WINDOW,  # 2023-02-01 -> 2023-03-01
            "focus_start": "2023-05-01T00:00:00Z",
            "focus_end": "2023-05-08T00:00:00Z",
        },
    )
    assert r.status_code == 200
    assert jobs_seen(r.json()) == 0


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


# === Admin "view as user" (as_user) =========================================
# An admin can pass ?as_user=<mila_ldap email> to preview the dashboard scoped
# to that user. JSON endpoints resolve it through _scope_or_view_as (hard 403
# for a non-admin, 404 for an unknown email); the homepage adds a soft guard
# (unknown email -> the admin stays in their own view, no 404 page).


@pytest.mark.parametrize(
    "path,total",
    [(path, total) for _, path, total in _SCOPE_TOTALS],
    ids=[name for name, _, _ in _SCOPE_TOTALS],
)
@pytest.mark.parametrize("email", [_USER, _OTHER_USER], ids=["petit", "beau"])
def test_as_user_matches_direct_scope(app, scoped_db, email, path, total):
    """Admin with ?as_user=X sees exactly what X sees connecting directly."""
    direct = total(app.client(email).get(path, params=WINDOW).json())
    scoped = total(
        app.client(_ADMIN).get(path, params={**WINDOW, "as_user": email}).json()
    )
    assert scoped > 0
    assert scoped == pytest.approx(direct)


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize(
    "path", ["/dash/metrics", "/dash/metrics/job_counts"], ids=["homepage", "json"]
)
def test_as_user_forbidden_for_non_admin(app, path):
    """as_user is admin-only: a regular user supplying it (even another user's
    email) is rejected 403 on both the homepage and a JSON endpoint — it never
    silently degrades to the caller's own scope."""
    app.client(_USER).get(
        path, params={**WINDOW, "as_user": _OTHER_USER}, expect_status=403
    )


@pytest.mark.usefixtures("read_only_db")
@pytest.mark.parametrize("endpoint", [name for name, _ in _BUCKETED])
def test_as_user_is_checked_before_an_empty_window_shortcut(app, endpoint):
    """A window with no bucket answers nothing, but it still answers the right
    caller: every bucketed endpoint resolves its scope before returning its empty
    shape, so a non-admin passing as_user gets its 403 whatever the window."""
    app.client(_USER).get(
        f"/dash/metrics/{endpoint}",
        params={"start": "2023-02-15", "end": "2023-02-15", "as_user": _OTHER_USER},
        expect_status=403,
    )


@pytest.mark.usefixtures("read_only_db")
def test_as_user_unknown_returns_404_on_json(app):
    """An admin targeting an unknown email on a JSON endpoint gets a hard 404 —
    _scope_or_view_as fails closed rather than falling back to the full view."""
    app.client(_ADMIN).get(
        "/dash/metrics/job_counts",
        params={**WINDOW, "as_user": _NOT_IN_DB},
        expect_status=404,
    )


@pytest.mark.usefixtures("read_only_db")
def test_homepage_view_as_valid_user(app):
    """Homepage as_user=<valid>: 200, renders the impersonation badge for that
    user, and drops to the non-admin layout (the 'View as user' entry control
    is replaced by the 'clear' control)."""
    html = app.client(_ADMIN).get("/dash/metrics", params={"as_user": _USER}).text
    assert "as user" in html and _USER in html
    assert 'onclick="clearViewAsUser()"' in html  # the "clear" control is shown
    assert 'onclick="viewAsUser()"' not in html  # the entry control is hidden


@pytest.mark.usefixtures("read_only_db")
def test_homepage_view_as_unknown_is_soft_error(app):
    """Homepage as_user=<unknown>: a typo is a soft error — 200, the admin stays
    in their own view (entry control still present) with an inline message,
    rather than the 404 the JSON endpoints return."""
    resp = app.client(_ADMIN).get("/dash/metrics", params={"as_user": _NOT_IN_DB})
    assert resp.status_code == 200
    assert f'Unknown user "{_NOT_IN_DB}"' in resp.text
    assert 'onclick="viewAsUser()"' in resp.text  # still the admin entry control


_STORAGE_KEY_RE = re.compile(r'const STORAGE_KEY = ("[^"]+")')


def _storage_key(client, params=None):
    """The STORAGE_KEY string literal inlined in the homepage <script>."""
    html = client.get("/dash/metrics", params=params or {}).text
    m = _STORAGE_KEY_RE.search(html)
    assert m, "STORAGE_KEY not found in homepage"
    return m.group(1)


@pytest.mark.usefixtures("read_only_db")
def test_view_as_storage_key_is_distinct(app):
    """The localStorage namespace (STORAGE_KEY) is keyed on (identity, role,
    view-as target): an admin viewing as X gets a bucket distinct from both
    their own view and X's real session, so a preview never clobbers or reuses
    anyone's saved state."""
    admin_own = _storage_key(app.client(_ADMIN))
    admin_as_user = _storage_key(app.client(_ADMIN), {"as_user": _USER})
    user_direct = _storage_key(app.client(_USER))

    assert len({admin_own, admin_as_user, user_direct}) == 3
