"""Tests for the `/dash` dashboard endpoints in ``sarc.api.metrics``.

Each endpoint is checked twice: an empty-data test (default window, no jobs)
and a value test on enriched data. Plus branch tests (period bucketing,
validation, filters, sort) and the basic-auth tests. The HTML page is not tested.
"""

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import col, func, select

from sarc.api import auth
from sarc.api.metrics import router as metrics_router
from sarc.config import config
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.support import GpuRguDB

# Covers every factory-seeded job (submitted from 2023-02-14, +6h each).
WINDOW = {"start": "2023-02-01", "end": "2023-03-01"}


@pytest.fixture
def dash_client():
    """TestClient over an app mounting only the /dash router."""
    app = FastAPI()
    app.include_router(metrics_router)
    return TestClient(app)


# === Empty-data smoke =======================================================
# Each endpoint on the default window (no jobs): assert the right empty
# container. clusters/job_states are static, so only their type is checked.
EMPTY_ENDPOINTS = [
    ("clusters", "/dash/clusters", lambda d: isinstance(d, list)),
    ("job_states", "/dash/job_states", lambda d: isinstance(d, list)),
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
        lambda d: (
            d["primary"]["values"] == []
            and d["secondary"] is None
            and d["paired"] is None
        ),
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


# === Value tests (enriched data) ============================================
# Constants make the RGU/metric outputs exact: physical RGU (the default
# rgu_type) = allocated_gres_gpu * drac_rgu; rgu_hours = rgu * elapsed / 3600.
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
def dash_db(read_write_db):
    """Writable DB with a few jobs turned into GPU jobs (+ statistics).

    Returns the facts the value tests assert against.
    """
    sess = read_write_db
    # GpuRguDB first: harmonized_gpu_type is an FK to it.
    sess.add(GpuRguDB(name=_GPU, rgu=10.0, drac_rgu=_DRAC_RGU))
    sess.flush()

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
    """Bucketed counts sum to the jobs in the window."""
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


def test_metric_distribution_paired_with_data(dash_client, dash_db):
    """metric2 -> 2D pass with both marginals and the paired heatmap.

    All jobs at gpu_utilization 0.4 / gpu_memory 0.6 -> one cell (bx=40, by=60),
    marginals at 0.41 / 0.61.
    """
    data = dash_client.get(
        "/dash/metrics/metric_distribution",
        params={**WINDOW, "metric": "gpu_utilization", "metric2": "gpu_memory"},
    ).json()
    assert data["primary"]["values"] == pytest.approx([0.41])
    assert sum(data["primary"]["weights"]) == pytest.approx(dash_db.total_weight)
    assert data["secondary"]["values"] == pytest.approx([0.61])
    assert sum(data["secondary"]["weights"]) == pytest.approx(dash_db.total_weight)
    z = data["paired"]["z"]
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
        ("/dash/metrics/metric_trend", "metric"),
        ("/dash/metrics/metric_trend", "metric2"),
        ("/dash/metrics/metric_distribution", "metric"),
        ("/dash/metrics/metric_distribution", "metric2"),
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
def test_physical_path_filters(dash_client):
    """Cluster + user + state filters on the SlurmJobDB path."""
    r = dash_client.get("/dash/metrics/job_counts", params={**WINDOW, **_FILTERS})
    assert r.status_code == 200


@pytest.mark.usefixtures("read_only_db")
def test_billing_path_filters(dash_client):
    """Same filters on the billing (job_series_view) path."""
    r = dash_client.get(
        "/dash/metrics/rgu_usage", params={**WINDOW, "rgu_type": "billing", **_FILTERS}
    )
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
@pytest.mark.parametrize("sort_by", ["waste", "gpu_utilization_mean", "gpu_memory_max"])
def test_jobs_sort_by_stat_column(dash_client, sort_by):
    """Sorting by a stat column joins that stat into the page subquery."""
    r = dash_client.get("/dash/metrics/jobs", params={**WINDOW, "sort_by": sort_by})
    assert r.status_code == 200


# === Basic auth gate ========================================================

_USER, _PASSWORD = "dashuser", "s3cret"


@pytest.fixture
def basic_auth_enabled(monkeypatch):
    """Enable the env-driven gate by patching the import-time module globals."""
    monkeypatch.setattr(auth, "_DASH_BASIC_AUTH_USER", _USER)
    monkeypatch.setattr(auth, "_DASH_BASIC_AUTH_PASSWORD", _PASSWORD)


@pytest.mark.usefixtures("read_only_db")
def test_basic_auth_off_by_default(dash_client):
    """No credentials configured -> the gate is a no-op."""
    assert dash_client.get("/dash/clusters").status_code == 200


@pytest.mark.usefixtures("read_only_db", "basic_auth_enabled")
def test_basic_auth_missing_credentials_401(dash_client):
    assert dash_client.get("/dash/clusters").status_code == 401


@pytest.mark.usefixtures("read_only_db", "basic_auth_enabled")
def test_basic_auth_wrong_credentials_401(dash_client):
    r = dash_client.get("/dash/clusters", auth=(_USER, "wrong"))
    assert r.status_code == 401


@pytest.mark.usefixtures("read_only_db", "basic_auth_enabled")
def test_basic_auth_correct_credentials_200(dash_client):
    r = dash_client.get("/dash/clusters", auth=(_USER, _PASSWORD))
    assert r.status_code == 200
