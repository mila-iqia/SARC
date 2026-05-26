import bisect
import math
import re
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse

from sarc.client.gpumetrics import get_cluster_gpu_billings, get_rgus
from sarc.client.job import _jobs_collection, get_available_clusters

router = APIRouter(prefix="/dash")

UTC = timezone.utc

_DEFAULT_WINDOW_DAYS = 1
_DEFAULT_PERIOD = "1h"

_COMPLETED = {"job_state": "COMPLETED"}

# Metrics stored in stored_statistics that are normalised to [0, 1]
_METRICS_0_1: dict[str, str] = {
    "gpu_sm_occupancy": "SM occupancy",
    "gpu_utilization": "GPU utilization",
    "gpu_utilization_fp16": "GPU util. FP16",
    "gpu_utilization_fp32": "GPU util. FP32",
    "gpu_utilization_fp64": "GPU util. FP64",
    "gpu_memory": "GPU memory",
    "system_memory": "System memory",
}


_PERIOD_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([hdwm]?)$", re.IGNORECASE)
_PERIOD_MULTIPLIERS = {"h": 1 / 24, "d": 1, "w": 7, "m": 30}


def _parse_period(s: str) -> timedelta:
    m = _PERIOD_RE.match(s.strip())
    if not m:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period {s!r}. Use N[h/d/w/m] (e.g. 12h, 1d, 2w, 1m).",
        )
    n, unit = float(m.group(1)), (m.group(2) or "d").lower()
    return timedelta(days=n * _PERIOD_MULTIPLIERS[unit])


def _date_range(start, end):
    today = datetime.now(UTC).date()
    if start is None:
        start = today
    if end is None:
        end = today - timedelta(days=_DEFAULT_WINDOW_DAYS)
    begin = min(start, end)
    finish = max(start, end)
    begin_dt = datetime(begin.year, begin.month, begin.day, tzinfo=UTC)
    finish_dt = datetime(finish.year, finish.month, finish.day, tzinfo=UTC) + timedelta(
        days=1
    )
    return begin_dt, finish_dt


def _apply_focus(
    begin_dt: datetime,
    finish_dt: datetime,
    focus_start: datetime | None,
    focus_end: datetime | None,
) -> tuple[datetime, datetime]:
    if focus_start is not None:
        fs = focus_start if focus_start.tzinfo else focus_start.replace(tzinfo=UTC)
        begin_dt = max(begin_dt, fs)
    if focus_end is not None:
        fe = focus_end if focus_end.tzinfo else focus_end.replace(tzinfo=UTC)
        finish_dt = min(finish_dt, fe)
    return begin_dt, finish_dt


def _nan_to_none(v: float | None) -> float | None:
    return None if (isinstance(v, float) and math.isnan(v)) else v


def _cluster_filter(cluster: str | None) -> dict:
    return {"cluster_name": cluster} if cluster else {}


def _user_filter(cluster_user: str | None) -> dict:
    return {"user": cluster_user} if cluster_user else {}


class _RguContext:
    """Pre-loaded data needed to compute RGU for a batch of jobs."""

    def __init__(self):
        self.clusters = {c.cluster_name: c for c in get_available_clusters()}
        self.gpu_to_rgu = get_rgus()
        self._billing_cache: dict[str, list] = {}

    def compute(self, doc: dict) -> float:  # noqa: PLR0911
        """Return RGU for one MongoDB job document, or NaN if not computable."""
        cluster_name = doc.get("cluster_name", "")
        cluster_cfg = self.clusters.get(cluster_name)
        if cluster_cfg is None:
            return math.nan

        alloc = doc.get("allocated", {})
        req = doc.get("requested", {})
        gpu_type = alloc.get("gpu_type")
        if not gpu_type:
            return math.nan

        billing = alloc.get("billing") or 0
        gres_gpu = req.get("gres_gpu") or 0
        if gres_gpu:
            gres_gpu = max(billing, gres_gpu)

        gpu_type_rgu = self.gpu_to_rgu.get(gpu_type.split(":")[0].rstrip(), math.nan)
        if math.isnan(gpu_type_rgu):
            return math.nan

        if cluster_cfg.billing_is_gpu:
            return gres_gpu * gpu_type_rgu

        if cluster_name not in self._billing_cache:
            self._billing_cache[cluster_name] = get_cluster_gpu_billings(cluster_name)
        all_billings = self._billing_cache[cluster_name]
        if not all_billings:
            return math.nan

        end_time = doc.get("end_time") or datetime.now(UTC)
        start_time = end_time - timedelta(seconds=doc.get("elapsed_time", 0))

        if start_time < all_billings[0].since:
            return gres_gpu * gpu_type_rgu

        idx = max(
            0, bisect.bisect_right([b.since for b in all_billings], start_time) - 1
        )
        gpu_billing = all_billings[idx].gpu_to_billing.get(gpu_type, math.nan)
        if math.isnan(gpu_billing):
            return math.nan
        return (gres_gpu / gpu_billing) * gpu_type_rgu


@router.get("/metrics", response_class=HTMLResponse)
def metrics_global_page():
    return _HTML


@router.get("/clusters")
def metrics_clusters() -> list[str]:
    return sorted(c.cluster_name for c in get_available_clusters())


@router.get("/metrics/data")
def metrics_global_data(
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
):
    begin_dt, finish_dt = _date_range(start, end)
    step = _parse_period(period)
    fmt = "%Y-%m-%d %H:%M" if step < timedelta(days=1) else "%Y-%m-%d"
    coll = _jobs_collection().get_collection()

    periods = []
    current = begin_dt
    while current < finish_dt:
        period_end = current + step
        count = coll.count_documents(
            {
                **_COMPLETED,
                **_cluster_filter(cluster),
                **_user_filter(cluster_user),
                "submit_time": {"$gte": current, "$lt": period_end},
            }
        )
        periods.append({"period_start": current.strftime(fmt), "count": count})
        current = period_end

    return periods


@router.get("/metrics/scatter")
def metrics_global_scatter(
    start: date = Query(default=None),
    end: date = Query(default=None),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    coll = _jobs_collection().get_collection()

    query = {
        **_COMPLETED,
        **_cluster_filter(cluster),
        **_user_filter(cluster_user),
        "submit_time": {"$gte": begin_dt, "$lt": finish_dt},
        "elapsed_time": {"$exists": True},
        "time_limit": {"$ne": None},
        "start_time": {"$ne": None},
    }
    cursor = coll.find(
        query,
        {
            "elapsed_time": 1,
            "time_limit": 1,
            "start_time": 1,
            "submit_time": 1,
            "_id": 0,
        },
    )

    return [
        {
            "elapsed": doc["elapsed_time"],
            "limit": doc["time_limit"],
            "wait": (doc["start_time"] - doc["submit_time"]).total_seconds(),
        }
        for doc in cursor
    ]


@router.get("/metrics/density")
def metrics_global_density(
    start: date = Query(default=None),
    end: date = Query(default=None),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    metric: str = Query(default="gpu_sm_occupancy"),
    metric2: str | None = Query(default=None),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
):
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")
    if metric2 is not None and metric2 not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric2!r}")

    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    coll = _jobs_collection().get_collection()
    rgu_ctx = _RguContext()

    # Require primary metric; also require secondary when specified so both
    # density traces and the paired scatter share the same job population.
    query: dict = {
        **_COMPLETED,
        **_cluster_filter(cluster),
        **_user_filter(cluster_user),
        "submit_time": {"$gte": begin_dt, "$lt": finish_dt},
        "allocated.gpu_type": {"$ne": None},
        f"stored_statistics.{metric}.mean": {"$exists": True, "$gte": 0},
    }
    if metric2:
        query[f"stored_statistics.{metric2}.mean"] = {"$exists": True, "$gte": 0}

    projection = {
        "cluster_name": 1,
        "allocated.billing": 1,
        "allocated.gpu_type": 1,
        "requested.gres_gpu": 1,
        "end_time": 1,
        "elapsed_time": 1,
        f"stored_statistics.{metric}.mean": 1,
        "_id": 0,
    }
    if metric2:
        projection[f"stored_statistics.{metric2}.mean"] = 1

    p_values: list[float] = []
    p_weights: list[float] = []
    s_values: list[float] = []
    s_weights: list[float] = []
    paired_x: list[float] = []
    paired_y: list[float] = []

    for doc in coll.find(query, projection):
        stats = doc.get("stored_statistics") or {}
        v1 = (stats.get(metric) or {}).get("mean")
        if v1 is None or math.isnan(v1):
            continue
        gres_rgu = rgu_ctx.compute(doc)
        if math.isnan(gres_rgu):
            continue
        weight = gres_rgu * (doc.get("elapsed_time") or 0)
        p_values.append(v1)
        p_weights.append(weight)
        if metric2:
            v2 = (stats.get(metric2) or {}).get("mean")
            if v2 is not None and not math.isnan(v2):
                s_values.append(v2)
                s_weights.append(weight)
                paired_x.append(v1)
                paired_y.append(v2)

    return {
        "primary": {"values": p_values, "weights": p_weights},
        "secondary": {"values": s_values, "weights": s_weights} if metric2 else None,
        "paired": {"x": paired_x, "y": paired_y} if metric2 else None,
    }


@router.get("/metrics/histogram")
def metrics_global_histogram(
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    metric: str = Query(default="gpu_sm_occupancy"),
):
    begin_dt, finish_dt = _date_range(start, end)
    step = _parse_period(period)
    fmt = "%Y-%m-%d %H:%M" if step < timedelta(days=1) else "%Y-%m-%d"
    coll = _jobs_collection().get_collection()
    rgu_ctx = _RguContext()

    period_starts: list[datetime] = []
    period_data: list[dict] = []
    current = begin_dt
    while current < finish_dt:
        period_starts.append(current)
        period_data.append(
            {
                "period_start": current.strftime(fmt),
                "rgu_requested": 0.0,
                "rgu_used": 0.0,
            }
        )
        current += step

    query = {
        **_COMPLETED,
        **_cluster_filter(cluster),
        **_user_filter(cluster_user),
        "submit_time": {"$gte": begin_dt, "$lt": finish_dt},
        "allocated.gpu_type": {"$ne": None},
    }
    projection = {
        "cluster_name": 1,
        "allocated.billing": 1,
        "allocated.gpu_type": 1,
        "requested.gres_gpu": 1,
        "submit_time": 1,
        "end_time": 1,
        "elapsed_time": 1,
        f"stored_statistics.{metric}.mean": 1,
        "_id": 0,
    }

    for doc in coll.find(query, projection):
        gres_rgu = rgu_ctx.compute(doc)
        if math.isnan(gres_rgu):
            continue

        submit_time = doc.get("submit_time")
        if submit_time is None:
            continue
        bucket = bisect.bisect_right(period_starts, submit_time) - 1
        if bucket < 0 or bucket >= len(period_data):
            continue

        elapsed_h = (doc.get("elapsed_time") or 0) / 3600
        period_data[bucket]["rgu_requested"] += gres_rgu * elapsed_h

        m_mean = (doc.get("stored_statistics") or {}).get(metric, {}).get("mean")
        if m_mean is not None and not math.isnan(m_mean):
            period_data[bucket]["rgu_used"] += gres_rgu * elapsed_h * m_mean

    return period_data


@router.get("/metrics/user_rgu")
def metrics_global_user_rgu(
    start: date = Query(default=None),
    end: date = Query(default=None),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    metric: str = Query(default="gpu_sm_occupancy"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    coll = _jobs_collection().get_collection()
    rgu_ctx = _RguContext()

    query = {
        **_COMPLETED,
        **_cluster_filter(cluster),
        **_user_filter(cluster_user),
        "submit_time": {"$gte": begin_dt, "$lt": finish_dt},
        "allocated.gpu_type": {"$ne": None},
    }
    projection = {
        "cluster_name": 1,
        "user": 1,
        "allocated.billing": 1,
        "allocated.gpu_type": 1,
        "requested.gres_gpu": 1,
        "end_time": 1,
        "elapsed_time": 1,
        f"stored_statistics.{metric}.mean": 1,
        "_id": 0,
    }

    by_user: dict[str, dict] = {}
    for doc in coll.find(query, projection):
        gres_rgu = rgu_ctx.compute(doc)
        if math.isnan(gres_rgu):
            continue

        user = doc.get("user") or "unknown"
        if user not in by_user:
            by_user[user] = {"user": user, "rgu_requested": 0.0, "rgu_used": 0.0}
        elapsed_h = (doc.get("elapsed_time") or 0) / 3600
        by_user[user]["rgu_requested"] += gres_rgu * elapsed_h

        m_mean = (doc.get("stored_statistics") or {}).get(metric, {}).get("mean")
        if m_mean is not None and not math.isnan(m_mean):
            by_user[user]["rgu_used"] += gres_rgu * elapsed_h * m_mean

    return sorted(by_user.values(), key=lambda r: r["rgu_requested"], reverse=True)


@router.get("/metrics/jobs")
def metrics_jobs(
    start: date = Query(default=None),
    end: date = Query(default=None),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    limit: int = Query(default=50, gt=0, le=500),
    metric: str = Query(default="gpu_sm_occupancy"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    coll = _jobs_collection().get_collection()
    rgu_ctx = _RguContext()

    query = {
        **_COMPLETED,
        **_cluster_filter(cluster),
        **_user_filter(cluster_user),
        "submit_time": {"$gte": begin_dt, "$lt": finish_dt},
        "allocated.gpu_type": {"$ne": None},
    }
    projection = {
        "user": 1,
        "job_state": 1,
        "elapsed_time": 1,
        "cluster_name": 1,
        "allocated.billing": 1,
        "allocated.gpu_type": 1,
        "requested.gres_gpu": 1,
        "nodes": 1,
        "end_time": 1,
        "stored_statistics.gpu_utilization.mean": 1,
        "stored_statistics.gpu_sm_occupancy.mean": 1,
        "stored_statistics.gpu_memory.max": 1,
        f"stored_statistics.{metric}.mean": 1,
        "_id": 0,
    }

    jobs = []
    for doc in coll.find(query, projection):
        rgu = rgu_ctx.compute(doc)
        if math.isnan(rgu):
            continue
        stats = doc.get("stored_statistics") or {}
        alloc = doc.get("allocated") or {}
        elapsed = doc.get("elapsed_time") or 0
        rgu_hours = round(rgu * elapsed / 3600, 2)
        metric_mean = _nan_to_none((stats.get(metric) or {}).get("mean"))
        waste = (
            round(rgu_hours * (1 - metric_mean), 2) if metric_mean is not None else None
        )
        jobs.append(
            {
                "cluster": doc.get("cluster_name") or "",
                "user": doc.get("user") or "",
                "job_state": doc.get("job_state") or "",
                "elapsed": elapsed,
                "nodes": ", ".join(doc.get("nodes") or []) or None,
                "gpu_type": alloc.get("gpu_type") or "",
                "rgu": round(rgu, 2),
                "rgu_hours": rgu_hours,
                "waste": waste,
                "gpu_utilization_mean": _nan_to_none(
                    (stats.get("gpu_utilization") or {}).get("mean")
                ),
                "gpu_sm_occupancy_mean": _nan_to_none(
                    (stats.get("gpu_sm_occupancy") or {}).get("mean")
                ),
                "gpu_memory_max": _nan_to_none(
                    (stats.get("gpu_memory") or {}).get("max")
                ),
            }
        )

    jobs.sort(key=lambda j: j["rgu_hours"], reverse=True)
    return jobs[:limit]


_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Global Job Metrics</title>
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 32px; color: #222; }
    h1 { margin-bottom: 24px; }
    .controls {
      display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-end;
      margin-bottom: 24px; padding: 16px; background: #f5f5f5; border-radius: 8px;
    }
    .ctrl { display: flex; flex-direction: column; gap: 4px; }
    label { font-size: 13px; font-weight: 600; color: #555; }
    input[type="date"], input[type="number"], input[type="text"], select {
      padding: 6px 10px; font-size: 14px;
      border: 1px solid #ccc; border-radius: 4px; background: #fff;
    }
    input[type="number"] { width: 90px; }
    input[type="text"]   { width: 120px; }
    button {
      padding: 8px 20px; font-size: 14px; font-weight: 600;
      background: #1a6fd4; color: #fff; border: none; border-radius: 4px; cursor: pointer;
    }
    button:hover { background: #155cb5; }
    #status { font-size: 13px; color: #888; margin-bottom: 12px; min-height: 18px; }

    /* Tile grid */
    #tile-grid { display: flex; flex-direction: column; gap: 20px; }
    .tile-row   { display: flex; gap: 12px; align-items: flex-start; }
    .tile       { flex: 1; min-width: 0; }
    .tile-header {
      display: flex; justify-content: space-between; align-items: center;
      font-size: 13px; font-weight: 600; color: #555; padding: 2px 0 6px;
    }
    .tile-chart  { width: 100%; height: 480px; }
    .tile-remove {
      background: none; border: none; cursor: pointer; color: #bbb;
      font-size: 18px; padding: 0 2px; line-height: 1; font-weight: 400;
    }
    .tile-remove:hover { color: #c00; background: none; }
    .tile-add-col {
      flex-shrink: 0; display: flex; align-items: flex-start; padding-top: 28px;
    }
    .add-tile-btn {
      width: 26px; height: 26px; border-radius: 50%; padding: 0;
      font-size: 17px; font-weight: 300; line-height: 1;
      background: #eee; color: #555; border: 1px solid #ccc;
    }
    .add-tile-btn:hover { background: #dde6f5; color: #1a6fd4; border-color: #1a6fd4; }
    .plot-picker {
      position: absolute; z-index: 9999; background: #fff;
      border: 1px solid #ccc; border-radius: 4px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.15);
      min-width: 160px; padding: 4px 0;
    }
    .plot-picker-item {
      padding: 6px 12px; font-size: 13px; cursor: pointer;
      color: #333; white-space: nowrap;
    }
    .plot-picker-item:hover { background: #f0f4ff; color: #1a6fd4; }
    .tile-change-sel {
      font-size: 12px; padding: 2px 4px; border: 1px solid #e0e0e0;
      border-radius: 3px; color: #666; background: #f8f8f8; max-width: 120px;
    }
    .add-select {
      padding: 4px 6px; font-size: 13px; border: 1px dashed #aaa;
      border-radius: 4px; background: #fafafa; cursor: pointer; color: #555;
    }
    .add-row-wrap {
      display: flex; align-items: center; gap: 8px; padding: 2px 0;
    }
    .add-row-label { font-size: 13px; color: #888; }

    /* Job table */
    .job-table-wrap { width: 100%; height: 100%; overflow: auto; }
    .job-table { width: 100%; border-collapse: collapse; font-size: 13px; }
    .job-table th {
      position: sticky; top: 0; background: #f0f0f0; padding: 6px 8px;
      text-align: left; font-size: 12px; font-weight: 600; color: #444;
      cursor: pointer; user-select: none; white-space: nowrap;
      border-bottom: 2px solid #ccc;
    }
    .job-table th:hover { background: #e4e4e4; }
    .job-table th.sort-asc::after  { content: ' ▲'; font-size: 10px; }
    .job-table th.sort-desc::after { content: ' ▼'; font-size: 10px; }
    .job-table td { padding: 4px 8px; border-bottom: 1px solid #eee; vertical-align: middle; }
    .job-table tr:hover td { background: #f4f6ff; }
    .job-table .col-cmd {
      max-width: 100px; overflow: hidden; text-overflow: ellipsis;
      white-space: nowrap; font-family: monospace; font-size: 11px; color: #555;
    }
  </style>
</head>
<body>
  <h1>Global Job Metrics (Completed Jobs)</h1>
  <div class="controls">
    <div class="ctrl">
      <label for="start">Start</label>
      <input type="date" id="start" />
    </div>
    <div class="ctrl">
      <label for="end">End</label>
      <input type="date" id="end" />
    </div>
    <div class="ctrl">
      <label for="period">Period</label>
      <input type="text" id="period" value="__DEFAULT_PERIOD__" style="width:80px"
             placeholder="e.g. 1d, 12h, 2w" />
    </div>
    <div class="ctrl">
      <label for="focus_start">Focus from</label>
      <input type="datetime-local" id="focus_start" />
    </div>
    <div class="ctrl">
      <label for="focus_end">Focus to</label>
      <input type="datetime-local" id="focus_end" />
    </div>
    <div class="ctrl">
      <label for="cluster-select">Cluster</label>
      <select id="cluster-select"></select>
    </div>
    <div class="ctrl">
      <label for="cluster_user">User</label>
      <input type="text" id="cluster_user" placeholder="all users"
             autocomplete="off" data-lpignore="true" data-form-type="other" />
    </div>
    <div class="ctrl">
      <label for="top_n_users">Top N users</label>
      <input type="number" id="top_n_users" value="16" min="1" step="1" />
    </div>
    <div class="ctrl">
      <label for="metric-select">Primary metric</label>
      <select id="metric-select"></select>
    </div>
    <div class="ctrl">
      <label for="metric2-select">Secondary metric</label>
      <select id="metric2-select"></select>
    </div>
    <button onclick="loadCharts()">Update</button>
  </div>
  <div id="status"></div>
  <div id="tile-grid"></div>

  <script>
    const SEC_TO_H = 1 / 3600;

    function isoDate(d) { return d.toISOString().split('T')[0]; }

    function pLayout(extra) {
      return { autosize: true, margin: { l: 60, r: 20, t: 40, b: 60 }, ...extra };
    }

    const METRICS = {
      gpu_sm_occupancy:     'SM occupancy',
      gpu_utilization:      'GPU utilization',
      gpu_utilization_fp16: 'GPU util. FP16',
      gpu_utilization_fp32: 'GPU util. FP32',
      gpu_utilization_fp64: 'GPU util. FP64',
      gpu_memory:           'GPU memory',
      system_memory:        'System memory',
    };

    const PLOT_NAMES = {
      bar:           'Jobs per period',
      scatter:       'Elapsed vs time limit',
      wait:          'Wait time',
      histogram:     'RGU requested vs used',
      density:       'Metric density',
      metricscatter: 'Metric scatter',
      userrgu:       'RGU by user',
      jobtable:      'Job table',
    };
    const PLOT_KEYS = Object.keys(PLOT_NAMES);

    // Populate metric dropdowns
    const metSel  = document.getElementById('metric-select');
    const met2Sel = document.getElementById('metric2-select');
    const noneOpt = document.createElement('option');
    noneOpt.value = ''; noneOpt.textContent = '— none —';
    met2Sel.appendChild(noneOpt);
    Object.entries(METRICS).forEach(([key, label]) => {
      [metSel, met2Sel].forEach(sel => {
        const opt = document.createElement('option');
        opt.value = key; opt.textContent = label;
        sel.appendChild(opt);
      });
    });
    metSel.value = 'gpu_sm_occupancy';
    met2Sel.value = '';

    // Populate cluster dropdown, then kick off the initial load
    fetch('/dash/clusters').then(r => r.json()).then(clusters => {
      const sel = document.getElementById('cluster-select');
      clusters.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c; opt.textContent = c;
        if (c === 'mila') opt.selected = true;
        sel.appendChild(opt);
      });
      loadCharts();
    });

    const today = new Date();
    const windowAgo = new Date(today);
    windowAgo.setDate(windowAgo.getDate() - __DEFAULT_WINDOW_DAYS__);
    document.getElementById('start').value = isoDate(windowAgo);
    document.getElementById('end').value   = isoDate(today);

    // ── Tile layout ──────────────────────────────────────────────────────────
    // tileLayout: string[][]  e.g. [['bar','scatter'],['histogram']]
    // URL param:  plots=bar,scatter;histogram

    let tileLayout = [];
    let lastData   = null;

    function layoutToParam(l) {
      return l.map(row => row.join(',')).join(';');
    }

    function paramToLayout(s) {
      if (!s) return [['bar']];
      const rows = s.split(';')
        .map(row => row.split(',').filter(p => PLOT_NAMES[p]))
        .filter(r => r.length > 0);
      return rows.length ? rows : [['bar']];
    }

    function syncUrl() {
      const url = new URL(window.location);
      url.searchParams.set('plots', layoutToParam(tileLayout));
      window.history.replaceState({}, '', url);
    }

    let _picker = null;

    function closePicker() {
      if (_picker) { _picker.remove(); _picker = null; }
    }

    function openPicker(anchor, onPick) {
      closePicker();
      const menu = document.createElement('div');
      menu.className = 'plot-picker';
      _picker = menu;
      PLOT_KEYS.forEach(key => {
        const item = document.createElement('div');
        item.className = 'plot-picker-item';
        item.textContent = PLOT_NAMES[key];
        item.addEventListener('click', e => { e.stopPropagation(); closePicker(); onPick(key); });
        menu.appendChild(item);
      });
      document.body.appendChild(menu);
      const r = anchor.getBoundingClientRect();
      menu.style.top  = (r.bottom + window.scrollY + 4) + 'px';
      menu.style.left = (r.right  + window.scrollX - menu.offsetWidth) + 'px';
      setTimeout(() => document.addEventListener('click', closePicker, { once: true }), 0);
    }

    function makeAddTileButton(onAdd) {
      const btn = document.createElement('button');
      btn.className = 'add-tile-btn';
      btn.textContent = '+';
      btn.title = 'Add plot to this row';
      btn.addEventListener('click', e => { e.stopPropagation(); openPicker(btn, onAdd); });
      return btn;
    }

    function makeAddSelect(onAdd) {
      const sel = document.createElement('select');
      sel.className = 'add-select';
      const ph = document.createElement('option');
      ph.value = ''; ph.textContent = '＋ add plot';
      sel.appendChild(ph);
      PLOT_KEYS.forEach(key => {
        const opt = document.createElement('option');
        opt.value = key; opt.textContent = PLOT_NAMES[key];
        sel.appendChild(opt);
      });
      sel.addEventListener('change', () => {
        if (sel.value) { onAdd(sel.value); sel.value = ''; }
      });
      return sel;
    }

    function renderLayout() {
      const grid = document.getElementById('tile-grid');
      grid.innerHTML = '';

      tileLayout.forEach((row, rowIdx) => {
        const rowDiv = document.createElement('div');
        rowDiv.className = 'tile-row';

        row.forEach((plotType, colIdx) => {
          const tile = document.createElement('div');
          tile.className = 'tile';

          const header = document.createElement('div');
          header.className = 'tile-header';
          const title = document.createElement('span');
          title.textContent = PLOT_NAMES[plotType];
          const actions = document.createElement('span');
          actions.style.cssText = 'display:flex;align-items:center;gap:4px';
          const changeSel = document.createElement('select');
          changeSel.className = 'tile-change-sel';
          PLOT_KEYS.forEach(key => {
            const opt = document.createElement('option');
            opt.value = key; opt.textContent = PLOT_NAMES[key];
            if (key === plotType) opt.selected = true;
            changeSel.appendChild(opt);
          });
          changeSel.addEventListener('change', () => {
            tileLayout[rowIdx][colIdx] = changeSel.value;
            syncUrl(); renderLayout();
          });
          const rmBtn = document.createElement('button');
          rmBtn.className = 'tile-remove';
          rmBtn.textContent = '×';
          rmBtn.title = 'Remove';
          rmBtn.onclick = () => removeTile(rowIdx, colIdx);
          actions.appendChild(changeSel);
          actions.appendChild(rmBtn);
          header.appendChild(title);
          header.appendChild(actions);

          const chartDiv = document.createElement('div');
          chartDiv.className = 'tile-chart';
          chartDiv.id = `tile-${rowIdx}-${colIdx}`;

          tile.appendChild(header);
          tile.appendChild(chartDiv);
          rowDiv.appendChild(tile);
        });

        const addCol = document.createElement('div');
        addCol.className = 'tile-add-col';
        addCol.appendChild(makeAddTileButton(pt => {
          tileLayout[rowIdx].push(pt);
          syncUrl();
          renderLayout();
        }));
        rowDiv.appendChild(addCol);
        grid.appendChild(rowDiv);
      });

      // New-row control
      const wrap = document.createElement('div');
      wrap.className = 'add-row-wrap';
      const lbl = document.createElement('span');
      lbl.className = 'add-row-label';
      lbl.textContent = 'New row:';
      wrap.appendChild(lbl);
      wrap.appendChild(makeAddSelect(plotType => {
        tileLayout.push([plotType]);
        syncUrl();
        renderLayout();
      }));
      grid.appendChild(wrap);

      // Render charts into the freshly created divs
      if (lastData) {
        tileLayout.forEach((row, ri) => {
          row.forEach((plotType, ci) => renderPlot(`tile-${ri}-${ci}`, plotType, lastData));
        });
      }
    }

    function removeTile(rowIdx, colIdx) {
      tileLayout[rowIdx].splice(colIdx, 1);
      if (tileLayout[rowIdx].length === 0) tileLayout.splice(rowIdx, 1);
      if (tileLayout.length === 0) tileLayout = [['bar']];
      syncUrl();
      renderLayout();
    }

    // ── Per-plot renderers ───────────────────────────────────────────────────

    function renderPlot(id, type, d) {
      const fn = { bar: renderBar, scatter: renderScatter, wait: renderWait,
                   histogram: renderHistogram, density: renderDensity,
                   metricscatter: renderMetricScatter, userrgu: renderUserRgu,
                   jobtable: renderJobTable }[type];
      if (fn) fn(id, d);
    }

    function renderBar(id, d) {
      if (!d.bar.length) return;
      const colors = d.bar.map(r => {
        const inf = periodInFocus(r.period_start);
        return inf === null ? '#1a6fd4' : inf ? '#e07020' : '#c8d8f0';
      });
      Plotly.react(id, [{
        type: 'bar',
        x: d.bar.map(r => r.period_start), y: d.bar.map(r => r.count),
        marker: { color: colors },
        hovertemplate: 'Period: %{x}<br>Jobs: %{y}<extra></extra>',
      }], pLayout({
        title: { text: 'Completed Jobs per Period (click to focus)', font: { size: 16 } },
        xaxis: { title: 'Period start', type: 'category', tickangle: -45 },
        yaxis: { title: 'Number of jobs' },
        bargap: 0.15, margin: { l: 60, r: 20, t: 40, b: 100 },
      }), { responsive: true }).then(gd => {
        gd.removeAllListeners('plotly_click');
        gd.on('plotly_click', evt => {
          if (evt.points && evt.points.length) clickSetFocus(evt.points[0].x);
        });
      });
    }

    function renderScatter(id, d) {
      if (!d.scatter.length) return;
      Plotly.react(id, [{
        type: 'scatter', mode: 'markers',
        x: d.scatter.map(r => +(r.limit   * SEC_TO_H).toFixed(3)),
        y: d.scatter.map(r => +(r.elapsed * SEC_TO_H).toFixed(3)),
        marker: { color: '#1a6fd4', opacity: 0.4, size: 5 },
        hovertemplate: 'Limit: %{x}h<br>Elapsed: %{y}h<extra></extra>',
      }], pLayout({
        title: { text: 'Elapsed vs Time Limit', font: { size: 16 } },
        xaxis: { title: 'Time limit (hours)' },
        yaxis: { title: 'Elapsed time (hours)' },
      }), { responsive: true });
    }

    function renderWait(id, d) {
      if (!d.scatter.length) return;
      Plotly.react(id, [{
        type: 'scatter', mode: 'markers',
        x: d.scatter.map(r => +(r.limit * SEC_TO_H).toFixed(3)),
        y: d.scatter.map(r => +(r.wait  * SEC_TO_H).toFixed(3)),
        marker: { color: '#1a6fd4', opacity: 0.4, size: 5 },
        hovertemplate: 'Limit: %{x}h<br>Wait: %{y}h<extra></extra>',
      }], pLayout({
        title: { text: 'Wait Time vs Time Limit', font: { size: 16 } },
        xaxis: { title: 'Time limit (hours)' },
        yaxis: { title: 'Wait time (hours)' },
      }), { responsive: true });
    }

    function renderHistogram(id, d) {
      if (!d.histogram.length) return;
      const periods    = d.histogram.map(r => r.period_start);
      const rguUsed    = d.histogram.map(r => r.rgu_used);
      const rguUnused  = d.histogram.map(r => r.rgu_requested - r.rgu_used);
      const colorUsed  = periods.map(p => { const f = periodInFocus(p); return f === null ? '#1a6fd4' : f ? '#e07020' : '#c8d8f0'; });
      const colorUnused= periods.map(p => { const f = periodInFocus(p); return f === null ? '#a8c8f0' : f ? '#f0c090' : '#e4eef8'; });
      Plotly.react(id, [
        { type: 'bar', name: 'Used',   x: periods, y: rguUsed,
          marker: { color: colorUsed },
          hovertemplate: 'Period: %{x}<br>RGU used: %{y:.1f}<extra></extra>' },
        { type: 'bar', name: 'Unused', x: periods, y: rguUnused,
          marker: { color: colorUnused },
          hovertemplate: 'Period: %{x}<br>RGU unused: %{y:.1f}<extra></extra>' },
      ], pLayout({
        title: { text: 'Total RGU per Period — Used vs Unused (click to focus)', font: { size: 16 } },
        barmode: 'stack',
        xaxis: { title: 'Period start', type: 'category', tickangle: -45 },
        yaxis: { title: 'Total RGU·h' },
        legend: { x: 0.8, y: 0.95 },
        margin: { l: 60, r: 20, t: 40, b: 100 },
      }), { responsive: true }).then(gd => {
        gd.removeAllListeners('plotly_click');
        gd.on('plotly_click', evt => {
          if (evt.points && evt.points.length) clickSetFocus(evt.points[0].x);
        });
      });
    }

    function renderDensity(id, d) {
      const metric  = document.getElementById('metric-select').value;
      const metric2 = document.getElementById('metric2-select').value;
      const ml  = METRICS[metric]  || metric;
      const ml2 = metric2 ? (METRICS[metric2] || metric2) : null;
      const traces = [];
      if (d.density.primary.values.length) {
        traces.push({
          type: 'histogram', name: ml,
          x: d.density.primary.values, y: d.density.primary.weights,
          histfunc: 'sum', histnorm: 'probability density',
          opacity: metric2 ? 0.7 : 1, marker: { color: '#1a6fd4' }, nbinsx: 50,
          hovertemplate: ml + ': %{x:.2f}<br>Density: %{y:.4f}<extra></extra>',
        });
      }
      if (metric2 && d.density.secondary && d.density.secondary.values.length) {
        traces.push({
          type: 'histogram', name: ml2,
          x: d.density.secondary.values, y: d.density.secondary.weights,
          histfunc: 'sum', histnorm: 'probability density',
          opacity: 0.7, marker: { color: '#e05c2a' }, nbinsx: 50,
          hovertemplate: ml2 + ': %{x:.2f}<br>Density: %{y:.4f}<extra></extra>',
        });
      }
      if (traces.length) {
        Plotly.react(id, traces, pLayout({
          title: { text: 'Metric Density — weighted by RGU·time', font: { size: 16 } },
          barmode: 'overlay',
          xaxis: { title: 'Metric mean' }, yaxis: { title: 'Density' },
          legend: { x: 0.75, y: 0.95 },
        }), { responsive: true });
      }
    }

    function renderMetricScatter(id, d) {
      const metric  = document.getElementById('metric-select').value;
      const metric2 = document.getElementById('metric2-select').value;
      if (!metric2 || !d.density.paired || !d.density.paired.x.length) return;
      const ml  = METRICS[metric]  || metric;
      const ml2 = METRICS[metric2] || metric2;
      Plotly.react(id, [{
        type: 'scatter', mode: 'markers',
        x: d.density.paired.x, y: d.density.paired.y,
        marker: { color: '#1a6fd4', opacity: 0.4, size: 5 },
        hovertemplate: ml + ': %{x:.3f}<br>' + ml2 + ': %{y:.3f}<extra></extra>',
      }], pLayout({
        title: { text: ml + ' vs ' + ml2, font: { size: 16 } },
        xaxis: { title: ml }, yaxis: { title: ml2 },
      }), { responsive: true });
    }

    function renderUserRgu(id, d) {
      if (!d.userrgu.length) return;
      const topN         = Math.max(1, parseInt(document.getElementById('top_n_users').value, 10) || 10);
      const selectedUser = document.getElementById('cluster_user').value.trim();

      // Top N rows, sorted descending; append selected user below if outside top N
      let topRows = d.userrgu.slice(0, topN);
      if (selectedUser && !topRows.some(r => r.user === selectedUser)) {
        const sel = d.userrgu.find(r => r.user === selectedUser);
        if (sel) topRows = [...topRows, sel];
      }
      // Reverse so highest-RGU user appears at the top of the horizontal chart
      const rows      = topRows.reverse();
      const users     = rows.map(r => r.user);
      const rguUsed   = rows.map(r => r.rgu_used);
      const rguUnused = rows.map(r => r.rgu_requested - r.rgu_used);
      const colorUsed   = rows.map(r => r.user === selectedUser ? '#e07020' : '#1a6fd4');
      const colorUnused = rows.map(r => r.user === selectedUser ? '#f0c090' : '#a8c8f0');
      const chartH    = Math.max(300, users.length * 24 + 80);
      const el = document.getElementById(id);
      if (el) el.style.height = chartH + 'px';
      Plotly.react(id, [
        { type: 'bar', name: 'Used',   orientation: 'h', y: users, x: rguUsed,
          marker: { color: colorUsed },
          hovertemplate: '%{y}<br>RGU used: %{x:.1f}<extra></extra>' },
        { type: 'bar', name: 'Unused', orientation: 'h', y: users, x: rguUnused,
          marker: { color: colorUnused },
          hovertemplate: '%{y}<br>RGU unused: %{x:.1f}<extra></extra>' },
      ], pLayout({
        title: { text: 'Total RGU by User — Used vs Unused (click to filter)', font: { size: 16 } },
        barmode: 'stack', height: chartH,
        xaxis: { title: 'Total RGU' },
        yaxis: { title: '', automargin: true },
        legend: { x: 0.8, y: 1.05, orientation: 'h' },
        margin: { l: 20, r: 20, t: 40, b: 50 },
      }), { responsive: true }).then(gd => {
        gd.removeAllListeners('plotly_click');
        gd.on('plotly_click', data => {
          if (!data.points || !data.points.length) return;
          const clicked  = data.points[0].y;
          const current  = document.getElementById('cluster_user').value.trim();
          document.getElementById('cluster_user').value = clicked === current ? '' : clicked;
          loadCharts();
        });
      });
    }

    function renderJobTable(id, d) {
      const el = document.getElementById(id);
      if (!el) return;
      if (!d.jobs || !d.jobs.length) { el.textContent = 'No jobs found.'; return; }

      const COLS = [
        { key: 'cluster',               label: 'Cluster' },
        { key: 'user',                  label: 'User' },
        { key: 'job_state',             label: 'State' },
        { key: 'elapsed',               label: 'Elapsed',   fmt: v => {
            if (v == null) return '—';
            const s = Math.floor(v), h = Math.floor(s/3600), m = Math.floor((s%3600)/60), sec = s%60;
            return h > 0 ? `${h}h${String(m).padStart(2,'0')}m` : `${m}m${String(sec).padStart(2,'0')}s`;
          }},
        { key: 'nodes',                 label: 'Nodes',     cls: 'col-cmd' },
        { key: 'gpu_type',              label: 'GPU type' },
        { key: 'rgu',                   label: 'RGU',       fmt: v => v != null ? v.toFixed(2) : '—' },
        { key: 'rgu_hours',             label: 'RGU·h',     fmt: v => v != null ? v.toFixed(2) : '—' },
        { key: 'waste',                 label: 'Waste·h',   fmt: v => v != null ? v.toFixed(2) : '—' },
        { key: 'gpu_utilization_mean',  label: 'GPU util',  fmt: v => v != null ? (v*100).toFixed(1)+'%' : '—' },
        { key: 'gpu_sm_occupancy_mean', label: 'SM occ',    fmt: v => v != null ? (v*100).toFixed(1)+'%' : '—' },
        { key: 'gpu_memory_max',        label: 'Mem max',   fmt: v => v != null ? (v*100).toFixed(1)+'%' : '—' },
      ];

      // Persist sort state on the element across re-renders
      let sortKey = el._jtSortKey || 'rgu_hours';
      let sortDir = el._jtSortDir != null ? el._jtSortDir : -1;  // -1 = desc

      function draw() {
        el._jtSortKey = sortKey;
        el._jtSortDir = sortDir;
        const rows = [...d.jobs].sort((a, b) => {
          const av = a[sortKey], bv = b[sortKey];
          if (av == null && bv == null) return 0;
          if (av == null) return 1;
          if (bv == null) return -1;
          return sortDir * (typeof av === 'string' ? av.localeCompare(bv) : av - bv);
        });

        el.innerHTML = '';
        const wrap = document.createElement('div');
        wrap.className = 'job-table-wrap';
        const tbl = document.createElement('table');
        tbl.className = 'job-table';

        const thead = tbl.createTHead();
        const hr = thead.insertRow();
        COLS.forEach(col => {
          const th = document.createElement('th');
          th.textContent = col.label;
          if (col.key === sortKey) th.className = sortDir === 1 ? 'sort-asc' : 'sort-desc';
          th.addEventListener('click', () => {
            if (sortKey === col.key) sortDir = -sortDir;
            else { sortKey = col.key; sortDir = -1; }
            draw();
          });
          hr.appendChild(th);
        });

        const tbody = tbl.createTBody();
        rows.forEach(job => {
          const tr = tbody.insertRow();
          COLS.forEach(col => {
            const td = tr.insertCell();
            const raw = job[col.key];
            td.textContent = col.fmt ? col.fmt(raw) : (raw ?? '—');
            if (col.cls) { td.className = col.cls; td.title = raw || ''; }
          });
        });

        wrap.appendChild(tbl);
        el.appendChild(wrap);
      }

      draw();
    }

    // ── Focus helpers ────────────────────────────────────────────────────────

    function periodToMs() {
      const s = document.getElementById('period').value.trim();
      const m = s.match(/^(\d+(?:\.\d+)?)\s*([hdwm]?)$/i);
      if (!m) return 86400000;
      const n = parseFloat(m[1]), u = (m[2] || 'd').toLowerCase();
      return n * { h: 3600000, d: 86400000, w: 604800000, m: 2592000000 }[u];
    }

    function periodStartToMs(s) {
      return new Date(s.length === 10 ? s + 'T00:00:00Z' : s.replace(' ', 'T') + ':00Z').getTime();
    }

    // Returns true/false if focus is active, null if no focus set (all bars same)
    function periodInFocus(periodStartStr) {
      const fs = document.getElementById('focus_start').value;
      const fe = document.getElementById('focus_end').value;
      if (!fs && !fe) return null;
      const pMs = periodStartToMs(periodStartStr);
      const fsMs = fs ? new Date(fs + ':00Z').getTime() : -Infinity;
      const feMs = fe ? new Date(fe + ':00Z').getTime() :  Infinity;
      return pMs >= fsMs && pMs < feMs;
    }

    function clickSetFocus(periodStartStr) {
      const startMs   = periodStartToMs(periodStartStr);
      const toInput   = ms => new Date(ms).toISOString().slice(0, 16);
      const newStart  = toInput(startMs);
      const newEnd    = toInput(startMs + periodToMs());
      const curStart  = document.getElementById('focus_start').value;
      const curEnd    = document.getElementById('focus_end').value;
      // Toggle off if clicking the already-focused period
      if (curStart === newStart && curEnd === newEnd) {
        document.getElementById('focus_start').value = '';
        document.getElementById('focus_end').value   = '';
      } else {
        document.getElementById('focus_start').value = newStart;
        document.getElementById('focus_end').value   = newEnd;
      }
      loadCharts();
    }

    // ── Data loading ─────────────────────────────────────────────────────────

    async function loadCharts() {
      const start        = document.getElementById('start').value;
      const end          = document.getElementById('end').value;
      const period       = document.getElementById('period').value.trim();
      const cluster      = document.getElementById('cluster-select').value;
      const cluster_user = document.getElementById('cluster_user').value.trim();
      const metric       = document.getElementById('metric-select').value;
      const metric2      = document.getElementById('metric2-select').value;
      const focus_start  = document.getElementById('focus_start').value;
      const focus_end    = document.getElementById('focus_end').value;
      const status       = document.getElementById('status');

      if (!start || !end) { status.textContent = 'Please select start and end dates.'; return; }

      const focusParams = {
        ...(focus_start ? { focus_start } : {}),
        ...(focus_end   ? { focus_end }   : {}),
      };

      status.textContent = 'Loading…';
      try {
        const base          = { start, end, cluster, ...(cluster_user ? { cluster_user } : {}) };
        const densityParams = { ...base, metric, ...(metric2 ? { metric2 } : {}) };
        const [barResp, scatterResp, histResp, densityResp, userRguResp, jobsResp] = await Promise.all([
          fetch('/dash/metrics/data?'      + new URLSearchParams({ ...base, period })),
          fetch('/dash/metrics/scatter?'   + new URLSearchParams({ ...base, ...focusParams })),
          fetch('/dash/metrics/histogram?' + new URLSearchParams({ ...base, period, metric })),
          fetch('/dash/metrics/density?'   + new URLSearchParams({ ...densityParams, ...focusParams })),
          fetch('/dash/metrics/user_rgu?'  + new URLSearchParams({ start, end, cluster, metric, ...focusParams })),
          fetch('/dash/metrics/jobs?'      + new URLSearchParams({ ...base, metric, ...focusParams })),
        ]);

        if (!barResp.ok || !scatterResp.ok || !histResp.ok || !densityResp.ok || !userRguResp.ok || !jobsResp.ok) {
          status.textContent = 'Error fetching data.'; return;
        }

        const [barData, scatterData, histData, densityData, userRguData, jobsData] = await Promise.all([
          barResp.json(), scatterResp.json(), histResp.json(), densityResp.json(), userRguResp.json(), jobsResp.json(),
        ]);

        lastData = { bar: barData, scatter: scatterData, histogram: histData,
                     density: densityData, userrgu: userRguData, jobs: jobsData };
        renderLayout();

        status.textContent =
          barData.length + ' period(s), ' + scatterData.length + ' scatter job(s), ' +
          histData.length + ' RGU period(s), ' + densityData.primary.values.length +
          ' density job(s), ' + userRguData.length + ' user(s), ' + jobsData.length + ' jobs loaded.';
      } catch (e) {
        status.textContent = 'Request failed: ' + e.message;
      }
    }

    // ── Init ─────────────────────────────────────────────────────────────────

    tileLayout = paramToLayout(new URLSearchParams(window.location.search).get('plots'));
    renderLayout();
  </script>
</body>
</html>""".replace("__DEFAULT_WINDOW_DAYS__", str(_DEFAULT_WINDOW_DAYS)).replace(
    "__DEFAULT_PERIOD__", _DEFAULT_PERIOD
)
