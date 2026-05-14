import bisect
import math
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse

from sarc.client.gpumetrics import get_cluster_gpu_billings, get_rgus
from sarc.client.job import _jobs_collection, get_available_clusters

router = APIRouter(prefix="/dash")

UTC = timezone.utc

_COMPLETED = {"job_state": "COMPLETED"}

# Metrics stored in stored_statistics that are normalised to [0, 1]
_METRICS_0_1: dict[str, str] = {
    "gpu_sm_occupancy":      "SM occupancy",
    "gpu_utilization":       "GPU utilization",
    "gpu_utilization_fp16":  "GPU util. FP16",
    "gpu_utilization_fp32":  "GPU util. FP32",
    "gpu_utilization_fp64":  "GPU util. FP64",
    "gpu_memory":            "GPU memory",
    "system_memory":         "System memory",
}


def _date_range(start, end):
    today = datetime.now(UTC).date()
    if start is None:
        start = today
    if end is None:
        end = today - timedelta(days=7)
    begin = min(start, end)
    finish = max(start, end)
    begin_dt = datetime(begin.year, begin.month, begin.day, tzinfo=UTC)
    finish_dt = datetime(finish.year, finish.month, finish.day, tzinfo=UTC) + timedelta(days=1)
    return begin_dt, finish_dt


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

    def compute(self, doc: dict) -> float:
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

        idx = max(0, bisect.bisect_right([b.since for b in all_billings], start_time) - 1)
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
    range_days: float = Query(default=1.0, gt=0),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
):
    begin_dt, finish_dt = _date_range(start, end)
    step = timedelta(days=range_days)
    coll = _jobs_collection().get_collection()

    periods = []
    current = begin_dt
    while current < finish_dt:
        period_end = current + step
        count = coll.count_documents(
            {**_COMPLETED, **_cluster_filter(cluster), **_user_filter(cluster_user), "submit_time": {"$gte": current, "$lt": period_end}}
        )
        periods.append({"period_start": current.date().isoformat(), "count": count})
        current = period_end

    return periods


@router.get("/metrics/scatter")
def metrics_global_scatter(
    start: date = Query(default=None),
    end: date = Query(default=None),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
):
    begin_dt, finish_dt = _date_range(start, end)
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
        query, {"elapsed_time": 1, "time_limit": 1, "start_time": 1, "submit_time": 1, "_id": 0}
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
):
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")
    if metric2 is not None and metric2 not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric2!r}")

    begin_dt, finish_dt = _date_range(start, end)
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

    p_values:  list[float] = []
    p_weights: list[float] = []
    s_values:  list[float] = []
    s_weights: list[float] = []
    paired_x:  list[float] = []
    paired_y:  list[float] = []

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
        "primary":   {"values": p_values,  "weights": p_weights},
        "secondary": {"values": s_values,  "weights": s_weights} if metric2 else None,
        "paired":    {"x": paired_x, "y": paired_y}              if metric2 else None,
    }


@router.get("/metrics/histogram")
def metrics_global_histogram(
    start: date = Query(default=None),
    end: date = Query(default=None),
    range_days: float = Query(default=1.0, gt=0),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    metric: str = Query(default="gpu_sm_occupancy"),
):
    begin_dt, finish_dt = _date_range(start, end)
    step = timedelta(days=range_days)
    coll = _jobs_collection().get_collection()
    rgu_ctx = _RguContext()

    period_starts: list[datetime] = []
    period_data: list[dict] = []
    current = begin_dt
    while current < finish_dt:
        period_starts.append(current)
        period_data.append({"period_start": current.date().isoformat(), "rgu_requested": 0.0, "rgu_used": 0.0})
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

        period_data[bucket]["rgu_requested"] += gres_rgu

        m_mean = (doc.get("stored_statistics") or {}).get(metric, {}).get("mean")
        if m_mean is not None and not math.isnan(m_mean):
            period_data[bucket]["rgu_used"] += gres_rgu * m_mean

    return period_data


@router.get("/metrics/user_rgu")
def metrics_global_user_rgu(
    start: date = Query(default=None),
    end: date = Query(default=None),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    metric: str = Query(default="gpu_sm_occupancy"),
):
    begin_dt, finish_dt = _date_range(start, end)
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
        by_user[user]["rgu_requested"] += gres_rgu

        m_mean = (doc.get("stored_statistics") or {}).get(metric, {}).get("mean")
        if m_mean is not None and not math.isnan(m_mean):
            by_user[user]["rgu_used"] += gres_rgu * m_mean

    return sorted(by_user.values(), key=lambda r: r["rgu_requested"], reverse=True)


_HTML = """<!DOCTYPE html>
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
      margin-bottom: 24px; padding: 16px; background: #f5f5f5;
      border-radius: 8px;
    }
    .ctrl { display: flex; flex-direction: column; gap: 4px; }
    label { font-size: 13px; font-weight: 600; color: #555; }
    input[type="date"], input[type="number"], input[type="text"], select {
      padding: 6px 10px; font-size: 14px;
      border: 1px solid #ccc; border-radius: 4px; background: #fff;
    }
    input[type="number"] { width: 90px; }
    input[type="text"] { width: 120px; }
    button {
      padding: 8px 20px; font-size: 14px; font-weight: 600;
      background: #1a6fd4; color: #fff; border: none;
      border-radius: 4px; cursor: pointer;
    }
    button:hover { background: #155cb5; }
    #status { font-size: 13px; color: #888; margin-bottom: 8px; min-height: 18px; }
    .chart { width: 100%; height: 520px; }
    .chart.hidden { display: none; }
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
      <label for="range_days">Period (days)</label>
      <input type="number" id="range_days" value="1" min="0.1" step="0.1" />
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
      <label for="metric-select">Primary metric</label>
      <select id="metric-select"></select>
    </div>
    <div class="ctrl">
      <label for="metric2-select">Secondary metric</label>
      <select id="metric2-select"></select>
    </div>
    <div class="ctrl">
      <label for="plot-select">Plot</label>
      <select id="plot-select" onchange="showPlot(this.value)">
        <option value="bar">Jobs per period</option>
        <option value="scatter">Elapsed vs time limit</option>
        <option value="wait">Wait time vs time limit</option>
        <option value="histogram">RGU requested vs used</option>
        <option value="density">Metric density</option>
        <option value="metricscatter">Metric scatter</option>
        <option value="userrgu">RGU by user</option>
      </select>
    </div>
    <button onclick="loadCharts()">Update</button>
  </div>
  <div id="status"></div>
  <div id="chart-bar" class="chart"></div>
  <div id="chart-scatter" class="chart hidden"></div>
  <div id="chart-wait" class="chart hidden"></div>
  <div id="chart-histogram" class="chart hidden"></div>
  <div id="chart-density" class="chart hidden"></div>
  <div id="chart-metricscatter" class="chart hidden"></div>
  <div id="chart-userrgu" class="chart hidden"></div>

  <script>
    const SEC_TO_H = 1 / 3600;

    function isoDate(d) { return d.toISOString().split('T')[0]; }

    function layout(extra) {
      return { autosize: true, margin: { l: 60, r: 20, t: 60, b: 60 }, ...extra };
    }

    function showPlot(which) {
      ['bar', 'scatter', 'wait', 'histogram', 'density', 'metricscatter', 'userrgu'].forEach(id => {
        document.getElementById('chart-' + id).classList.toggle('hidden', id !== which);
      });
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

    // Populate cluster dropdown
    fetch('/dash/clusters').then(r => r.json()).then(clusters => {
      const sel = document.getElementById('cluster-select');
      clusters.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c;
        opt.textContent = c;
        if (c === 'mila') opt.selected = true;
        sel.appendChild(opt);
      });
    });

    const today = new Date();
    const sevenAgo = new Date(today);
    sevenAgo.setDate(sevenAgo.getDate() - 7);
    document.getElementById('start').value = isoDate(sevenAgo);
    document.getElementById('end').value = isoDate(today);

    async function loadCharts() {
      const start   = document.getElementById('start').value;
      const end     = document.getElementById('end').value;
      const rangeDays = document.getElementById('range_days').value;
      const cluster  = document.getElementById('cluster-select').value;
      const cluster_user = document.getElementById('cluster_user').value.trim();
      const metric       = document.getElementById('metric-select').value;
      const metric2      = document.getElementById('metric2-select').value;
      const status       = document.getElementById('status');

      if (!start || !end) { status.textContent = 'Please select start and end dates.'; return; }

      status.textContent = 'Loading…';
      try {
        const base = { start, end, cluster, ...(cluster_user ? { cluster_user } : {}) };
        const densityParams = { ...base, metric, ...(metric2 ? { metric2 } : {}) };
        const [barResp, scatterResp, histResp, densityResp, userRguResp] = await Promise.all([
          fetch('/dash/metrics/data?'      + new URLSearchParams({ ...base, range_days: rangeDays })),
          fetch('/dash/metrics/scatter?'   + new URLSearchParams(base)),
          fetch('/dash/metrics/histogram?' + new URLSearchParams({ ...base, range_days: rangeDays, metric })),
          fetch('/dash/metrics/density?'   + new URLSearchParams(densityParams)),
          fetch('/dash/metrics/user_rgu?'  + new URLSearchParams({ ...base, metric })),
        ]);

        if (!barResp.ok || !scatterResp.ok || !histResp.ok || !densityResp.ok || !userRguResp.ok) {
          status.textContent = 'Error fetching data.';
          return;
        }

        const [barData, scatterData, histData, densityData, userRguData] = await Promise.all([
          barResp.json(), scatterResp.json(), histResp.json(), densityResp.json(), userRguResp.json(),
        ]);

        // Bar chart
        if (barData.length) {
          Plotly.react('chart-bar', [{
            type: 'bar',
            x: barData.map(d => d.period_start),
            y: barData.map(d => d.count),
            marker: { color: '#1a6fd4' },
            hovertemplate: 'Period: %{x}<br>Jobs: %{y}<extra></extra>',
          }], layout({
            title: { text: 'Completed Jobs per Period', font: { size: 18 } },
            xaxis: { title: 'Period start', type: 'category', tickangle: -45 },
            yaxis: { title: 'Number of jobs' },
            bargap: 0.15, margin: { l: 60, r: 20, t: 60, b: 100 },
          }), { responsive: true });
        }

        // Elapsed vs time limit scatter
        if (scatterData.length) {
          Plotly.react('chart-scatter', [{
            type: 'scatter', mode: 'markers',
            x: scatterData.map(d => +(d.limit   * SEC_TO_H).toFixed(3)),
            y: scatterData.map(d => +(d.elapsed * SEC_TO_H).toFixed(3)),
            marker: { color: '#1a6fd4', opacity: 0.4, size: 5 },
            hovertemplate: 'Limit: %{x}h<br>Elapsed: %{y}h<extra></extra>',
          }], layout({
            title: { text: 'Elapsed vs Time Limit (Completed Jobs)', font: { size: 18 } },
            xaxis: { title: 'Time limit (hours)' },
            yaxis: { title: 'Elapsed time (hours)' },
          }), { responsive: true });
        }

        // Wait time vs time limit scatter
        if (scatterData.length) {
          Plotly.react('chart-wait', [{
            type: 'scatter', mode: 'markers',
            x: scatterData.map(d => +(d.limit * SEC_TO_H).toFixed(3)),
            y: scatterData.map(d => +(d.wait  * SEC_TO_H).toFixed(3)),
            marker: { color: '#1a6fd4', opacity: 0.4, size: 5 },
            hovertemplate: 'Limit: %{x}h<br>Wait: %{y}h<extra></extra>',
          }], layout({
            title: { text: 'Wait Time vs Time Limit (Completed Jobs)', font: { size: 18 } },
            xaxis: { title: 'Time limit (hours)' },
            yaxis: { title: 'Wait time (hours)' },
          }), { responsive: true });
        }

        // RGU stacked bar (waterline = used / requested split)
        if (histData.length) {
          const periods   = histData.map(d => d.period_start);
          const rguUsed   = histData.map(d => d.rgu_used);
          const rguUnused = histData.map(d => d.rgu_requested - d.rgu_used);
          Plotly.react('chart-histogram', [
            {
              type: 'bar', name: 'Used',
              x: periods, y: rguUsed,
              marker: { color: '#1a6fd4' },
              hovertemplate: 'Period: %{x}<br>RGU used: %{y:.1f}<extra></extra>',
            },
            {
              type: 'bar', name: 'Unused',
              x: periods, y: rguUnused,
              marker: { color: '#a8c8f0' },
              hovertemplate: 'Period: %{x}<br>RGU unused: %{y:.1f}<extra></extra>',
            },
          ], layout({
            title: { text: 'Total RGU per Period — Used vs Unused', font: { size: 18 } },
            barmode: 'stack',
            xaxis: { title: 'Period start', type: 'category', tickangle: -45 },
            yaxis: { title: 'Total RGU' },
            legend: { x: 0.8, y: 0.95 },
            margin: { l: 60, r: 20, t: 60, b: 100 },
          }), { responsive: true });
        }

        // Metric density (weighted by RGU x elapsed) — one or two overlaid traces
        const metricLabel  = METRICS[metric]  || metric;
        const metric2Label = metric2 ? (METRICS[metric2] || metric2) : null;
        const densityTraces = [];
        if (densityData.primary.values.length) {
          densityTraces.push({
            type: 'histogram', name: metricLabel,
            x: densityData.primary.values, y: densityData.primary.weights,
            histfunc: 'sum', histnorm: 'probability density',
            opacity: metric2 ? 0.7 : 1,
            marker: { color: '#1a6fd4' }, nbinsx: 50,
            hovertemplate: metricLabel + ': %{x:.2f}<br>Density: %{y:.4f}<extra></extra>',
          });
        }
        if (metric2 && densityData.secondary && densityData.secondary.values.length) {
          densityTraces.push({
            type: 'histogram', name: metric2Label,
            x: densityData.secondary.values, y: densityData.secondary.weights,
            histfunc: 'sum', histnorm: 'probability density',
            opacity: 0.7,
            marker: { color: '#e05c2a' }, nbinsx: 50,
            hovertemplate: metric2Label + ': %{x:.2f}<br>Density: %{y:.4f}<extra></extra>',
          });
        }
        if (densityTraces.length) {
          Plotly.react('chart-density', densityTraces, layout({
            title: { text: 'Metric Density — weighted by RGU·time', font: { size: 18 } },
            barmode: 'overlay',
            xaxis: { title: 'Metric mean' },
            yaxis: { title: 'Density' },
            legend: { x: 0.75, y: 0.95 },
          }), { responsive: true });
        }

        // Metric scatter (primary x, secondary y) — only when metric2 selected
        if (metric2 && densityData.paired && densityData.paired.x.length) {
          Plotly.react('chart-metricscatter', [{
            type: 'scatter', mode: 'markers',
            x: densityData.paired.x, y: densityData.paired.y,
            marker: { color: '#1a6fd4', opacity: 0.4, size: 5 },
            hovertemplate: metricLabel + ': %{x:.3f}<br>' + metric2Label + ': %{y:.3f}<extra></extra>',
          }], layout({
            title: { text: metricLabel + ' vs ' + metric2Label, font: { size: 18 } },
            xaxis: { title: metricLabel },
            yaxis: { title: metric2Label },
          }), { responsive: true });
        }

        // RGU by user (stacked bar, same waterline style)
        if (userRguData.length) {
          // Sorted descending by requested — reverse for horizontal bars so top = most
          const rows      = [...userRguData].reverse();
          const users     = rows.map(d => d.user);
          const rguUsed   = rows.map(d => d.rgu_used);
          const rguUnused = rows.map(d => d.rgu_requested - d.rgu_used);
          const rowHeight = 24;
          const chartH    = Math.max(300, users.length * rowHeight + 80);
          Plotly.react('chart-userrgu', [
            {
              type: 'bar', name: 'Used', orientation: 'h',
              y: users, x: rguUsed,
              marker: { color: '#1a6fd4' },
              hovertemplate: '%{y}<br>RGU used: %{x:.1f}<extra></extra>',
            },
            {
              type: 'bar', name: 'Unused', orientation: 'h',
              y: users, x: rguUnused,
              marker: { color: '#a8c8f0' },
              hovertemplate: '%{y}<br>RGU unused: %{x:.1f}<extra></extra>',
            },
          ], layout({
            title: { text: 'Total RGU by User — Used vs Unused', font: { size: 18 } },
            barmode: 'stack',
            height: chartH,
            xaxis: { title: 'Total RGU' },
            yaxis: { title: '', automargin: true },
            legend: { x: 0.8, y: 1.05, orientation: 'h' },
            margin: { l: 20, r: 20, t: 60, b: 50 },
          }), { responsive: true });
          document.getElementById('chart-userrgu').style.height = chartH + 'px';
        }

        status.textContent =
          barData.length + ' period(s), ' + scatterData.length + ' scatter job(s), ' +
          histData.length + ' RGU period(s), ' + densityData.primary.values.length + ' density job(s), ' +
          userRguData.length + ' user(s) loaded.';
      } catch (e) {
        status.textContent = 'Request failed: ' + e.message;
      }
    }

    loadCharts();
  </script>
</body>
</html>"""
