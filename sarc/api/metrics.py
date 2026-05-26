import math
import re
from collections.abc import Generator
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from sqlmodel import FLOAT, Session, and_, case, col, func, select

from sarc.config import config
from sarc.db.cluster import SlurmClusterDB, get_available_clusters
from sarc.db.job import SlurmJobDB
from sarc.db.job_series import JobSeriesDB
from sarc.models.job import SlurmState

router = APIRouter(prefix="/dash")


def session_dep() -> Generator[Session]:
    with config().db.session() as sess:
        yield sess


UTC = timezone.utc

_DEFAULT_WINDOW_DAYS = 1
_DEFAULT_PERIOD = "1h"


# Metrics stored in JobSeriesDB.statistics that are normalised to [0, 1]
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


def _apply_common_filters(query, cluster: str | None, cluster_user: str | None):
    """Apply COMPLETED + cluster/user filters to a JobSeriesDB query."""
    query = query.where(JobSeriesDB.job_state == SlurmState.COMPLETED)
    if cluster:
        query = query.where(JobSeriesDB.cluster_name == cluster)
    if cluster_user:
        query = query.where(JobSeriesDB.cluster_user == cluster_user)
    return query


def _resolve_cluster_id(sess: Session, cluster: str | None) -> int | None:
    """Look up the cluster id once; returns None if cluster filter is unset."""
    if not cluster:
        return None
    cid = SlurmClusterDB.id_by_name(sess, cluster)
    if cid is None:
        raise HTTPException(status_code=404, detail=f"Unknown cluster {cluster!r}")
    return cid


def _apply_slurm_job_filters(query, cluster_id: int | None, cluster_user: str | None):
    """Apply COMPLETED + cluster_id/user filters to a SlurmJobDB query.

    Filters by cluster_id (resolved upfront) to avoid the SlurmClusterDB join
    that JobSeriesDB needs for cluster_name.
    """
    query = query.where(SlurmJobDB.job_state == SlurmState.COMPLETED)
    if cluster_id is not None:
        query = query.where(SlurmJobDB.cluster_id == cluster_id)
    if cluster_user:
        query = query.where(SlurmJobDB.cluster_user == cluster_user)
    return query


def _stat_mean(stats: dict | None, metric: str) -> float | None:
    """Read statistics[metric]['mean'] from a JobSeriesDB.statistics JSON value."""
    if not stats:
        return None
    entry = stats.get(metric)
    if not entry:
        return None
    v = entry.get("mean")
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return v


def _stat_mean_sql(metric: str):
    """SQL expression for statistics[metric]['mean'] cast as FLOAT.

    Returns NULL when the path is missing; NaN passes through and must be
    filtered by the caller (via `expr == expr`, since NaN != NaN).
    """
    return _stat_field_sql(metric, "mean")


def _stat_field_sql(metric: str, field: str):
    """SQL expression for statistics[metric][field] cast as FLOAT."""
    return func.cast(
        func.json_extract_path_text(JobSeriesDB.statistics, metric, field), FLOAT
    )


@router.get("/metrics", response_class=HTMLResponse)
def metrics_global_page():
    return _HTML


@router.get("/clusters")
def metrics_clusters(sess: Session = Depends(session_dep)) -> list[str]:
    return sorted(c.name for c in get_available_clusters(sess))


@router.get("/metrics/data")
def metrics_global_data(
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _date_range(start, end)
    step = _parse_period(period)
    fmt = "%Y-%m-%d %H:%M" if step < timedelta(days=1) else "%Y-%m-%d"
    cluster_id = _resolve_cluster_id(sess, cluster)

    # Bucket each job by floor((submit_time - begin) / step) and group in SQL
    # instead of issuing one COUNT per bucket. Queries SlurmJobDB directly to
    # skip the JobSeriesDB view (RGU/statistics aggregations are not needed).
    step_seconds = step.total_seconds()
    bucket_expr = func.floor(
        func.extract("epoch", SlurmJobDB.submit_time - begin_dt) / step_seconds
    ).label("bucket")

    query = select(bucket_expr, func.count().label("count")).where(
        col(SlurmJobDB.submit_time) >= begin_dt, col(SlurmJobDB.submit_time) < finish_dt
    )
    query = _apply_slurm_job_filters(query, cluster_id, cluster_user)
    query = query.group_by(bucket_expr).order_by(bucket_expr)

    counts = {int(row.bucket): int(row.count) for row in sess.exec(query)}

    periods = []
    current = begin_dt
    idx = 0
    while current < finish_dt:
        periods.append(
            {"period_start": current.strftime(fmt), "count": counts.get(idx, 0)}
        )
        current += step
        idx += 1

    return periods


@router.get("/metrics/scatter")
def metrics_global_scatter(
    start: date = Query(default=None),
    end: date = Query(default=None),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    cluster_id = _resolve_cluster_id(sess, cluster)

    query = select(
        SlurmJobDB.elapsed_time,
        SlurmJobDB.time_limit,
        SlurmJobDB.start_time,
        SlurmJobDB.submit_time,
    ).where(
        col(SlurmJobDB.submit_time) >= begin_dt,
        col(SlurmJobDB.submit_time) < finish_dt,
        col(SlurmJobDB.time_limit).is_not(None),
        col(SlurmJobDB.start_time).is_not(None),
    )
    query = _apply_slurm_job_filters(query, cluster_id, cluster_user)

    return [
        {
            "elapsed": elapsed_time,
            "limit": time_limit,
            "wait": (start_time - submit_time).total_seconds(),
        }
        for elapsed_time, time_limit, start_time, submit_time in sess.exec(query)
    ]


_HEATMAP_BINS = 100


def _build_heatmap_payload(
    sess: Session,
    base_filters: list,
    x_expr,
    y_expr,
    x_min: float,
    x_max: float,
    y_min: float,
    y_max: float,
):
    """Aggregate count(*) per (bin_x, bin_y) cell over NBINS×NBINS bins.

    Returns a dense NBINS×NBINS matrix (zero-filled) plus the bin-centre arrays
    used as Plotly heatmap axes. Bounds (min/max) are passed in so callers can
    share a single MIN/MAX pass across multiple heatmaps.
    """
    x_range = max(x_max - x_min, 1e-9)
    y_range = max(y_max - y_min, 1e-9)
    bin_x = func.least(
        func.greatest(func.floor((x_expr - x_min) * _HEATMAP_BINS / x_range), 0),
        _HEATMAP_BINS - 1,
    ).label("bx")
    bin_y = func.least(
        func.greatest(func.floor((y_expr - y_min) * _HEATMAP_BINS / y_range), 0),
        _HEATMAP_BINS - 1,
    ).label("by")
    q = (
        select(bin_x, bin_y, func.count().label("c"))
        .where(*base_filters)
        .group_by(bin_x, bin_y)
    )
    z = [[0] * _HEATMAP_BINS for _ in range(_HEATMAP_BINS)]
    for r in sess.exec(q):
        z[int(r.by)][int(r.bx)] = int(r.c)

    x_step = x_range / _HEATMAP_BINS
    y_step = y_range / _HEATMAP_BINS
    xs = [x_min + (i + 0.5) * x_step for i in range(_HEATMAP_BINS)]
    ys = [y_min + (i + 0.5) * y_step for i in range(_HEATMAP_BINS)]
    return {"x": xs, "y": ys, "z": z}


@router.get("/metrics/heatmap")
def metrics_global_heatmap(
    start: date = Query(default=None),
    end: date = Query(default=None),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    cluster_id = _resolve_cluster_id(sess, cluster)

    wait_expr = func.extract("epoch", SlurmJobDB.start_time - SlurmJobDB.submit_time)
    base_filters = [
        col(SlurmJobDB.submit_time) >= begin_dt,
        col(SlurmJobDB.submit_time) < finish_dt,
        col(SlurmJobDB.time_limit).is_not(None),
        col(SlurmJobDB.start_time).is_not(None),
        SlurmJobDB.job_state == SlurmState.COMPLETED,
    ]
    if cluster_id is not None:
        base_filters.append(SlurmJobDB.cluster_id == cluster_id)
    if cluster_user:
        base_filters.append(SlurmJobDB.cluster_user == cluster_user)

    bounds = sess.exec(
        select(
            func.min(SlurmJobDB.time_limit).label("min_l"),
            func.max(SlurmJobDB.time_limit).label("max_l"),
            func.min(SlurmJobDB.elapsed_time).label("min_e"),
            func.max(SlurmJobDB.elapsed_time).label("max_e"),
            func.min(wait_expr).label("min_w"),
            func.max(wait_expr).label("max_w"),
        ).where(*base_filters)
    ).one()

    if bounds.min_l is None:
        # No matching rows
        return {"elapsed_vs_limit": None, "wait_vs_limit": None}

    elapsed_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        SlurmJobDB.time_limit,
        SlurmJobDB.elapsed_time,
        float(bounds.min_l),
        float(bounds.max_l),
        float(bounds.min_e),
        float(bounds.max_e),
    )
    wait_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        SlurmJobDB.time_limit,
        wait_expr,
        float(bounds.min_l),
        float(bounds.max_l),
        float(bounds.min_w),
        float(bounds.max_w),
    )

    return {"elapsed_vs_limit": elapsed_hmap, "wait_vs_limit": wait_hmap}


_DENSITY_BINS = 50  # matches Plotly nbinsx in the frontend
_PAIRED_SAMPLE_LIMIT = 5000


def _density_bin_expr(metric_expr):
    """SQL expression for floor(metric_expr * NBINS), clipped to [0, NBINS-1]."""
    return func.least(
        func.greatest(func.floor(metric_expr * _DENSITY_BINS), 0), _DENSITY_BINS - 1
    )


def _valid_metric_filter(metric_expr):
    """SQL predicate: metric expr is not NULL, >= 0, and not NaN (NaN != NaN)."""
    return and_(
        metric_expr.is_not(None),
        metric_expr >= 0,
        metric_expr == metric_expr,  # noqa: PLR0124
    )


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
    sess: Session = Depends(session_dep),
):
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")
    if metric2 is not None and metric2 not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric2!r}")

    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    m1 = _stat_mean_sql(metric)
    weight = JobSeriesDB.rgu * JobSeriesDB.elapsed_time
    bin_width = 1.0 / _DENSITY_BINS

    # Common job-population filter shared by primary, secondary and paired.
    # When metric2 is specified, both metric and metric2 must be valid (matches
    # original Python loop: secondary failure skips the primary too).
    base_filters = [
        col(JobSeriesDB.submit_time) >= begin_dt,
        col(JobSeriesDB.submit_time) < finish_dt,
        col(JobSeriesDB.allocated_gpu_type).is_not(None),
        col(JobSeriesDB.rgu).is_not(None),
        _valid_metric_filter(m1),
    ]
    if metric2:
        m2 = _stat_mean_sql(metric2)
        base_filters.append(_valid_metric_filter(m2))
    else:
        m2 = None

    def _binned_query(metric_expr):
        bin_expr = _density_bin_expr(metric_expr).label("bin")
        q = (
            select(bin_expr, func.sum(weight).label("w"))
            .where(*base_filters)
            .group_by(bin_expr)
            .order_by(bin_expr)
        )
        return _apply_common_filters(q, cluster, cluster_user)

    def _bin_to_payload(rows):
        # Convert (bin_index, weight_sum) rows to centred-value/weight lists
        # consumable by Plotly's histogram. Each bin yields a single (x, y)
        # pair at the bin centre, which Plotly's nbinsx=50 will resolve back
        # to a 50-bar density plot.
        values, weights = [], []
        for r in rows:
            centre = (int(r.bin) + 0.5) * bin_width
            values.append(centre)
            weights.append(float(r.w or 0.0))
        return values, weights

    p_values, p_weights = _bin_to_payload(sess.exec(_binned_query(m1)))

    if not metric2 or m2 is None:
        return {
            "primary": {"values": p_values, "weights": p_weights},
            "secondary": None,
            "paired": None,
        }

    s_values, s_weights = _bin_to_payload(sess.exec(_binned_query(m2)))

    # Paired scatter (metric x vs metric2 y): cap to a uniform random sample
    # so the response stays browser-friendly even on multi-million-row windows.
    paired_q = (
        select(m1.label("x"), m2.label("y"))
        .where(*base_filters)
        .order_by(func.random())
        .limit(_PAIRED_SAMPLE_LIMIT)
    )
    paired_q = _apply_common_filters(paired_q, cluster, cluster_user)
    paired_x: list[float] = []
    paired_y: list[float] = []
    for row in sess.exec(paired_q):
        paired_x.append(float(row.x))
        paired_y.append(float(row.y))

    return {
        "primary": {"values": p_values, "weights": p_weights},
        "secondary": {"values": s_values, "weights": s_weights},
        "paired": {"x": paired_x, "y": paired_y},
    }


@router.get("/metrics/histogram")
def metrics_global_histogram(
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    cluster: str | None = Query(default=None),
    cluster_user: str | None = Query(default=None),
    metric: str = Query(default="gpu_sm_occupancy"),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _date_range(start, end)
    step = _parse_period(period)
    fmt = "%Y-%m-%d %H:%M" if step < timedelta(days=1) else "%Y-%m-%d"
    step_seconds = step.total_seconds()

    # Aggregate per bucket directly in SQL: SUM(rgu * elapsed / 3600) for
    # requested, and the same multiplied by the metric mean for used. The
    # `m == m` test filters NaN (NaN != NaN), substituting 0 in that branch.
    bucket_expr = func.floor(
        func.extract("epoch", JobSeriesDB.submit_time - begin_dt) / step_seconds
    ).label("bucket")
    rgu_hours = JobSeriesDB.rgu * JobSeriesDB.elapsed_time / 3600.0
    m_mean = _stat_mean_sql(metric)
    # NaN is its only non-equal value; `m_mean == m_mean` is the SQL idiom.
    rgu_used_term = case(
        (m_mean == m_mean, rgu_hours * m_mean),  # noqa: PLR0124
        else_=0.0,
    )

    query = (
        select(
            bucket_expr,
            func.sum(rgu_hours).label("rgu_requested"),
            func.sum(rgu_used_term).label("rgu_used"),
        )
        .where(
            col(JobSeriesDB.submit_time) >= begin_dt,
            col(JobSeriesDB.submit_time) < finish_dt,
            col(JobSeriesDB.allocated_gpu_type).is_not(None),
            col(JobSeriesDB.rgu).is_not(None),
        )
        .group_by(bucket_expr)
        .order_by(bucket_expr)
    )
    query = _apply_common_filters(query, cluster, cluster_user)

    sums = {
        int(row.bucket): (float(row.rgu_requested or 0.0), float(row.rgu_used or 0.0))
        for row in sess.exec(query)
    }

    period_data = []
    current = begin_dt
    idx = 0
    while current < finish_dt:
        req, used = sums.get(idx, (0.0, 0.0))
        period_data.append(
            {
                "period_start": current.strftime(fmt),
                "rgu_requested": req,
                "rgu_used": used,
            }
        )
        current += step
        idx += 1

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
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    # Aggregate by user directly in SQL: SUM(rgu * elapsed / 3600) per user.
    rgu_hours = JobSeriesDB.rgu * JobSeriesDB.elapsed_time / 3600.0
    m_mean = _stat_mean_sql(metric)
    rgu_used_term = case(
        (m_mean == m_mean, rgu_hours * m_mean),  # noqa: PLR0124
        else_=0.0,
    )
    user_expr = func.coalesce(JobSeriesDB.cluster_user, "unknown").label("user")
    rgu_requested_sum = func.sum(rgu_hours).label("rgu_requested")

    query = (
        select(user_expr, rgu_requested_sum, func.sum(rgu_used_term).label("rgu_used"))
        .where(
            col(JobSeriesDB.submit_time) >= begin_dt,
            col(JobSeriesDB.submit_time) < finish_dt,
            col(JobSeriesDB.allocated_gpu_type).is_not(None),
            col(JobSeriesDB.rgu).is_not(None),
        )
        .group_by(user_expr)
        .order_by(rgu_requested_sum.desc(), user_expr)
    )
    query = _apply_common_filters(query, cluster, cluster_user)

    return [
        {
            "user": row.user,
            "rgu_requested": float(row.rgu_requested or 0.0),
            "rgu_used": float(row.rgu_used or 0.0),
        }
        for row in sess.exec(query)
    ]


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
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    # Sort key (rgu * elapsed) and the stat extractions are pushed into SQL so
    # we only materialise `limit` rows, not the full result set.
    rgu_hours = (JobSeriesDB.rgu * JobSeriesDB.elapsed_time / 3600.0).label("rgu_hours")
    metric_mean = _stat_mean_sql(metric).label("metric_mean")
    gpu_util_mean = _stat_mean_sql("gpu_utilization").label("gpu_utilization_mean")
    gpu_sm_mean = _stat_mean_sql("gpu_sm_occupancy").label("gpu_sm_occupancy_mean")
    gpu_mem_max = _stat_field_sql("gpu_memory", "max").label("gpu_memory_max")

    query = (
        select(  # ty:ignore[no-matching-overload]
            JobSeriesDB.cluster_name,
            JobSeriesDB.cluster_user,
            JobSeriesDB.job_state,
            JobSeriesDB.elapsed_time,
            JobSeriesDB.nodes,
            JobSeriesDB.allocated_gpu_type,
            JobSeriesDB.rgu,
            rgu_hours,
            metric_mean,
            gpu_util_mean,
            gpu_sm_mean,
            gpu_mem_max,
        )
        .where(
            col(JobSeriesDB.submit_time) >= begin_dt,
            col(JobSeriesDB.submit_time) < finish_dt,
            col(JobSeriesDB.allocated_gpu_type).is_not(None),
            col(JobSeriesDB.rgu).is_not(None),
            JobSeriesDB.rgu == JobSeriesDB.rgu,  # NaN guard   # noqa: PLR0124
        )
        .order_by(rgu_hours.desc(), col(JobSeriesDB.cluster_user))
        .limit(limit)
    )
    query = _apply_common_filters(query, cluster, cluster_user)

    jobs = []
    for row in sess.exec(query):
        mm = _nan_to_none(row.metric_mean)
        rh = float(row.rgu_hours)
        waste = round(rh * (1 - mm), 2) if mm is not None else None
        jobs.append(
            {
                "cluster": row.cluster_name or "",
                "user": row.cluster_user or "",
                "job_state": row.job_state.value if row.job_state is not None else "",
                "elapsed": row.elapsed_time or 0,
                "nodes": ", ".join(row.nodes or []) or None,
                "gpu_type": row.allocated_gpu_type or "",
                "rgu": round(float(row.rgu), 2),
                "rgu_hours": round(rh, 2),
                "waste": waste,
                "gpu_utilization_mean": _nan_to_none(row.gpu_utilization_mean),
                "gpu_sm_occupancy_mean": _nan_to_none(row.gpu_sm_occupancy_mean),
                "gpu_memory_max": _nan_to_none(row.gpu_memory_max),
            }
        )

    return jobs


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
      // Server pre-bins into 50 equal bins over [0, 1] (matches xbins below).
      const XBINS = { start: 0, end: 1, size: 1 / 50 };
      if (d.density.primary.values.length) {
        traces.push({
          type: 'histogram', name: ml,
          x: d.density.primary.values, y: d.density.primary.weights,
          histfunc: 'sum', histnorm: 'probability density',
          opacity: metric2 ? 0.7 : 1, marker: { color: '#1a6fd4' },
          xbins: XBINS, autobinx: false,
          hovertemplate: ml + ': %{x:.2f}<br>Density: %{y:.4f}<extra></extra>',
        });
      }
      if (metric2 && d.density.secondary && d.density.secondary.values.length) {
        traces.push({
          type: 'histogram', name: ml2,
          x: d.density.secondary.values, y: d.density.secondary.weights,
          histfunc: 'sum', histnorm: 'probability density',
          opacity: 0.7, marker: { color: '#e05c2a' },
          xbins: XBINS, autobinx: false,
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
