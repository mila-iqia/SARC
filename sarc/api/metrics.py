import math
import re
from collections.abc import Generator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import aliased
from sqlmodel import Session, and_, case, col, func, select, text

from sarc.api.auth import require_basic_auth
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB, get_available_clusters
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.job_series import JobSeriesDB
from sarc.models.job import SlurmState

router = APIRouter(prefix="/dash", dependencies=[Depends(require_basic_auth)])


def session_dep() -> Generator[Session]:
    with config().db.session() as sess:
        # Disable parallel query for dashboard requests: parallel workers
        # allocate dynamic shared-memory segments in /dev/shm, which is tiny in
        # a default container (~64 MB) and overflows on big joins/aggregations
        # ("could not resize shared memory segment"). No measurable speedup here.
        # SET LOCAL (not SET) scopes it to this request's transaction, so it
        # never leaks to later sessions reusing the pooled connection.
        sess.connection().execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
        yield sess


UTC = timezone.utc

_DEFAULT_WINDOW_DAYS = 1
_DEFAULT_PERIOD = "w"

# Hides the raw scatter plots (elapsed-vs-limit, wait) from the dashboard UI
# and skips the /metrics/scatter HTTP call entirely. The endpoint itself stays
# available for direct queries. Re-enable only on small windows: 1 year of
# completed jobs is enough to lock up the browser.
_ALLOW_SCATTER: bool = False


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


_PERIOD_RE = re.compile(r"^(\d+(?:\.\d+)?)?\s*([hdwm]?)$", re.IGNORECASE)
_PERIOD_MULTIPLIERS = {"h": 1 / 24, "d": 1, "w": 7, "m": 30}
# Single-letter period -> PostgreSQL date_trunc field, for calendar bucketing.
_CALENDAR_TRUNC = {"h": "hour", "d": "day", "w": "week", "m": "month"}
_BUCKET_KEYFMT = "%Y-%m-%d %H:%M:%S"


def _parse_period(s: str) -> timedelta | str:
    """Parse a period into a fixed step or a calendar unit.

    - ``N`` / ``N<unit>`` (e.g. ``5``, ``2w``, ``1m``): fixed window -> timedelta
      (``1m`` = 30 days, unchanged). Buckets step uniformly from ``begin``.
    - ``<unit>`` alone (``h``/``d``/``w``/``m``): calendar window -> the
      ``date_trunc`` field name. Buckets follow calendar boundaries (week =
      Monday, month = 1st), clipped to the requested range.
    """
    m = _PERIOD_RE.match(s.strip())
    if not m or not (m.group(1) or m.group(2)):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid period {s!r}. Use N[h/d/w/m] for a fixed window "
                f"(e.g. 12h, 1d, 2w, 1m) or h/d/w/m alone for calendar buckets."
            ),
        )
    num, unit = m.group(1), (m.group(2) or "d").lower()
    if num is None:
        return _CALENDAR_TRUNC[unit]
    return timedelta(days=float(num) * _PERIOD_MULTIPLIERS[unit])


def _label_fmt(period: timedelta | str) -> str:
    sub_daily = period == "hour" or (
        isinstance(period, timedelta) and period < timedelta(days=1)
    )
    return "%Y-%m-%d %H:%M" if sub_daily else "%Y-%m-%d"


def _calendar_trunc(dt: datetime, field: str) -> datetime:
    """Floor dt to a calendar boundary, mirroring PostgreSQL date_trunc."""
    if field == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if field == "week":
        dt -= timedelta(days=dt.weekday())  # back to Monday
    elif field == "month":
        dt = dt.replace(day=1)
    return dt


def _calendar_next(dt: datetime, field: str) -> datetime:
    """Next calendar boundary after a truncated dt."""
    if field == "hour":
        return dt + timedelta(hours=1)
    if field == "day":
        return dt + timedelta(days=1)
    if field == "week":
        return dt + timedelta(weeks=1)
    return (
        dt.replace(year=dt.year + 1, month=1)
        if dt.month == 12
        else dt.replace(month=dt.month + 1)
    )


def _bucket_expr(period: timedelta | str, time_col, begin_dt: datetime):
    """SQL bucket expression: floor((t - begin)/step) for a fixed period,
    date_trunc(unit, t) for a calendar period."""
    if isinstance(period, timedelta):
        return func.floor(
            func.extract("epoch", time_col - begin_dt) / period.total_seconds()
        ).label("bucket")
    return func.date_trunc(period, time_col).label("bucket")


def _sql_bucket_key(period: timedelta | str, raw):
    """Normalise a SQL bucket value into a key matching _iter_buckets()."""
    return int(raw) if isinstance(period, timedelta) else raw.strftime(_BUCKET_KEYFMT)


def _iter_buckets(begin_dt: datetime, finish_dt: datetime, period: timedelta | str):
    """Yield (key, period_start, period_end) for every bucket in [begin, finish),
    with period_start/end clipped to the range. key matches _sql_bucket_key()."""
    if isinstance(period, timedelta):
        cur, idx = begin_dt, 0
        while cur < finish_dt:
            nxt = cur + period
            yield idx, cur, min(nxt, finish_dt)
            cur, idx = nxt, idx + 1
    else:
        frontier = _calendar_trunc(begin_dt, period)
        while frontier < finish_dt:
            nxt = _calendar_next(frontier, period)
            yield (
                frontier.strftime(_BUCKET_KEYFMT),
                max(frontier, begin_dt),
                min(nxt, finish_dt),
            )
            frontier = nxt


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


def _apply_job_states(query, state_col, job_states: list[str]):
    """Filter by the given Slurm states; an empty list means no filter (all)."""
    if job_states:
        query = query.where(col(state_col).in_(job_states))
    return query


def _apply_common_filters(
    query, clusters: list[str], cluster_user: str | None, job_states: list[str]
):
    """Apply job-state + cluster/user filters to a JobSeriesDB query.

    An empty ``clusters`` list means no cluster filter (all clusters).
    """
    query = _apply_job_states(query, JobSeriesDB.job_state, job_states)
    if clusters:
        query = query.where(col(JobSeriesDB.cluster_name).in_(clusters))
    if cluster_user:
        query = query.where(JobSeriesDB.cluster_user == cluster_user)
    return query


def _rgu_col(rgu_type: str):
    """RGU column to aggregate across the dashboard's RGU plots.

    ``physical`` uses the GPUs actually allocated (``physical_rgu``); any other
    value falls back to the billing-normalized RGU (``rgu``), which runs ~1.5x
    higher than the physical count for Mila.
    """
    return col(JobSeriesDB.physical_rgu if rgu_type == "physical" else JobSeriesDB.rgu)


def _resolve_cluster_ids(sess: Session, clusters: list[str]) -> list[int] | None:
    """Look up cluster ids; returns None if the cluster filter is unset (empty).

    Raises 404 on the first unknown cluster name.
    """
    if not clusters:
        return None
    ids: list[int] = []
    for name in clusters:
        cid = SlurmClusterDB.id_by_name(sess, name)
        if cid is None:
            raise HTTPException(status_code=404, detail=f"Unknown cluster {name!r}")
        ids.append(cid)
    return ids


def _apply_slurm_job_filters(
    query,
    cluster_ids: list[int] | None,
    cluster_user: str | None,
    job_states: list[str],
):
    """Apply job-state + cluster_id/user filters to a SlurmJobDB query.

    Filters by cluster_ids (resolved upfront) to avoid the SlurmClusterDB join
    that JobSeriesDB needs for cluster_name. None/empty means no cluster filter.
    """
    query = _apply_job_states(query, SlurmJobDB.job_state, job_states)
    if cluster_ids:
        query = query.where(col(SlurmJobDB.cluster_id).in_(cluster_ids))
    if cluster_user:
        query = query.where(SlurmJobDB.cluster_user == cluster_user)
    return query


@router.get("/metrics", response_class=HTMLResponse)
def metrics_global_page():
    return _HTML


@router.get("/clusters")
def metrics_clusters(sess: Session = Depends(session_dep)) -> list[str]:
    return sorted(c.name for c in get_available_clusters(sess))


@router.get("/job_states")
def metrics_job_states() -> list[str]:
    """All Slurm job states the dashboard can filter on."""
    return [s.value for s in SlurmState]


@router.get("/metrics/data")
def metrics_global_data(
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)
    cluster_ids = _resolve_cluster_ids(sess, clusters)

    # Bucket each job in SQL (floor for fixed periods, date_trunc for calendar)
    # and group, instead of one COUNT per bucket. Queries SlurmJobDB directly to
    # skip the JobSeriesDB view (RGU/statistics aggregations are not needed).
    bucket_expr = _bucket_expr(parsed, SlurmJobDB.submit_time, begin_dt)

    query = select(bucket_expr, func.count().label("count")).where(
        col(SlurmJobDB.submit_time) >= begin_dt, col(SlurmJobDB.submit_time) < finish_dt
    )
    query = _apply_slurm_job_filters(query, cluster_ids, cluster_user, job_states)
    # group_by by label name: pg8000 binds parameters server-side, so repeating
    # the expression would yield distinct $N placeholders in SELECT vs GROUP BY
    # and PostgreSQL could not match them (42803). Same pattern below.
    query = query.group_by("bucket").order_by("bucket")

    counts = {
        _sql_bucket_key(parsed, row.bucket): int(row.count) for row in sess.exec(query)
    }

    return [
        {
            "period_start": ps.strftime(fmt),
            "period_end": pe.strftime(fmt),
            "count": counts.get(key, 0),
        }
        for key, ps, pe in _iter_buckets(begin_dt, finish_dt, parsed)
    ]


@router.get("/metrics/scatter")
def metrics_global_scatter(
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    cluster_ids = _resolve_cluster_ids(sess, clusters)

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
    query = _apply_slurm_job_filters(query, cluster_ids, cluster_user, job_states)

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
    sess: Session, base_filters: list, x_expr, y_expr, x_max: float, y_max: float
):
    """Aggregate count(*) per (bin_x, bin_y) over NBINS×NBINS log-spaced bins.

    Bins are uniform in log10(value+1) space so highly-skewed distributions
    (durations spanning many orders of magnitude) get even resolution rather
    than collapsing into the first linear bin. No data is dropped: every job
    is counted in exactly one cell. The min is fixed at 0 and the +1 offset
    avoids log10(0).
    """
    log_x_max = max(math.log10(x_max + 1.0), 1e-9)
    log_y_max = max(math.log10(y_max + 1.0), 1e-9)

    # PostgreSQL: log(numeric) with one arg is base-10.
    log_x = func.log(x_expr + 1.0)
    log_y = func.log(y_expr + 1.0)
    bin_x = func.least(
        func.greatest(func.floor(log_x * _HEATMAP_BINS / log_x_max), 0),
        _HEATMAP_BINS - 1,
    ).label("bx")
    bin_y = func.least(
        func.greatest(func.floor(log_y * _HEATMAP_BINS / log_y_max), 0),
        _HEATMAP_BINS - 1,
    ).label("by")
    q = (
        select(bin_x, bin_y, func.count().label("c"))
        .where(*base_filters)
        .group_by("bx", "by")
    )
    z = [[0] * _HEATMAP_BINS for _ in range(_HEATMAP_BINS)]
    total = 0
    for r in sess.exec(q):
        c = int(r.c)
        z[int(r.by)][int(r.bx)] = c
        total += c

    # Bin centres in log space then converted back to linear value (seconds).
    log_x_step = log_x_max / _HEATMAP_BINS
    log_y_step = log_y_max / _HEATMAP_BINS
    xs = [10 ** ((i + 0.5) * log_x_step) - 1.0 for i in range(_HEATMAP_BINS)]
    ys = [10 ** ((i + 0.5) * log_y_step) - 1.0 for i in range(_HEATMAP_BINS)]
    return {"x": xs, "y": ys, "z": z, "total": total}


@router.get("/metrics/heatmap")
def metrics_global_heatmap(
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    cluster_ids = _resolve_cluster_ids(sess, clusters)

    wait_expr = func.extract("epoch", SlurmJobDB.start_time - SlurmJobDB.submit_time)
    base_filters = [
        col(SlurmJobDB.submit_time) >= begin_dt,
        col(SlurmJobDB.submit_time) < finish_dt,
        col(SlurmJobDB.time_limit).is_not(None),
        col(SlurmJobDB.start_time).is_not(None),
    ]
    if cluster_ids:
        base_filters.append(col(SlurmJobDB.cluster_id).in_(cluster_ids))
    if cluster_user:
        base_filters.append(col(SlurmJobDB.cluster_user) == cluster_user)
    if job_states:
        base_filters.append(col(SlurmJobDB.job_state).in_(job_states))

    bounds = sess.exec(
        select(
            func.max(SlurmJobDB.time_limit).label("max_l"),
            func.max(SlurmJobDB.elapsed_time).label("max_e"),
            func.max(wait_expr).label("max_w"),
        ).where(*base_filters)
    ).one()

    if bounds.max_l is None:
        # No matching rows
        return {"elapsed_vs_limit": None, "wait_vs_limit": None, "total_jobs": 0}

    elapsed_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        SlurmJobDB.time_limit,
        SlurmJobDB.elapsed_time,
        float(bounds.max_l),
        float(bounds.max_e),
    )
    wait_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        SlurmJobDB.time_limit,
        wait_expr,
        float(bounds.max_l),
        float(bounds.max_w),
    )

    return {
        "elapsed_vs_limit": elapsed_hmap,
        "wait_vs_limit": wait_hmap,
        "total_jobs": int(elapsed_hmap["total"]),
    }


_DENSITY_BINS = 50  # matches Plotly nbinsx in the frontend
# Paired-heatmap resolution: 2x the density bins = 100, the same finesse as
# the elapsed/wait heatmaps (_HEATMAP_BINS). Kept as an exact multiple so the
# density marginals fold out of the 2D pass by pairwise bin summation.
_PAIRED_BINS = 2 * _DENSITY_BINS


def _density_bin_expr(metric_expr, nbins: int = _DENSITY_BINS):
    """SQL expression for floor(metric_expr * nbins), clipped to [0, nbins-1]."""
    return func.least(func.greatest(func.floor(metric_expr * nbins), 0), nbins - 1)


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
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    metric2: str | None = Query(default=None),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    rgu_type: str = Query(default="physical"),
    sess: Session = Depends(session_dep),
):
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")
    if metric2 is not None and metric2 not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric2!r}")

    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    # Read each metric mean via a targeted LEFT JOIN on jobstatisticdb (one
    # aliased row per metric) instead of JobSeriesDB.statistics, which forces a
    # per-row json_object_agg of all stats. See histogram for the rationale.
    js1 = aliased(JobStatisticDB)
    m1 = col(js1.mean)
    rgu_col = _rgu_col(rgu_type)
    weight = rgu_col * JobSeriesDB.elapsed_time
    bin_width = 1.0 / _DENSITY_BINS

    # Common job-population filter shared by primary, secondary and paired.
    # When metric2 is specified, both metric and metric2 must be valid (matches
    # original Python loop: secondary failure skips the primary too).
    base_filters = [
        col(JobSeriesDB.submit_time) >= begin_dt,
        col(JobSeriesDB.submit_time) < finish_dt,
        col(JobSeriesDB.allocated_gpu_type).is_not(None),
        rgu_col.is_not(None),
        _valid_metric_filter(m1),
    ]
    if metric2:
        js2 = aliased(JobStatisticDB)
        m2 = col(js2.mean)
        base_filters.append(_valid_metric_filter(m2))
    else:
        js2 = None
        m2 = None

    def _add_metric_joins(q):
        q = q.select_from(JobSeriesDB).join(
            js1,
            and_(
                col(js1.job_id) == col(JobSeriesDB.job_db_id), col(js1.name) == metric
            ),
            isouter=True,
        )
        if js2 is not None:
            q = q.join(
                js2,
                and_(
                    col(js2.job_id) == col(JobSeriesDB.job_db_id),
                    col(js2.name) == metric2,
                ),
                isouter=True,
            )
        return q

    def _binned_query(metric_expr):
        bin_expr = _density_bin_expr(metric_expr).label("bin")
        q = _add_metric_joins(select(bin_expr, func.sum(weight).label("w")))
        q = q.where(*base_filters).group_by("bin").order_by("bin")
        return _apply_common_filters(q, clusters, cluster_user, job_states)

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

    if not metric2 or m2 is None:
        p_values, p_weights = _bin_to_payload(sess.exec(_binned_query(m1)))
        return {
            "primary": {"values": p_values, "weights": p_weights},
            "secondary": None,
            "paired": None,
        }

    # One 2D-binned pass serves all three payloads: primary is the per-column
    # weight sum, secondary the per-row one, and paired the 100x100 cell-count
    # matrix rendered as a heatmap (no sampling: like the elapsed/wait
    # heatmaps, every job lands in exactly one cell). Replaces two 1D scans
    # plus an ORDER BY random() LIMIT 5000 over the full view (a 1% sample
    # that still paid every view join per row) — ~22s -> ~4s on a 5-month
    # window, and the heatmap is exact.
    bx = _density_bin_expr(m1, _PAIRED_BINS).label("bx")
    by = _density_bin_expr(m2, _PAIRED_BINS).label("by")
    # group_by by label, not by expression: pg8000's server-side binding
    # renders the expression with fresh placeholders in GROUP BY (error 42803).
    q2d = _add_metric_joins(
        select(bx, by, func.sum(weight).label("w"), func.count().label("n"))
    )
    q2d = q2d.where(*base_filters).group_by("bx", "by")
    q2d = _apply_common_filters(q2d, clusters, cluster_user, job_states)

    z = [[0] * _PAIRED_BINS for _ in range(_PAIRED_BINS)]  # z[by][bx] (Plotly)
    p_w = [0.0] * _PAIRED_BINS
    s_w = [0.0] * _PAIRED_BINS
    p_seen = [False] * _PAIRED_BINS
    s_seen = [False] * _PAIRED_BINS
    for r in sess.exec(q2d):
        ibx, iby = int(r.bx), int(r.by)
        w = float(r.w or 0.0)
        z[iby][ibx] += int(r.n)
        p_w[ibx] += w
        s_w[iby] += w
        p_seen[ibx] = True
        s_seen[iby] = True

    def _marginal(weights, seens):
        """Fold the 100-bin accumulators down to the 50 density bins.

        floor(m*100)//2 == floor(m*50) (clipping included), so this is
        bit-identical to a direct 1D scan at _DENSITY_BINS resolution.
        """
        ratio = _PAIRED_BINS // _DENSITY_BINS
        out_v, out_w = [], []
        for i in range(_DENSITY_BINS):
            cells = range(i * ratio, (i + 1) * ratio)
            if any(seens[j] for j in cells):
                out_v.append((i + 0.5) * bin_width)
                out_w.append(sum(weights[j] for j in cells))
        return out_v, out_w

    p_values, p_weights = _marginal(p_w, p_seen)
    s_values, s_weights = _marginal(s_w, s_seen)

    centres = [(i + 0.5) / _PAIRED_BINS for i in range(_PAIRED_BINS)]
    return {
        "primary": {"values": p_values, "weights": p_weights},
        "secondary": {"values": s_values, "weights": s_weights},
        "paired": {"x": centres, "y": centres, "z": z},
    }


@router.get("/metrics/histogram")
def metrics_global_histogram(
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    rgu_type: str = Query(default="physical"),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)

    # Aggregate per bucket directly in SQL: SUM(rgu * elapsed / 3600) for
    # requested, and the same multiplied by the metric mean for used. The
    # `m == m` test filters NaN (NaN != NaN), substituting 0 in that branch.
    bucket_expr = _bucket_expr(parsed, JobSeriesDB.submit_time, begin_dt)
    rgu_col = _rgu_col(rgu_type)
    rgu_hours = rgu_col * JobSeriesDB.elapsed_time / 3600.0
    # Read the metric mean via a targeted LEFT JOIN on jobstatisticdb rather than
    # JobSeriesDB.statistics: the latter is a per-row json_object_agg of *all*
    # stats and dominates the query (40+ min on a year of jobs). The join hits
    # the jobstatisticdb(job_id) index, filtered to the single metric we need;
    # statistics then goes unreferenced and Postgres prunes its subquery.
    m_mean = col(JobStatisticDB.mean)
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
        .join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(JobSeriesDB.job_db_id),
                col(JobStatisticDB.name) == metric,
            ),
            isouter=True,
        )
        .where(
            col(JobSeriesDB.submit_time) >= begin_dt,
            col(JobSeriesDB.submit_time) < finish_dt,
            col(JobSeriesDB.allocated_gpu_type).is_not(None),
            rgu_col.is_not(None),
        )
        .group_by("bucket")
        .order_by("bucket")
    )
    query = _apply_common_filters(query, clusters, cluster_user, job_states)

    sums = {
        _sql_bucket_key(parsed, row.bucket): (
            float(row.rgu_requested or 0.0),
            float(row.rgu_used or 0.0),
        )
        for row in sess.exec(query)
    }

    period_data = []
    for key, ps, pe in _iter_buckets(begin_dt, finish_dt, parsed):
        req, used = sums.get(key, (0.0, 0.0))
        period_data.append(
            {
                "period_start": ps.strftime(fmt),
                "period_end": pe.strftime(fmt),
                "rgu_requested": req,
                "rgu_used": used,
            }
        )

    return period_data


@router.get("/metrics/rgu_by_cluster")
def metrics_rgu_by_cluster(
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    rgu_type: str = Query(default="physical"),
    sess: Session = Depends(session_dep),
):
    """Total RGU.h per period, stacked by cluster.

    Aggregates the same RGU metric as /histogram (SUM(rgu * elapsed / 3600))
    and groups by cluster_name. When ``clusters`` is given, only those clusters
    are kept (empty = all clusters). Returns one series per cluster, aligned on
    a shared period axis; clusters with no RGU at all (e.g. no billing) are
    dropped.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)

    bucket_expr = _bucket_expr(parsed, JobSeriesDB.submit_time, begin_dt)
    rgu_col = _rgu_col(rgu_type)
    rgu_hours = rgu_col * col(JobSeriesDB.elapsed_time) / 3600.0
    query = (
        select(bucket_expr, JobSeriesDB.cluster_name, func.sum(rgu_hours).label("rgu"))
        .where(
            col(JobSeriesDB.submit_time) >= begin_dt,
            col(JobSeriesDB.submit_time) < finish_dt,
            col(JobSeriesDB.allocated_gpu_type).is_not(None),
            rgu_col.is_not(None),
        )
        .group_by("bucket", col(JobSeriesDB.cluster_name))
        .order_by("bucket")
    )
    query = _apply_common_filters(query, clusters, cluster_user, job_states)

    sums = {}
    totals = {}
    for r in sess.exec(query):
        if not r.cluster_name:
            continue
        v = float(r.rgu or 0.0)
        sums[(_sql_bucket_key(parsed, r.bucket), r.cluster_name)] = v
        totals[r.cluster_name] = totals.get(r.cluster_name, 0.0) + v

    # Largest total first -> drawn at the bottom of the stack (Plotly stacks the
    # first trace at the base). Ties broken by name for a stable order.
    stacked_clusters = sorted(
        (c for c, t in totals.items() if t > 0), key=lambda c: (-totals[c], c)
    )
    buckets = list(_iter_buckets(begin_dt, finish_dt, parsed))

    return {
        "periods": [
            {"period_start": ps.strftime(fmt), "period_end": pe.strftime(fmt)}
            for _, ps, pe in buckets
        ],
        "series": [
            {"cluster": c, "rgu": [sums.get((k, c), 0.0) for k, _, _ in buckets]}
            for c in stacked_clusters
        ],
    }


@router.get("/metrics/metric_trend")
def metrics_metric_trend(
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    metric2: str | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Per-period averages of a metric's per-job ``mean`` and ``max``.

    For each period bucket, averages the per-job statistic values over the
    jobs submitted in that bucket (plain per-job average, not duration
    weighted). Jobs lacking the statistic are simply absent from the average
    (inner join) and no GPU/RGU filter is applied, so system metrics also
    cover CPU-only jobs. Returns one series per requested metric, aligned on
    a shared period axis; buckets with no data yield null (a curve gap), not 0.
    """
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")
    if metric2 is not None and metric2 not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric2!r}")
    wanted = [metric] + ([metric2] if metric2 and metric2 != metric else [])

    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)

    bucket_expr = _bucket_expr(parsed, JobSeriesDB.submit_time, begin_dt)
    m_mean = col(JobStatisticDB.mean)
    m_max = col(JobStatisticDB.max)
    # NaN-proof averages: NaN != NaN, and a single NaN would contaminate the
    # whole AVG, so each value is independently nulled out when NaN.
    avg_mean = func.avg(case((m_mean == m_mean, m_mean))).label("avg_mean")  # noqa: PLR0124
    avg_max = func.avg(case((m_max == m_max, m_max))).label("avg_max")  # noqa: PLR0124

    query = (
        select(
            bucket_expr,
            col(JobStatisticDB.name).label("metric_name"),
            avg_mean,
            avg_max,
        )
        .select_from(JobSeriesDB)
        .join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(JobSeriesDB.job_db_id),
                col(JobStatisticDB.name).in_(wanted),
            ),
        )
        .where(
            col(JobSeriesDB.submit_time) >= begin_dt,
            col(JobSeriesDB.submit_time) < finish_dt,
        )
        .group_by("bucket", col(JobStatisticDB.name))
        .order_by("bucket")
    )
    query = _apply_common_filters(query, clusters, cluster_user, job_states)

    cells = {}
    for r in sess.exec(query):
        key = _sql_bucket_key(parsed, r.bucket)
        cells[(key, r.metric_name)] = (
            _nan_to_none(r.avg_mean),
            _nan_to_none(r.avg_max),
        )

    buckets = list(_iter_buckets(begin_dt, finish_dt, parsed))
    return {
        "periods": [
            {"period_start": ps.strftime(fmt), "period_end": pe.strftime(fmt)}
            for _, ps, pe in buckets
        ],
        "series": [
            {
                "metric": m,
                "mean": [cells.get((k, m), (None, None))[0] for k, _, _ in buckets],
                "max": [cells.get((k, m), (None, None))[1] for k, _, _ in buckets],
            }
            for m in wanted
        ],
    }


@router.get("/metrics/user_rgu")
def metrics_global_user_rgu(
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    rgu_type: str = Query(default="physical"),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    # Aggregate by user directly in SQL: SUM(rgu * elapsed / 3600) per user.
    rgu_col = _rgu_col(rgu_type)
    rgu_hours = rgu_col * JobSeriesDB.elapsed_time / 3600.0
    # Targeted LEFT JOIN on jobstatisticdb instead of JobSeriesDB.statistics
    # (per-row json_object_agg). See histogram for the rationale.
    m_mean = col(JobStatisticDB.mean)
    rgu_used_term = case(
        (m_mean == m_mean, rgu_hours * m_mean),  # noqa: PLR0124
        else_=0.0,
    )
    user_expr = func.coalesce(JobSeriesDB.cluster_user, "unknown").label("user")
    rgu_requested_sum = func.sum(rgu_hours).label("rgu_requested")

    query = (
        select(user_expr, rgu_requested_sum, func.sum(rgu_used_term).label("rgu_used"))
        .join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(JobSeriesDB.job_db_id),
                col(JobStatisticDB.name) == metric,
            ),
            isouter=True,
        )
        .where(
            col(JobSeriesDB.submit_time) >= begin_dt,
            col(JobSeriesDB.submit_time) < finish_dt,
            col(JobSeriesDB.allocated_gpu_type).is_not(None),
            rgu_col.is_not(None),
        )
        .group_by("user")
        .order_by(rgu_requested_sum.desc(), user_expr)
    )
    query = _apply_common_filters(query, clusters, cluster_user, job_states)

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
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    limit: int = Query(default=50, gt=0, le=500),
    metric: str = Query(default="gpu_sm_occupancy"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    rgu_type: str = Query(default="physical"),
    sess: Session = Depends(session_dep),
):
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    # Sort key (rgu * elapsed) and the stat extractions are pushed into SQL so
    # we only materialise `limit` rows, not the full result set.
    rgu_col = _rgu_col(rgu_type)
    rgu_hours = (rgu_col * JobSeriesDB.elapsed_time / 3600.0).label("rgu_hours")
    # Targeted LEFT JOINs on jobstatisticdb (one aliased row per distinct stat
    # name) instead of JobSeriesDB.statistics, which forces a per-row
    # json_object_agg of all stats. See histogram for the rationale.
    stat_names = {metric, "gpu_utilization", "gpu_sm_occupancy", "gpu_memory"}
    js = {name: aliased(JobStatisticDB) for name in sorted(stat_names)}
    metric_mean = col(js[metric].mean).label("metric_mean")
    gpu_util_mean = col(js["gpu_utilization"].mean).label("gpu_utilization_mean")
    gpu_sm_mean = col(js["gpu_sm_occupancy"].mean).label("gpu_sm_occupancy_mean")
    gpu_mem_max = col(js["gpu_memory"].max).label("gpu_memory_max")

    # count(*) OVER () = total matching jobs before LIMIT kicks in; ~free
    # since the ORDER BY already walks the full filtered set.
    total_col = func.count().over().label("total")

    query = select(  # ty:ignore[no-matching-overload]
        JobSeriesDB.cluster_name,
        JobSeriesDB.cluster_user,
        JobSeriesDB.job_state,
        JobSeriesDB.elapsed_time,
        JobSeriesDB.nodes,
        JobSeriesDB.allocated_gpu_type,
        JobSeriesDB.harmonized_gpu_type,
        rgu_col.label("rgu"),
        rgu_hours,
        metric_mean,
        gpu_util_mean,
        gpu_sm_mean,
        gpu_mem_max,
        total_col,
    ).select_from(JobSeriesDB)
    for name, alias in js.items():
        query = query.join(
            alias,
            and_(
                col(alias.job_id) == col(JobSeriesDB.job_db_id), col(alias.name) == name
            ),
            isouter=True,
        )
    query = (
        query.where(
            col(JobSeriesDB.submit_time) >= begin_dt,
            col(JobSeriesDB.submit_time) < finish_dt,
            col(JobSeriesDB.allocated_gpu_type).is_not(None),
            rgu_col.is_not(None),
            rgu_col == rgu_col,  # NaN guard   # noqa: PLR0124
        )
        .order_by(rgu_hours.desc(), col(JobSeriesDB.cluster_user))
        .limit(limit)
    )
    query = _apply_common_filters(query, clusters, cluster_user, job_states)

    jobs = []
    total = 0
    for row in sess.exec(query):
        total = int(row.total)
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
                # Harmonised name (the one RGU is computed from) when known;
                # raw Slurm name otherwise.
                "gpu_type": row.harmonized_gpu_type or row.allocated_gpu_type or "",
                "rgu": round(float(row.rgu), 2),
                "rgu_hours": round(rh, 2),
                "waste": waste,
                "gpu_utilization_mean": _nan_to_none(row.gpu_utilization_mean),
                "gpu_sm_occupancy_mean": _nan_to_none(row.gpu_sm_occupancy_mean),
                "gpu_memory_max": _nan_to_none(row.gpu_memory_max),
            }
        )

    return {"total": total, "jobs": jobs}


_html_path = Path(__file__).parent / "metrics.html"

_HTML = (
    _html_path.read_text(encoding="utf-8")
    .replace("__DEFAULT_PERIOD__", _DEFAULT_PERIOD)
    .replace("__ALLOW_SCATTER__", "true" if _ALLOW_SCATTER else "false")
)
