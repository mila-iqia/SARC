import hashlib
import math
import re
from collections.abc import Generator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import literal_column, nulls_last
from sqlalchemy.orm import aliased
from sqlmodel import Session, and_, case, col, func, select

from sarc.api.v0 import Requestor, requestor
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, SlurmJobDB
from sarc.db.support import GpuRguDB
from sarc.models.job import SlurmState


def _scope(req: Requestor) -> int | Literal["admin"]:
    """Non-admin → their own UserDB id, used to restrict every /dash query to
    their jobs; admin (or auth off, where requestor yields admin) → the sentinel
    ``"admin"``, no scoping. A string rather than None on purpose: a forgotten
    return then yields None, a type error caught by the checker, instead of
    silently masquerading as admin and granting full scope. Passed explicitly to
    the filter helpers, mirroring how /v0 uses the requestor — no implicit/global
    state. ``req.user`` is non-None for a non-admin (requestor raises 403
    otherwise)."""
    if req.is_admin:
        return "admin"
    # requestor guarantees a non-admin has a DB-loaded user (403 otherwise), and a
    # persisted UserDB always has an int id (Optional only before insert).
    assert req.user is not None and req.user.id is not None
    return req.user.id


router = APIRouter(prefix="/dash", dependencies=[Depends(requestor)])


def session_dep() -> Generator[Session]:
    with config.db.session() as sess:
        yield sess


UTC = timezone.utc

_DEFAULT_WINDOW_DAYS = 1
_DEFAULT_PERIOD = "w"

# GPU/system metrics (stored per-job in JobStatisticDB) normalised to [0, 1]
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
    date_trunc(unit, t) for a calendar period.

    Calendar buckets truncate ``timezone('UTC', t)`` (i.e. ``t AT TIME ZONE
    'UTC'``), not ``t`` directly: ``date_trunc`` on a timestamptz floors in the
    session's TimeZone, and the IAM/Connector engine used in prod does not pin
    timezone=utc the way the direct-connection engine does. Truncating the UTC
    wall-clock keeps buckets aligned with the UTC begin/finish bounds on any
    connection. Portable form, unlike date_trunc's PG16+ 3-arg variant."""
    if isinstance(period, timedelta):
        return func.floor(
            func.extract("epoch", time_col - begin_dt) / period.total_seconds()
        ).label("bucket")
    return func.date_trunc(period, func.timezone("UTC", time_col)).label("bucket")


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


def _date_range(start, end) -> tuple[datetime, datetime]:
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
    scope_user_id: int | Literal["admin"],
):
    """Apply job-state + cluster_id/user filters to a SlurmJobDB query.

    Filters by cluster_ids (resolved upfront) to avoid joining SlurmClusterDB
    just to filter on cluster_name. None/empty means no cluster filter.
    ``scope_user_id`` (a non-admin's UserDB id) restricts to that user's jobs;
    the sentinel ``"admin"`` applies no user scoping.
    """
    if job_states:
        query = query.where(col(SlurmJobDB.job_state).in_(job_states))
    if cluster_ids:
        query = query.where(col(SlurmJobDB.cluster_id).in_(cluster_ids))
    if cluster_user:
        query = query.where(SlurmJobDB.cluster_user == cluster_user)
    if scope_user_id != "admin":
        query = query.where(SlurmJobDB.sarc_user_id == scope_user_id)
    return query


# Physical RGU per job: coalesce(allocated_gres_gpu, 0) * drac_rgu. Only valid on
# a query whose FROM was built by _apply_rgu_base (it left-joins gpurgudb on
# harmonized_gpu_type). A missing gpurgudb row makes it NULL, which
# _apply_rgu_base drops via its `_RGU_DRAC IS NOT NULL` filter.
_RGU_DRAC = func.coalesce(SlurmJobDB.allocated_gres_gpu, 0) * GpuRguDB.drac_rgu


def _apply_rgu_base(
    query,
    sess: Session,
    clusters: list[str],
    cluster_user: str | None,
    job_states: list[str],
    *,
    scope_user_id: int | Literal["admin"],
):
    """Build the shared base of every RGU query.

    Anchors ``query`` on SlurmJobDB, left-joins gpurgudb on harmonized_gpu_type
    (so ``_RGU_DRAC`` and ``col(GpuRguDB.drac_rgu)`` resolve), keeps only the GPU
    jobs whose physical RGU is calculable (a known gpu_type with a matching
    gpurgudb row), then applies the common cluster/user/state filters. ``query``
    must already project its columns; the caller still adds its own time window.
    """
    query = (
        query.select_from(SlurmJobDB)
        .join(
            GpuRguDB,
            col(GpuRguDB.name) == col(SlurmJobDB.harmonized_gpu_type),
            isouter=True,
        )
        .where(col(SlurmJobDB.allocated_gpu_type).is_not(None), _RGU_DRAC.is_not(None))
    )
    return _apply_slurm_job_filters(
        query,
        _resolve_cluster_ids(sess, clusters),
        cluster_user,
        job_states,
        scope_user_id,
    )


_TEMPLATES = Jinja2Templates(directory=Path(__file__).parent)


@router.get("/metrics", response_class=HTMLResponse)
def metrics_homepage(request: Request, req: Requestor = Depends(requestor)):
    """Serve the dashboard's single-page HTML UI; its charts call the JSON
    endpoints below. Rendered with Jinja2: ``is_admin`` adapts the page
    per-request (hide the user filter / RGU-by-user for a non-admin) with no
    round-trip — the backend scopes the data regardless — and the connected
    email is shown in the title/header, coloured by role (admin red, user grey).
    Jinja auto-escapes ``user_email`` in HTML; ``| tojson`` makes the
    booleans/lists safe to inline in <script>.

    ``storage_key`` namespaces the per-user localStorage state. We hash the
    identity rather than embed the email in clear as a key, and fold in the role
    so a promote/demote (or the force_user toggle used in local testing) starts
    fresh instead of reloading selections that reference now-absent controls.
    It's a namespacing key, not a secret, so a short digest is enough."""
    storage_key = (
        "sarc_dash_v1_"
        + hashlib.sha256(f"{req.email}|{req.is_admin}".encode()).hexdigest()[:16]
    )
    return _TEMPLATES.TemplateResponse(
        request,
        "metrics.html",
        {
            "is_admin": req.is_admin,
            "user_email": req.email,
            "default_period": _DEFAULT_PERIOD,
            "job_states": [s.value for s in SlurmState],
            "storage_key": storage_key,
        },
    )


@router.get("/metrics/job_counts")
def metrics_job_counts(
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    sess: Session = Depends(session_dep),
):
    """Job count per time bucket.

    Counts jobs whose submit_time falls in each ``period`` bucket of the window,
    after the cluster/user/state filters. Returns one {period_start, period_end,
    count} per bucket, with empty buckets reported as 0.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)

    # Bucket each job in SQL (floor for fixed periods, date_trunc for calendar)
    # and group, instead of one COUNT per bucket. Queries SlurmJobDB directly to
    # skip the JobSeriesDB view (RGU/statistics aggregations are not needed).
    bucket_expr = _bucket_expr(parsed, SlurmJobDB.submit_time, begin_dt)

    query = select(bucket_expr, func.count().label("count")).where(
        col(SlurmJobDB.submit_time) >= begin_dt, col(SlurmJobDB.submit_time) < finish_dt
    )
    query = _apply_slurm_job_filters(
        query,
        _resolve_cluster_ids(sess, clusters),
        cluster_user,
        job_states,
        _scope(req),
    )
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
    for bx, by, count in sess.exec(q):
        c = int(count)
        z[int(by)][int(bx)] = c
        total += c

    # Bin centres in log space then converted back to linear value (seconds).
    log_x_step = log_x_max / _HEATMAP_BINS
    log_y_step = log_y_max / _HEATMAP_BINS
    xs = [10 ** ((i + 0.5) * log_x_step) - 1.0 for i in range(_HEATMAP_BINS)]
    ys = [10 ** ((i + 0.5) * log_y_step) - 1.0 for i in range(_HEATMAP_BINS)]
    return {"x": xs, "y": ys, "z": z, "total": total}


@router.get("/metrics/job_times_vs_limit")
def metrics_job_times_vs_limit(
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Two job-count heatmaps relating each job's runtime to its requested time limit.

    Over jobs submitted in the window that have a time_limit and have started:
    ``elapsed_vs_limit`` plots elapsed_time (y) against time_limit (x), and
    ``wait_vs_limit`` plots the queue wait, start - submit (y), against time_limit
    (x). Each is a 100x100 log-binned grid of job counts. Returns both grids plus
    total_jobs. ``focus_start/end`` narrows the window.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    cluster_ids = _resolve_cluster_ids(sess, clusters)

    wait_expr = func.extract(
        "epoch", col(SlurmJobDB.start_time) - col(SlurmJobDB.submit_time)
    )
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
    scope_user_id = _scope(req)
    if scope_user_id != "admin":
        base_filters.append(col(SlurmJobDB.sarc_user_id) == scope_user_id)

    max_l, max_e, max_w = sess.exec(
        select(
            func.max(SlurmJobDB.time_limit).label("max_l"),
            func.max(SlurmJobDB.elapsed_time).label("max_e"),
            func.max(wait_expr).label("max_w"),
        ).where(*base_filters)
    ).one()

    if max_l is None:
        # No matching rows
        return {"elapsed_vs_limit": None, "wait_vs_limit": None, "total_jobs": 0}

    elapsed_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        SlurmJobDB.time_limit,
        SlurmJobDB.elapsed_time,
        float(max_l),
        float(max_e),
    )
    wait_hmap = _build_heatmap_payload(
        sess, base_filters, SlurmJobDB.time_limit, wait_expr, float(max_l), float(max_w)
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


# Postgres treats NaN = NaN as TRUE (unlike IEEE/Python), so `expr == expr` does
# NOT exclude NaN. Compare against this literal instead — it also adds no bind
# parameter, sidestepping pg8000's quirks around bound values.
_NAN = literal_column("'NaN'::float8")


def _is_real(expr):
    """SQL predicate: expr is a usable number — neither NULL nor NaN."""
    return and_(expr.is_not(None), expr != _NAN)


def _valid_metric_filter(metric_expr):
    """SQL predicate: metric is a real number (not NULL/NaN) and >= 0."""
    return and_(_is_real(metric_expr), metric_expr >= 0)


@router.get("/metrics/metric_distribution")
def metrics_metric_distribution(
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Duration-weighted distribution of a normalized GPU metric.

    ``metric`` is a [0, 1] GPU/system stat (e.g. gpu_sm_occupancy). Over GPU jobs
    in the window, bins each job's mean value into 50 bins weighted by
    rgu * elapsed (so long/big jobs count more). Returns {primary: {values,
    weights}}. The paired (metric vs metric2) heatmap is a separate endpoint,
    /metrics/metric_comparison.
    """
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")

    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    # Read the metric mean via a targeted LEFT JOIN on jobstatisticdb instead of
    # JobSeriesDB.statistics, which forces a per-row json_object_agg of all
    # stats. See histogram for the rationale.
    js1 = aliased(JobStatisticDB)
    m1 = col(js1.mean)
    weight = _RGU_DRAC * col(SlurmJobDB.elapsed_time)
    bin_width = 1.0 / _DENSITY_BINS

    # _apply_rgu_base anchors the FROM (SlurmJobDB + gpurgudb), keeps only jobs
    # with a calculable RGU and adds the common filters; then attach the stat
    # alias on the job id.
    bin_expr = _density_bin_expr(m1).label("bin")
    q = (
        _apply_rgu_base(
            select(bin_expr, func.sum(weight).label("w")),
            sess,
            clusters,
            cluster_user,
            job_states,
            scope_user_id=_scope(req),
        )
        .join(
            js1,
            and_(col(js1.job_id) == col(SlurmJobDB.id), col(js1.name) == metric),
            isouter=True,
        )
        .where(
            col(SlurmJobDB.submit_time) >= begin_dt,
            col(SlurmJobDB.submit_time) < finish_dt,
            _valid_metric_filter(m1),
        )
        .group_by("bin")
        .order_by("bin")
    )

    # Each bin yields a single (centre, weight) pair; Plotly's nbinsx=50 resolves
    # them back to a 50-bar density plot.
    values, weights = [], []
    for r in sess.exec(q):
        values.append((int(r.bin) + 0.5) * bin_width)
        weights.append(float(r.w or 0.0))
    return {"primary": {"values": values, "weights": weights}}


@router.get("/metrics/metric_comparison")
def metrics_metric_comparison(
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    metric2: str = Query(default="gpu_memory"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """100x100 paired heatmap of two normalized GPU metrics.

    Counts GPU jobs in the window into a 100x100 grid of (metric, metric2) mean
    values; a job contributes only if it carries both stats. No sampling: every
    job lands in exactly one cell (like the elapsed/wait heatmaps). Returns
    {x, y, z} with z[iby][ibx] the job count of that cell (Plotly heatmap order).
    """
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")
    if metric2 not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric2!r}")

    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    js1 = aliased(JobStatisticDB)
    js2 = aliased(JobStatisticDB)
    m1 = col(js1.mean)
    m2 = col(js2.mean)

    bx = _density_bin_expr(m1, _PAIRED_BINS).label("bx")
    by = _density_bin_expr(m2, _PAIRED_BINS).label("by")
    # group_by by label, not by expression: pg8000's server-side binding renders
    # the expression with fresh placeholders in GROUP BY (error 42803).
    q = (
        _apply_rgu_base(
            select(bx, by, func.count().label("n")),
            sess,
            clusters,
            cluster_user,
            job_states,
            scope_user_id=_scope(req),
        )
        .join(
            js1,
            and_(col(js1.job_id) == col(SlurmJobDB.id), col(js1.name) == metric),
            isouter=True,
        )
        .join(
            js2,
            and_(col(js2.job_id) == col(SlurmJobDB.id), col(js2.name) == metric2),
            isouter=True,
        )
        .where(
            col(SlurmJobDB.submit_time) >= begin_dt,
            col(SlurmJobDB.submit_time) < finish_dt,
            _valid_metric_filter(m1),
            _valid_metric_filter(m2),
        )
        .group_by("bx", "by")
    )

    z = [[0] * _PAIRED_BINS for _ in range(_PAIRED_BINS)]  # z[by][bx] (Plotly)
    for r in sess.exec(q):
        z[int(r.by)][int(r.bx)] += int(r.n)

    centres = [(i + 0.5) / _PAIRED_BINS for i in range(_PAIRED_BINS)]
    return {"x": centres, "y": centres, "z": z}


@router.get("/metrics/rgu_usage")
def metrics_rgu_usage(
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    sess: Session = Depends(session_dep),
):
    """Requested vs effectively-used RGU.h per time bucket.

    Over GPU jobs submitted in each ``period`` bucket: ``rgu_requested`` =
    SUM(rgu * elapsed / 3600); ``rgu_used`` = the same scaled by each job's mean
    ``metric`` (e.g. gpu_sm_occupancy). Their gap is wasted GPU capacity. Returns
    one row per bucket.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)

    # Aggregate per bucket: SUM(rgu * elapsed / 3600) for requested, the same
    # times the metric mean for used (the `m == m` test nulls NaN to 0). The
    # metric mean comes from a targeted LEFT JOIN on jobstatisticdb (one metric,
    # job_id index) — never the per-row json_object_agg of JobSeriesDB.statistics.
    bucket_expr = _bucket_expr(parsed, col(SlurmJobDB.submit_time), begin_dt)
    rgu_hours = _RGU_DRAC * col(SlurmJobDB.elapsed_time) / 3600.0
    m_mean = col(JobStatisticDB.mean)
    # Split used vs unmeasured on whether the metric is a real value (not
    # NULL/NaN); a missing measurement is kept apart from "unused" rather than
    # counted as waste.
    m_present = _is_real(m_mean)
    rgu_used_term = case((m_present, rgu_hours * m_mean), else_=0.0)
    rgu_unmeasured_term = case((m_present, 0.0), else_=rgu_hours)

    query = _apply_rgu_base(
        select(
            bucket_expr,
            func.sum(rgu_hours).label("rgu_requested"),
            func.sum(rgu_used_term).label("rgu_used"),
            func.sum(rgu_unmeasured_term).label("rgu_unmeasured"),
        ),
        sess,
        clusters,
        cluster_user,
        job_states,
        scope_user_id=_scope(req),
    )
    query = (
        query.join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(SlurmJobDB.id),
                col(JobStatisticDB.name) == metric,
            ),
            isouter=True,
        )
        .where(
            col(SlurmJobDB.submit_time) >= begin_dt,
            col(SlurmJobDB.submit_time) < finish_dt,
        )
        .group_by("bucket")
        .order_by("bucket")
    )

    sums = {
        _sql_bucket_key(parsed, row.bucket): (
            float(row.rgu_requested or 0.0),
            float(row.rgu_used or 0.0),
            float(row.rgu_unmeasured or 0.0),
        )
        for row in sess.exec(query)
    }

    period_data = []
    for key, ps, pe in _iter_buckets(begin_dt, finish_dt, parsed):
        requested, used, unmeasured = sums.get(key, (0.0, 0.0, 0.0))
        period_data.append(
            {
                "period_start": ps.strftime(fmt),
                "period_end": pe.strftime(fmt),
                "rgu_requested": requested,
                "rgu_used": used,
                "rgu_unmeasured": unmeasured,
            }
        )

    return period_data


@router.get("/metrics/rgu_by_cluster")
def metrics_rgu_by_cluster(
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    sess: Session = Depends(session_dep),
):
    """Total RGU.h per period, stacked by cluster.

    Aggregates the same RGU metric as /rgu_usage (SUM(rgu * elapsed / 3600))
    and groups by cluster_name. When ``clusters`` is given, only those clusters
    are kept (empty = all clusters). Returns one series per cluster, aligned on
    a shared period axis; clusters with no RGU at all (e.g. no billing) are
    dropped.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)

    # cluster_name is the stack key, so the query always joins clusters
    bucket_expr = _bucket_expr(parsed, col(SlurmJobDB.submit_time), begin_dt)
    rgu_hours = _RGU_DRAC * col(SlurmJobDB.elapsed_time) / 3600.0
    query = _apply_rgu_base(
        select(
            bucket_expr,
            col(SlurmClusterDB.name).label("cluster_name"),
            func.sum(rgu_hours).label("rgu"),
        ),
        sess,
        clusters,
        cluster_user,
        job_states,
        scope_user_id=_scope(req),
    ).join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
    query = (
        query.where(
            col(SlurmJobDB.submit_time) >= begin_dt,
            col(SlurmJobDB.submit_time) < finish_dt,
        )
        .group_by("bucket", "cluster_name")
        .order_by("bucket")
    )

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
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    sess: Session = Depends(session_dep),
):
    """Per-period averages of a metric's per-job ``mean`` and ``max``.

    For each period bucket, averages the per-job statistic values over the
    jobs submitted in that bucket (plain per-job average, not duration
    weighted). Jobs lacking the statistic are simply absent from the average
    (inner join) and no GPU/RGU filter is applied, so system metrics also
    cover CPU-only jobs. Returns a single ``series`` entry (the requested
    metric) on a period axis; buckets with no data yield null (a curve gap),
    not 0.
    """
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")

    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)

    # No RGU/statistics-view columns are needed, so query SlurmJobDB directly
    # rather than the JobSeriesDB view (whose per-row billing lateral and
    # user/membertype joins dominate). The view's inner joins never drop a job,
    # so the result is identical. Same pattern as /metrics/job_counts.
    bucket_expr = _bucket_expr(parsed, SlurmJobDB.submit_time, begin_dt)
    m_mean = col(JobStatisticDB.mean)
    m_max = col(JobStatisticDB.max)
    # NaN-proof averages: a single NaN would contaminate the whole AVG, so each
    # value is nulled out unless it is real (`x == x` won't do this on Postgres,
    # where NaN = NaN is TRUE — see _is_real).
    avg_mean = func.avg(case((_is_real(m_mean), m_mean))).label("avg_mean")
    avg_max = func.avg(case((_is_real(m_max), m_max))).label("avg_max")

    query = (
        select(bucket_expr, avg_mean, avg_max)
        .select_from(SlurmJobDB)
        .join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(SlurmJobDB.id),
                col(JobStatisticDB.name) == metric,
            ),
        )
        .where(
            col(SlurmJobDB.submit_time) >= begin_dt,
            col(SlurmJobDB.submit_time) < finish_dt,
        )
        .group_by("bucket")
        .order_by("bucket")
    )
    query = _apply_slurm_job_filters(
        query,
        _resolve_cluster_ids(sess, clusters),
        cluster_user,
        job_states,
        _scope(req),
    )

    cells = {}
    for r in sess.exec(query):
        key = _sql_bucket_key(parsed, r.bucket)
        cells[key] = (_nan_to_none(r.avg_mean), _nan_to_none(r.avg_max))

    buckets = list(_iter_buckets(begin_dt, finish_dt, parsed))
    return {
        "periods": [
            {"period_start": ps.strftime(fmt), "period_end": pe.strftime(fmt)}
            for _, ps, pe in buckets
        ],
        "series": [
            {
                "metric": metric,
                "mean": [cells.get(k, (None, None))[0] for k, _, _ in buckets],
                "max": [cells.get(k, (None, None))[1] for k, _, _ in buckets],
            }
        ],
    }


@router.get("/metrics/rgu_by_user")
def metrics_rgu_by_user(
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Requested vs used RGU.h aggregated per user (not over time).

    Same RGU.h measure as /rgu_usage, summed per cluster_user over GPU jobs in the
    window (requested = SUM(rgu * elapsed / 3600); used = scaled by the mean
    ``metric``). Returns a list sorted by descending requested RGU.h.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    # Aggregate by user: SUM(rgu * elapsed / 3600) per user. Metric mean via a
    # targeted LEFT JOIN on jobstatisticdb (one metric, job_id index) — see
    # rgu_usage.
    rgu_hours = _RGU_DRAC * col(SlurmJobDB.elapsed_time) / 3600.0
    m_mean = col(JobStatisticDB.mean)
    # Split used vs unmeasured on whether the metric is a real value (not
    # NULL/NaN); a missing measurement is kept apart from "unused".
    m_present = _is_real(m_mean)
    rgu_used_term = case((m_present, rgu_hours * m_mean), else_=0.0)
    rgu_unmeasured_term = case((m_present, 0.0), else_=rgu_hours)
    user_expr = func.coalesce(col(SlurmJobDB.cluster_user), "unknown").label("user")
    rgu_requested_sum = func.sum(rgu_hours).label("rgu_requested")

    query = _apply_rgu_base(
        select(
            user_expr,
            rgu_requested_sum,
            func.sum(rgu_used_term).label("rgu_used"),
            func.sum(rgu_unmeasured_term).label("rgu_unmeasured"),
        ),
        sess,
        clusters,
        cluster_user,
        job_states,
        scope_user_id=_scope(req),
    )
    query = (
        query.join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(SlurmJobDB.id),
                col(JobStatisticDB.name) == metric,
            ),
            isouter=True,
        )
        .where(
            col(SlurmJobDB.submit_time) >= begin_dt,
            col(SlurmJobDB.submit_time) < finish_dt,
        )
        .group_by("user")
        .order_by(rgu_requested_sum.desc(), user_expr)
    )

    return [
        {
            "user": row.user,
            "rgu_requested": float(row.rgu_requested or 0.0),
            "rgu_used": float(row.rgu_used or 0.0),
            "rgu_unmeasured": float(row.rgu_unmeasured or 0.0),
        }
        for row in sess.exec(query)
    ]


@router.get("/metrics/jobs")
def metrics_jobs(
    req: Requestor = Depends(requestor),
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    limit: int = Query(default=50, gt=0, le=500),
    offset: int = Query(default=0, ge=0),
    include_total: bool = Query(default=True),
    sort_by: str = Query(default="rgu_hours"),
    sort_dir: str = Query(default="desc"),
    metric: str = Query(default="gpu_sm_occupancy"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Paginated, sortable table of individual jobs.

    Lists GPU jobs submitted in the window (cluster/user/state filtered), one row
    per job: cluster, user, state, elapsed, GPU counts, billing, gpu_type, rgu,
    rgu_hours, per-job metric means and ``waste`` (rgu_hours * (1 - mean)). Sorted
    by ``sort_by``/``sort_dir`` and paginated by ``limit``/``offset``. Returns
    {total, jobs}. ``total`` is the full filtered count, computed by a SEPARATE
    query (kept out of the page query so the page parallelises — see below) and
    only when ``include_total`` is set (None otherwise). The frontend requests it
    on every page so the count and page numbers stay current as scraping adds
    jobs; the separate query stays cheap precisely because it parallelises.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)

    # Limit-first pagination. A `page` subquery ranks, paginates and counts the
    # full filtered set from the source alone (plus the single stat the sort needs,
    # if any); the outer query then joins the 3 display stats back onto just that
    # page of rows. Without this split, the 3 stat joins + count(*) would run over
    # the whole window (millions of rows) just to return 50. See the perf note in
    # docs / the /metrics/jobs investigation.
    rgu_hours_raw = _RGU_DRAC * col(SlurmJobDB.elapsed_time) / 3600.0

    # One aliased jobstatisticdb row per distinct stat name, LEFT-joined on the
    # job id (never JobSeriesDB.statistics, a per-row json_object_agg — see
    # rgu_usage).
    stat_names = {metric, "gpu_utilization", "gpu_sm_occupancy", "gpu_memory"}
    js = {name: aliased(JobStatisticDB) for name in sorted(stat_names)}
    metric_mean_raw = col(js[metric].mean)

    def _join_stat(query, name: str):
        alias = js[name]
        return query.join(
            alias,
            and_(col(alias.job_id) == col(SlurmJobDB.id), col(alias.name) == name),
            isouter=True,
        )

    # Sortable columns -> ORDER BY expression. Raw (unlabelled) so they compose
    # with nulls_last/asc/desc cleanly. `nodes` is an array and is not sortable,
    # so it is intentionally absent. Ranking by "cluster" needs the clusters join
    # and the keys in `sort_needs_stat` need their stat alias, so the page joins
    # just the one it needs; every other key ranks on slurm_jobs(+gpurgudb) alone.
    sortable = {
        "cluster": col(SlurmClusterDB.name),
        "job_id": col(SlurmJobDB.job_id),
        "submit_time": col(SlurmJobDB.submit_time),
        "user": col(SlurmJobDB.cluster_user),
        "job_state": col(SlurmJobDB.job_state),
        "elapsed": col(SlurmJobDB.elapsed_time),
        "requested_gpu": col(SlurmJobDB.requested_gres_gpu),
        "allocated_gpu": col(SlurmJobDB.allocated_gres_gpu),
        "billing": col(SlurmJobDB.allocated_billing),
        "gpu_type": func.coalesce(
            col(SlurmJobDB.harmonized_gpu_type), col(SlurmJobDB.allocated_gpu_type)
        ),
        "gpu_type_rgu": col(GpuRguDB.drac_rgu),
        "rgu": _RGU_DRAC,
        "rgu_hours": rgu_hours_raw,
        "waste": rgu_hours_raw * (1 - metric_mean_raw),
        "gpu_utilization_mean": col(js["gpu_utilization"].mean),
        "gpu_sm_occupancy_mean": col(js["gpu_sm_occupancy"].mean),
        "gpu_memory_max": col(js["gpu_memory"].max),
    }
    sort_needs_stat = {
        "waste": metric,
        "gpu_utilization_mean": "gpu_utilization",
        "gpu_sm_occupancy_mean": "gpu_sm_occupancy",
        "gpu_memory_max": "gpu_memory",
    }
    sort_expr = sortable.get(sort_by, rgu_hours_raw)
    ordered = sort_expr.asc() if sort_dir == "asc" else sort_expr.desc()
    # NULLs (e.g. a job missing this metric) always sort last; the job id is a
    # unique tiebreaker that makes the order total, so offset pagination never
    # skips or repeats a row between pages. Reused verbatim by the page and final
    # queries so both produce the same order.
    order_by = (nulls_last(ordered), col(SlurmJobDB.id))

    # Window only; the gpu_type/RGU validity filter now lives in _apply_rgu_base.
    base_filters = (
        col(SlurmJobDB.submit_time) >= begin_dt,
        col(SlurmJobDB.submit_time) < finish_dt,
    )

    # COUNT: the full filtered total, computed by its own query and only when
    # asked (include_total). The frontend requests it on every page so the count
    # and page numbers stay current as scraping adds jobs. It is deliberately kept
    # OUT of the page query below: a `count(*) OVER ()` there forces the whole
    # filtered set to be materialised AND disables parallelism, so every page would
    # pay the full-set cost. Isolated like this the count parallelises (and needs
    # neither the clusters nor the stat join), so paying it per page stays cheap.
    total: int | None = None
    if include_total:
        count_q = _apply_rgu_base(
            select(func.count()),
            sess,
            clusters,
            cluster_user,
            job_states,
            scope_user_id=_scope(req),
        ).where(*base_filters)
        total = int(sess.exec(count_q).one())

    # PAGE: the page's job ids only. The scan/sort runs here on the source alone
    # (+ the sort's stat alias when needed). With no window count it parallelises,
    # and a small offset top-N heapsorts instead of sorting the whole set.
    page_q = _apply_rgu_base(
        select(col(SlurmJobDB.id).label("jid")),
        sess,
        clusters,
        cluster_user,
        job_states,
        scope_user_id=_scope(req),
    )
    # clusters only when ranking by cluster_name; the stat alias only for stat sorts.
    if sort_by == "cluster":
        page_q = page_q.join(
            SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id)
        )
    if sort_by in sort_needs_stat:
        page_q = _join_stat(page_q, sort_needs_stat[sort_by])
    page = (
        page_q.where(*base_filters).order_by(*order_by).offset(offset).limit(limit)
    ).subquery()

    # FINAL: display columns + the 3 stats, fetched only for the page's rows
    # (joined back on the job id). The total comes from the separate count above.
    query = (
        _apply_rgu_base(
            select(  # ty:ignore[no-matching-overload]
                col(SlurmClusterDB.name).label("cluster_name"),
                col(SlurmJobDB.job_id),
                col(SlurmJobDB.submit_time).label("submit_time"),
                col(SlurmJobDB.cluster_user),
                col(SlurmJobDB.job_state),
                col(SlurmJobDB.elapsed_time).label("elapsed_time"),
                col(SlurmJobDB.nodes),
                col(SlurmJobDB.requested_gres_gpu),
                col(SlurmJobDB.allocated_gres_gpu),
                col(SlurmJobDB.allocated_billing),
                col(SlurmJobDB.allocated_gpu_type).label("allocated_gpu_type"),
                col(SlurmJobDB.harmonized_gpu_type),
                col(GpuRguDB.drac_rgu).label("gpu_type_rgu_drac"),
                _RGU_DRAC.label("rgu"),
                rgu_hours_raw.label("rgu_hours"),
                metric_mean_raw.label("metric_mean"),
                col(js["gpu_utilization"].mean).label("gpu_utilization_mean"),
                col(js["gpu_sm_occupancy"].mean).label("gpu_sm_occupancy_mean"),
                col(js["gpu_memory"].max).label("gpu_memory_max"),
            ),
            sess,
            clusters,
            cluster_user,
            job_states,
            scope_user_id=_scope(req),
        )
        .join(SlurmClusterDB, col(SlurmJobDB.cluster_id) == col(SlurmClusterDB.id))
        .join(page, page.c.jid == col(SlurmJobDB.id))
    )
    for name in js:
        query = _join_stat(query, name)
    query = query.order_by(*order_by)

    jobs = []
    for row in sess.exec(query):
        mm = _nan_to_none(row.metric_mean)
        rh = float(row.rgu_hours)
        waste = round(rh * (1 - mm), 2) if mm is not None else None
        jobs.append(
            {
                "cluster": row.cluster_name or "",
                "job_id": row.job_id,
                "submit_time": row.submit_time.isoformat() if row.submit_time else None,
                "user": row.cluster_user or "",
                "job_state": row.job_state.value if row.job_state is not None else "",
                "elapsed": row.elapsed_time or 0,
                "nodes": ", ".join(row.nodes or []) or None,
                "requested_gpu": row.requested_gres_gpu,
                "allocated_gpu": row.allocated_gres_gpu,
                "billing": row.allocated_billing,
                # Harmonised name (the one RGU is computed from) when known;
                # raw Slurm name otherwise.
                "gpu_type": row.harmonized_gpu_type or row.allocated_gpu_type or "",
                "gpu_type_rgu": _nan_to_none(row.gpu_type_rgu_drac),
                "rgu": round(float(row.rgu), 2),
                "rgu_hours": round(rh, 2),
                "waste": waste,
                "gpu_utilization_mean": _nan_to_none(row.gpu_utilization_mean),
                "gpu_sm_occupancy_mean": _nan_to_none(row.gpu_sm_occupancy_mean),
                "gpu_memory_max": _nan_to_none(row.gpu_memory_max),
            }
        )

    return {"total": total, "jobs": jobs}
