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
from sqlalchemy import ARRAY, Float, literal, literal_column, nulls_last, text
from sqlalchemy.orm import aliased
from sqlmodel import Session, and_, case, col, func, select

from sarc.api.v0 import Requestor, requestor
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB
from sarc.db.job_series import JobSeriesDB, job_series_select
from sarc.db.users import MatchingID, UserDB
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


def _find_user_by_email(sess: Session, email: str) -> UserDB | None:
    """mila_ldap email -> UserDB row, or None if unknown. Mirrors the lookup in
    ``requestor()``; used to resolve an admin's ``as_user`` override."""
    return sess.exec(
        select(UserDB)
        .join(MatchingID)
        .where(MatchingID.plugin_name == "mila_ldap", MatchingID.match_id == email)
    ).one_or_none()


def _scope_or_view_as(
    sess: Session, req: Requestor, as_user: str | None
) -> int | Literal["admin"]:
    """Scope for a /dash data query: ``_scope(req)`` by default, or — admin
    only — the impersonated user's id when ``as_user`` is set (the dashboard's
    "view as user" preview). Fails closed: 403 for a non-admin, 404 for an
    unknown email, never a silent fallback to the admin's full view."""
    if as_user is None:
        return _scope(req)
    if not req.is_admin:
        raise HTTPException(status_code=403, detail="as_user is admin-only")
    target = _find_user_by_email(sess, as_user)
    if target is None:
        raise HTTPException(status_code=404, detail=f"Unknown user {as_user!r}")
    assert target.id is not None
    return target.id


async def _dash_login_redirect(request: Request) -> None:
    """Router-level gate for ``/dash``: redirect unauthenticated requests to the
    OAuth login page instead of returning the API's 401. ``ensure_email`` raises
    a 307 to the ``/login`` route (recording the target URL in the session, so
    the user lands back on the dashboard after logging in) when anonymous, and
    returns the email (unused here — ``requestor`` re-derives it) otherwise.

    Registered *before* ``requestor`` in the router dependencies (FastAPI
    resolves them in order), so an anonymous request redirects here before the
    capability check in ``requestor`` runs; authenticated ones fall through to
    it. No-op when auth is disabled."""
    auth = config.server.auth
    if auth is None:
        return
    await auth.ensure_email(request)


router = APIRouter(
    prefix="/dash", dependencies=[Depends(_dash_login_redirect), Depends(requestor)]
)


# Postgres defaults to 8. It caps how many relations go into one join list, and
# the planner reorders only within a list: what does not fit is planned as its own
# unit first. job_series is 8 relations, so a subquery joined on top makes 9, and
# at 8 the view gets built for the whole window before that join happens. That is
# what /metrics/jobs pays: its 50-row page can no longer drive the query (6 s vs
# 0.1 s on 12M jobs). Postgres drops the view's unused joins either way -- they
# just count towards the limit first. 9 is enough; 12 adds headroom, ~1 ms planning.
# Do not raise it much further: geqo_threshold is 12 too. Past that many relations
# in one list, Postgres stops trying every join order and uses a genetic algorithm,
# which can pick a different plan on each run.
_JOIN_COLLAPSE_LIMIT = 12


def session_dep() -> Generator[Session]:
    with config.db.session() as sess:
        # LOCAL so it dies with this request's transaction instead of riding the
        # pooled connection into the next one (/v0 shares this engine); all /dash
        # queries run in that transaction, so one statement covers them.
        sess.connection().execute(
            text(f"SET LOCAL join_collapse_limit = {_JOIN_COLLAPSE_LIMIT}")
        )
        yield sess


UTC = timezone.utc

_DEFAULT_WINDOW_DAYS = 1
_DEFAULT_PERIOD = "w"

# GPU/system metrics (stored per-job in JobStatisticDB) normalized to [0, 1]
_METRICS_0_1: dict[str, str] = {
    "gpu_sm_occupancy": "SM occupancy",
    "gpu_utilization": "GPU utilization",
    "gpu_utilization_fp16": "GPU util. FP16",
    "gpu_utilization_fp32": "GPU util. FP32",
    "gpu_utilization_fp64": "GPU util. FP64",
    "gpu_memory": "GPU memory",
    "system_memory": "System memory",
}

# Metric means overlaid on the RGU-usage plot: fixed, unlike the `metric`
# selector that drives the bars, so the two curves stay comparable over time.
_TREND_METRICS = ("gpu_sm_occupancy", "gpu_utilization")


_PERIOD_RE = re.compile(r"^(\d+(?:\.\d+)?)?\s*([hdwm]?)$", re.IGNORECASE)
_PERIOD_MULTIPLIERS = {"h": 1 / 24, "d": 1, "w": 7, "m": 30}
# Single-letter period -> PostgreSQL date_trunc field, for calendar bucketing.
_CALENDAR_TRUNC = {"h": "hour", "d": "day", "w": "week", "m": "month"}


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


def _iter_buckets(begin_dt: datetime, finish_dt: datetime, period: timedelta | str):
    """Yield (period_start, period_end) for every bucket in [begin, finish),
    clipped to the range.

    The single definition of where a bucket starts and ends: ``_bucket_table``
    hands these same bounds to SQL, so ``bucket_index`` is a position here.
    """
    if isinstance(period, timedelta):
        cur = begin_dt
        while cur < finish_dt:
            nxt = cur + period
            yield cur, min(nxt, finish_dt)
            cur = nxt
    else:
        frontier = _calendar_trunc(begin_dt, period)
        while frontier < finish_dt:
            nxt = _calendar_next(frontier, period)
            yield max(frontier, begin_dt), min(nxt, finish_dt)
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
    finish_dt = datetime(finish.year, finish.month, finish.day, tzinfo=UTC)
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
    # A focus outside the range clamps past it. Collapse to the empty window
    # rather than an inverted one: _ran_between would build a tstzrange whose
    # lower bound is above its upper, which Postgres rejects outright.
    return begin_dt, max(begin_dt, finish_dt)


# --------------------------------------------------------------------------- #
# Pro-rating: attributing a job's usage to the time it was actually running
# --------------------------------------------------------------------------- #
#
# A job is charged to the periods it ran in, in proportion to the time it spent
# in each, rather than wholly to the period it was submitted in: a bar reads as
# "RGU.h spent in this week", not "RGU.h eventually spent by the jobs submitted
# this week". Two consequences: a job submitted before the window but running
# inside it now counts, and a bar no longer sums the jobs it is drawn from --
# only the slice of each that lands in the window. Every endpoint selects
# through the same ``_ran_between`` predicate.


def _job_run(cols):
    """The job's run as a ``tstzrange``, through the indexed SQL function."""
    return func.slurm_job_run(cols.start_time, cols.elapsed_time)


def _job_span(cols):
    """A job's run as epoch seconds: ``[start, start + elapsed)``.

    Anchored on ``elapsed_time``, not ``end_time``, so the slices add back up to
    the view's cost columns, which are all built from elapsed. The two disagree
    on 7.7 % of production jobs, ``end - start`` being the larger: whatever
    stretches the wall-clock (suspension, requeue) is not time consumed.
    """
    start = func.extract("epoch", cols.start_time)
    return start, start + cols.elapsed_time


def _ran_between(cols, lo, hi):
    """SQL predicate: the job was running at some point within ``[lo, hi)``.

    Bounds are epoch seconds -- either Python floats for a fixed window, or the
    bucket columns of ``_bucket_table``.

    A range overlap, not two comparisons: ``ix_slurm_jobs_run`` indexes exactly
    this expression (see ``slurm_job_run`` in sarc/db/job.py), and any other
    spelling reads every row. The degenerate cases fall out of range semantics:
    a zero-elapsed run is the empty range and overlaps nothing, which also
    excludes the incoherent rows (end before start, start before submit), all of
    them zero-elapsed; a job that never started yields NULL, the function being
    STRICT.
    """
    return _job_run(cols).op("&&")(
        func.tstzrange(func.to_timestamp(lo), func.to_timestamp(hi), "[)")
    )


def _overlap_hours(cols, lo, hi):
    """SQL expression: hours of the job's run that fall inside ``[lo, hi)``.

    Only meaningful where ``_ran_between`` holds; elsewhere it goes negative.
    Multiply by a per-job RGU *rate* (``allocated_rgu_drac``, GPU count x RGU
    weight, no time of its own) for the RGU.h the job owes to that span.
    """
    start, end = _job_span(cols)
    return (func.least(end, hi) - func.greatest(start, lo)) / 3600.0


def _bucket_table(begin_dt: datetime, finish_dt: datetime, period: timedelta | str):
    """The window's buckets as a table to join jobs against.

    Carries the bounds ``_iter_buckets`` already computes into SQL as epoch
    seconds: one definition of where a bucket starts, and no ``date_trunc``
    whose result depends on the session TimeZone. Rows are identified by
    position, matched back against the caller's own ``_iter_buckets`` list.

    Joining against it also selects: a job overlapping no bucket is outside the
    window, and unlike a submit_time range it keeps a job that started before it.

    Bounds go in as two arrays rather than a VALUES list: the Bind message counts
    parameters on an Int16, so three columns per bucket capped the endpoint at
    65535/3 = 21845 buckets (pg8000 raised before the server saw anything). Two
    parameters whatever the count, same plan, fixed-size statement.
    ``with_ordinality`` numbers from 1, making the position a guarantee.
    """
    bounds = list(_iter_buckets(begin_dt, finish_dt, period))
    unnested = (
        func.unnest(
            literal([ps.timestamp() for ps, _ in bounds], ARRAY(Float)),
            literal([pe.timestamp() for _, pe in bounds], ARRAY(Float)),
        )
        # render_derived: PG names a set-returning function's columns after the
        # function, AS bucket_bounds(...) names them one by one.
        .table_valued("bucket_start", "bucket_end", with_ordinality="ord")
        .render_derived()
        .alias("bucket_bounds")
    )
    return select(
        (unnested.c.ord - 1).label("bucket_index"),
        unnested.c.bucket_start,
        unnested.c.bucket_end,
    ).subquery("buckets")


def _no_buckets(begin_dt: datetime, finish_dt: datetime) -> bool:
    """Empty window (start == end): the bucketed endpoints return their empty
    shape instead of querying. It is the only case ``_iter_buckets`` yields
    nothing for, a calendar frontier being floored to at or before ``begin``.
    """
    return begin_dt >= finish_dt


def _nan_to_none(
    v: float | None, replace_with: float | int | None = None
) -> float | None:
    return replace_with if (isinstance(v, float) and math.isnan(v)) else v


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


def _apply_job_filters(
    query,
    cols,
    cluster_ids: list[int] | None,
    cluster_user: str | None,
    job_states: list[str],
    scope_user_id: int | Literal["admin"],
):
    """Common job filters on ``cols`` — a column namespace: a table/view model
    class, or the ``.c`` of a job_series_select subquery.

    Filters by cluster_ids (resolved upfront, no clusters join needed); empty
    means no cluster filter. ``scope_user_id`` (a non-admin's UserDB id)
    restricts to that user's jobs; the sentinel ``"admin"`` applies no scoping.
    """
    if job_states:
        query = query.where(cols.job_state.in_(job_states))
    if cluster_ids:
        query = query.where(cols.cluster_id.in_(cluster_ids))
    if cluster_user:
        query = query.where(cols.cluster_user == cluster_user)
    if scope_user_id != "admin":
        query = query.where(cols.sarc_user_id == scope_user_id)
    return query


def _apply_rgu_base_view(
    query,
    sess: Session,
    clusters: list[str],
    cluster_user: str | None,
    job_states: list[str],
    *,
    scope_user_id: int | Literal["admin"],
):
    """Build the shared base of every RGU query, on the job_series view.

    The view already carries the RGU weight (``allocated_rgu_drac`` =
    coalesce(allocated_gres_gpu, 0) * drac_rgu) and ``cluster_name``, so no
    gpurgudb/clusters join is needed. Keeps only GPU jobs whose physical RGU is
    computable, then the common cluster/user/state filters; the caller adds its
    own time window.
    """
    query = query.select_from(JobSeriesDB).where(
        col(JobSeriesDB.allocated_gpu_type).is_not(None),
        col(JobSeriesDB.allocated_rgu_drac).is_not(None),
    )
    return _apply_job_filters(
        query,
        JobSeriesDB,
        _resolve_cluster_ids(sess, clusters),
        cluster_user,
        job_states,
        scope_user_id,
    )


_TEMPLATES = Jinja2Templates(directory=Path(__file__).parent)


_AS_USER_QUERY = Query(
    default=None,
    description="Admin-only: preview scoped to this user's jobs (mila_ldap email).",
)


@router.get("/metrics", response_class=HTMLResponse)
def metrics_homepage(
    request: Request,
    req: Requestor = Depends(requestor),
    as_user: str | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Serve the dashboard's single-page HTML UI; its charts call the JSON
    endpoints below. Rendered with Jinja2: ``is_admin`` adapts the page
    per-request (hide the user filter / RGU-by-user for a non-admin) with no
    round-trip — the backend scopes the data regardless — and the connected
    email is shown in the title/header, coloured by role (admin red, user grey).
    Jinja auto-escapes ``user_email`` in HTML; ``| tojson`` makes the
    booleans/lists safe to inline in <script>.

    ``as_user`` (mila_ldap email) lets an admin preview the dashboard exactly
    as that user would see it: ``is_admin`` in the template goes False (hiding
    the admin-only widgets, same as the JSON endpoints scoping via
    ``_scope_or_view_as``) while ``admin_email``/``user_email`` keep showing
    the real admin's identity, next to a "clear" control. An unknown email is
    a soft error (``view_as_error``) that leaves the admin in their own view
    rather than a 404 page — a typo shouldn't blow away the dashboard; a
    non-admin supplying ``as_user`` still gets a hard 403.

    ``storage_key`` namespaces the per-user localStorage state; it hashes the
    identity rather than using the email in clear. The key is
    ``(identity, role, view-as target)``: each distinct view gets its own
    bucket, so a role change (promote/demote, or the force_user toggle) or an
    admin's view-as preview never reloads selections for controls it no longer
    shows. A preview's bucket also stays separate from the target's real
    session. It's a namespacing key, not a secret."""
    view_as_email = None
    view_as_error = None
    if as_user is not None:
        if not req.is_admin:
            raise HTTPException(status_code=403, detail="as_user is admin-only")
        if _find_user_by_email(sess, as_user) is not None:
            view_as_email = as_user
        else:
            view_as_error = as_user
    effective_is_admin = req.is_admin and view_as_email is None
    storage_key = (
        "sarc_dash_v1_"
        + hashlib.sha256(
            f"{req.email}|{req.is_admin}|{view_as_email or ''}".encode()
        ).hexdigest()[:16]
    )
    return _TEMPLATES.TemplateResponse(
        request,
        "metrics.html",
        {
            "is_admin": effective_is_admin,
            "admin_email": req.email if req.is_admin else None,
            "view_as_email": view_as_email,
            "view_as_error": view_as_error,
            "user_email": req.email,
            "default_period": _DEFAULT_PERIOD,
            "job_states": [s.value for s in SlurmState],
            "storage_key": storage_key,
        },
    )


@router.get("/metrics/job_counts")
def metrics_job_counts(
    req: Requestor = Depends(requestor),
    as_user: str | None = _AS_USER_QUERY,
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    sess: Session = Depends(session_dep),
):
    """Job count per time bucket.

    Counts the jobs *running* in each ``period`` bucket of the window, after the
    cluster/user/state filters. Returns one {period_start, period_end, count} per
    bucket, with empty buckets reported as 0.

    A count is not an integral over time, so nothing is pro-rated: a job spanning
    three buckets is one job in each, and the counts do not add up to a number of
    distinct jobs. It reads as occupancy, not as a submission rate.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)
    if _no_buckets(begin_dt, finish_dt):
        return []

    # Count every job, GPU or not; the DB view would degrade this to a
    # full-table scan (see JobSeriesDB docstring), job_series_select keeps it
    # narrow.
    js = job_series_select(
        "start_time",
        "elapsed_time",
        "cluster_id",
        "cluster_user",
        "job_state",
        "sarc_user_id",
    ).subquery()
    bucket_table = _bucket_table(begin_dt, finish_dt, parsed)

    query = select(bucket_table.c.bucket_index, func.count().label("count")).join_from(
        js,
        bucket_table,
        _ran_between(js.c, bucket_table.c.bucket_start, bucket_table.c.bucket_end),
    )
    query = _apply_job_filters(
        query,
        js.c,
        _resolve_cluster_ids(sess, clusters),
        cluster_user,
        job_states,
        _scope_or_view_as(sess, req, as_user),
    )
    query = query.group_by(bucket_table.c.bucket_index).order_by(
        bucket_table.c.bucket_index
    )

    counts = {row.bucket_index: int(row.count) for row in sess.exec(query)}

    return [
        {
            "period_start": ps.strftime(fmt),
            "period_end": pe.strftime(fmt),
            "count": counts.get(i, 0),
        }
        for i, (ps, pe) in enumerate(_iter_buckets(begin_dt, finish_dt, parsed))
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
    as_user: str | None = _AS_USER_QUERY,
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

    Over jobs that ran in the window and have a time_limit:
    ``elapsed_vs_limit`` plots elapsed_time (y) against time_limit (x), and
    ``wait_vs_limit`` plots the queue wait, start - submit (y), against time_limit
    (x). Each is a 100x100 log-binned grid of job counts. Returns both grids plus
    total_jobs. ``focus_start/end`` narrows the window.

    **NB** This endpoint is the only one not yet optimized, because it would need
    to add or expand covering indexes. We should later decide if we really need
    this endpoint/plot (currently seen in admin view only).
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    cluster_ids = _resolve_cluster_ids(sess, clusters)

    # Query the view directly: the columns read here (time_limit/start_time/
    # elapsed_time) are in no index, so this full-scans regardless, and the
    # planner prunes the view's unused joins on its own -- job_series_select
    # would compile to the identical plan here (see the NB above).
    wait_expr = func.extract(
        "epoch", col(JobSeriesDB.start_time) - col(JobSeriesDB.submit_time)
    )
    # _ran_between already implies start_time IS NOT NULL (STRICT function).
    base_filters = [
        _ran_between(JobSeriesDB, begin_dt.timestamp(), finish_dt.timestamp()),
        col(JobSeriesDB.time_limit).is_not(None),
    ]
    if cluster_ids:
        base_filters.append(col(JobSeriesDB.cluster_id).in_(cluster_ids))
    if cluster_user:
        base_filters.append(col(JobSeriesDB.cluster_user) == cluster_user)
    if job_states:
        base_filters.append(col(JobSeriesDB.job_state).in_(job_states))
    scope_user_id = _scope_or_view_as(sess, req, as_user)
    if scope_user_id != "admin":
        base_filters.append(col(JobSeriesDB.sarc_user_id) == scope_user_id)

    max_l, max_e, max_w = sess.exec(
        select(
            func.max(col(JobSeriesDB.time_limit)).label("max_l"),
            func.max(col(JobSeriesDB.elapsed_time)).label("max_e"),
            func.max(wait_expr).label("max_w"),
        ).where(*base_filters)
    ).one()

    if max_l is None:
        # No matching rows
        return {"elapsed_vs_limit": None, "wait_vs_limit": None, "total_jobs": 0}

    elapsed_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        col(JobSeriesDB.time_limit),
        col(JobSeriesDB.elapsed_time),
        float(max_l),
        float(max_e),
    )
    wait_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        col(JobSeriesDB.time_limit),
        wait_expr,
        float(max_l),
        float(max_w),
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
    as_user: str | None = _AS_USER_QUERY,
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
    running in the window, bins each job's mean value into 50 bins weighted by
    the RGU-seconds it spent *inside* the window, so long/big jobs count more and
    a job running past the window edge weighs only for the part inside. Returns
    {primary: {values, weights}}. The paired (metric vs metric2) heatmap is a
    separate endpoint, /metrics/metric_comparison.
    """
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")

    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    window = (begin_dt.timestamp(), finish_dt.timestamp())

    # View-anchored: weight = RGU-seconds inside the window (rate x overlap),
    # keeping the unit allocated_gpu_cost had. Metric mean via a targeted
    # jobstatisticdb join (parametrized), not the view's frozen stat columns.
    js1 = aliased(JobStatisticDB)
    m1 = col(js1.mean)
    weight = (
        col(JobSeriesDB.allocated_rgu_drac)
        * _overlap_hours(JobSeriesDB, *window)
        * 3600.0
    )
    bin_width = 1.0 / _DENSITY_BINS

    # _apply_rgu_base_view anchors the FROM on the view, keeps only calculable-RGU
    # jobs and adds the common filters; then attach the stat alias on the job id.
    bin_expr = _density_bin_expr(m1).label("bin")
    q = (
        _apply_rgu_base_view(
            select(bin_expr, func.sum(weight).label("w")),
            sess,
            clusters,
            cluster_user,
            job_states,
            scope_user_id=_scope_or_view_as(sess, req, as_user),
        )
        .join(
            js1,
            and_(
                col(js1.job_id) == col(JobSeriesDB.job_db_id), col(js1.name) == metric
            ),
            isouter=True,
        )
        .where(_ran_between(JobSeriesDB, *window), _valid_metric_filter(m1))
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
    as_user: str | None = _AS_USER_QUERY,
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

    Counts GPU jobs that ran in the window into a 100x100 grid of (metric,
    metric2) mean values; a job contributes only if it carries both stats. No
    sampling: every job lands in exactly one cell (like the elapsed/wait
    heatmaps), and a count is not pro-rated. Returns {x, y, z} with z[iby][ibx]
    the job count of that cell (Plotly heatmap order).
    """
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")
    if metric2 not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric2!r}")

    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    window = (begin_dt.timestamp(), finish_dt.timestamp())

    js1 = aliased(JobStatisticDB)
    js2 = aliased(JobStatisticDB)
    m1 = col(js1.mean)
    m2 = col(js2.mean)

    bx = _density_bin_expr(m1, _PAIRED_BINS).label("bx")
    by = _density_bin_expr(m2, _PAIRED_BINS).label("by")
    # group_by by label, not by expression: pg8000's server-side binding renders
    # the expression with fresh placeholders in GROUP BY (error 42803).
    q = (
        _apply_rgu_base_view(
            select(bx, by, func.count().label("n")),
            sess,
            clusters,
            cluster_user,
            job_states,
            scope_user_id=_scope_or_view_as(sess, req, as_user),
        )
        .join(
            js1,
            and_(
                col(js1.job_id) == col(JobSeriesDB.job_db_id), col(js1.name) == metric
            ),
            isouter=True,
        )
        .join(
            js2,
            and_(
                col(js2.job_id) == col(JobSeriesDB.job_db_id), col(js2.name) == metric2
            ),
            isouter=True,
        )
        .where(
            _ran_between(JobSeriesDB, *window),
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
    as_user: str | None = _AS_USER_QUERY,
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric: str = Query(default="gpu_sm_occupancy"),
    min_usage: float = Query(default=0.15, ge=0.0, le=1.0),
    sess: Session = Depends(session_dep),
):
    """Allocated vs effectively-used RGU.h per time bucket.

    Over GPU jobs *running* in each ``period`` bucket, each charged only for the
    time it spent there: ``rgu_allocated`` = SUM(rgu * hours in the bucket);
    ``rgu_used`` = the same scaled by each job's mean ``metric`` (e.g.
    gpu_sm_occupancy); ``rgu_wasted`` = the per-job shortfall below ``min_usage``
    (SUM of rgu_hours * (min_usage - mean) over measured jobs with mean <
    min_usage).

    Returns ``{periods, overall}``, one ``periods`` row per bucket. Each row also
    carries ``metric_means``: the plain per-job mean of every _TREND_METRICS
    entry over the jobs running in that bucket. A job spanning several buckets has its RGU.h split across them,
    its mean counted whole in each -- so the sums recombine across buckets and
    the means do not. ``overall`` holds the same means over the whole window,
    counting each job once, for callers that need a range-wide figure.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)
    scope_user_id = _scope_or_view_as(sess, req, as_user)
    empty_means = {name: {"mean": None} for name in _TREND_METRICS}
    if _no_buckets(begin_dt, finish_dt):
        return {"periods": [], "overall": empty_means}

    # Per bucket: allocated = RGU rate x hours landing inside (_overlap_hours);
    # used = the same scaled by the metric mean. The metric is parametrized over
    # 7 values but the view's *_waste columns are frozen to gpu_sm_occupancy /
    # cpu_utilization, hence our own targeted jobstatisticdb join.
    buckets = _bucket_table(begin_dt, finish_dt, parsed)
    rgu_hours = col(JobSeriesDB.allocated_rgu_drac) * _overlap_hours(
        JobSeriesDB, buckets.c.bucket_start, buckets.c.bucket_end
    )
    # `metric` reads off the trend alias whenever it is one of them (the
    # default): joining jobstatisticdb again on the same (name, job_id) row
    # would buy nothing.
    trend = {name: aliased(JobStatisticDB) for name in _TREND_METRICS}
    m_mean = col(trend.get(metric, JobStatisticDB).mean)
    # Split used vs unmeasured on whether the metric is a real value (not
    # NULL/NaN); a missing measurement is kept apart from "unused" rather than
    # counted as waste.
    m_present = _is_real(m_mean)
    rgu_used_term = case((m_present, rgu_hours * m_mean), else_=0.0)
    rgu_unmeasured_term = case((m_present, 0.0), else_=rgu_hours)
    # Shortfall to min_usage per job: a job above the threshold contributes 0
    # (its surplus never offsets another job's deficit), so the SUM is additive
    # across regroupings -- per-period bars, the whole-range view and a period
    # change all tell the same story.
    rgu_wasted_term = case(
        (and_(m_present, m_mean < min_usage), rgu_hours * (min_usage - m_mean)),
        else_=0.0,
    )

    # Plain per-job mean of a trend metric, plotted over the bars.
    def _trend_avg(name: str):
        t_mean = col(trend[name].mean)
        return func.avg(case((_is_real(t_mean), t_mean))).label(f"{name}_mean")

    # One alias per trend metric: (name, job_id) is unique, so these stay 1:1 and
    # leave the SUMs untouched. Shared with the whole-range query below.
    def _join_trends(q):
        for name in _TREND_METRICS:
            alias = trend[name]
            q = q.join(
                alias,
                and_(
                    col(alias.job_id) == col(JobSeriesDB.job_db_id),
                    col(alias.name) == name,
                ),
                isouter=True,
            )
        return q

    query = _apply_rgu_base_view(
        select(
            buckets.c.bucket_index,
            func.sum(rgu_hours).label("rgu_allocated"),
            func.sum(rgu_used_term).label("rgu_used"),
            func.sum(rgu_unmeasured_term).label("rgu_unmeasured"),
            func.sum(rgu_wasted_term).label("rgu_wasted"),
            *[_trend_avg(name) for name in _TREND_METRICS],
        ),  # ty:ignore[no-matching-overload]
        sess,
        clusters,
        cluster_user,
        job_states,
        scope_user_id=scope_user_id,
    )
    if metric not in trend:
        query = query.join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(JobSeriesDB.job_db_id),
                col(JobStatisticDB.name) == metric,
            ),
            isouter=True,
        )
    query = _join_trends(query)
    # The join is also the time filter: a job overlapping no bucket is outside
    # the window. A job spanning several buckets yields one row per bucket, which
    # is what splits its RGU.h across them -- and what makes the trend means
    # above read as "over the jobs running in this bucket".
    query = (
        query.join(
            buckets,
            _ran_between(JobSeriesDB, buckets.c.bucket_start, buckets.c.bucket_end),
        )
        .group_by(buckets.c.bucket_index)
        .order_by(buckets.c.bucket_index)
    )

    sums = {}
    trends = {}
    for row in sess.exec(query):
        key = row.bucket_index
        sums[key] = (
            float(row.rgu_allocated or 0.0),
            float(row.rgu_used or 0.0),
            float(row.rgu_unmeasured or 0.0),
            float(row.rgu_wasted or 0.0),
        )
        trends[key] = {
            name: {"mean": _nan_to_none(getattr(row, f"{name}_mean"))}
            for name in _TREND_METRICS
        }

    # Whole-range means, taken over the jobs and not over the bucket rows: joined
    # to the buckets, a job crossing three of them is averaged three times, so
    # recombining the rows above weighs each job by the buckets it spans -- a
    # duration weighting nobody asked for, and one that moves with the period
    # selector. Its own query because no per-bucket aggregate can undo that.
    overall_q = _join_trends(
        _apply_rgu_base_view(
            select(*[_trend_avg(name) for name in _TREND_METRICS]),
            sess,
            clusters,
            cluster_user,
            job_states,
            scope_user_id=scope_user_id,
        )
    ).where(_ran_between(JobSeriesDB, begin_dt.timestamp(), finish_dt.timestamp()))
    overall_row = sess.exec(overall_q).one()
    overall = {
        name: {"mean": _nan_to_none(getattr(overall_row, f"{name}_mean"))}
        for name in _TREND_METRICS
    }

    period_data = []
    for key, (ps, pe) in enumerate(_iter_buckets(begin_dt, finish_dt, parsed)):
        allocated, used, unmeasured, wasted = sums.get(key, (0.0, 0.0, 0.0, 0.0))
        period_data.append(
            {
                "period_start": ps.strftime(fmt),
                "period_end": pe.strftime(fmt),
                "rgu_allocated": allocated,
                "rgu_used": used,
                "rgu_unmeasured": unmeasured,
                "rgu_wasted": wasted,
                "metric_means": trends.get(key, empty_means),
            }
        )

    return {"periods": period_data, "overall": overall}


@router.get("/metrics/rgu_by_cluster")
def metrics_rgu_by_cluster(
    req: Requestor = Depends(requestor),
    as_user: str | None = _AS_USER_QUERY,
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    cluster_user: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    sess: Session = Depends(session_dep),
):
    """Total RGU.h per period, stacked by cluster.

    Aggregates the same pro-rated RGU metric as /rgu_usage, grouped by
    cluster_name. When ``clusters`` is given, only those clusters are kept (empty
    = all clusters). Returns one series per cluster, aligned on a shared period
    axis; clusters with no RGU at all (e.g. no billing) are dropped.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)
    if _no_buckets(begin_dt, finish_dt):
        return {"periods": [], "series": []}

    # The view carries cluster_name and allocated_rgu_drac (the per-job RGU rate,
    # allocated_gres_gpu * drac_rgu), so no clusters/gpurgudb join is needed.
    bucket_table = _bucket_table(begin_dt, finish_dt, parsed)
    rgu_hours = col(JobSeriesDB.allocated_rgu_drac) * _overlap_hours(
        JobSeriesDB, bucket_table.c.bucket_start, bucket_table.c.bucket_end
    )
    query = _apply_rgu_base_view(
        select(
            bucket_table.c.bucket_index,
            col(JobSeriesDB.cluster_name).label("cluster_name"),
            func.sum(rgu_hours).label("rgu"),
        ),
        sess,
        clusters,
        cluster_user,
        job_states,
        scope_user_id=_scope_or_view_as(sess, req, as_user),
    )
    query = (
        query.join(
            bucket_table,
            _ran_between(
                JobSeriesDB, bucket_table.c.bucket_start, bucket_table.c.bucket_end
            ),
        )
        .group_by(bucket_table.c.bucket_index, "cluster_name")
        .order_by(bucket_table.c.bucket_index)
    )

    sums = {}
    totals = {}
    for r in sess.exec(query):
        if not r.cluster_name:
            continue
        v = float(r.rgu or 0.0)
        sums[(r.bucket_index, r.cluster_name)] = v
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
            for ps, pe in buckets
        ],
        "series": [
            {"cluster": c, "rgu": [sums.get((i, c), 0.0) for i in range(len(buckets))]}
            for c in stacked_clusters
        ],
    }


@router.get("/metrics/metric_trend")
def metrics_metric_trend(
    req: Requestor = Depends(requestor),
    as_user: str | None = _AS_USER_QUERY,
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

    For each period bucket, averages the per-job statistic values over the jobs
    *running* in that bucket -- plain per-job average, not duration weighted, so
    a job spanning several buckets counts once in each. Jobs lacking the
    statistic are simply absent from the average (inner join) and no GPU/RGU
    filter is applied, so system metrics also cover CPU-only jobs. Returns a
    single ``series`` entry (the requested metric) on a period axis; buckets with
    no data yield null (a curve gap), not 0.
    """
    if metric not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric!r}")

    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)
    if _no_buckets(begin_dt, finish_dt):
        return {"periods": [], "series": [{"metric": metric, "mean": [], "max": []}]}

    # No GPU filter, unlike the RGU endpoints: ``metric`` is user-selected and
    # not always GPU-bound (system_memory is also measured on CPU-only jobs).
    # The DB view would then degrade to a full-table scan (see JobSeriesDB
    # docstring); job_series_select keeps it narrow, on ix_slurm_jobs_run.
    js = job_series_select(
        "job_db_id",
        "start_time",
        "elapsed_time",
        "cluster_id",
        "cluster_user",
        "job_state",
        "sarc_user_id",
    ).subquery()
    bucket_table = _bucket_table(begin_dt, finish_dt, parsed)
    m_mean = col(JobStatisticDB.mean)
    m_max = col(JobStatisticDB.max)
    # NaN-proof averages: a single NaN would contaminate the whole AVG, so each
    # value is nulled out unless it is real (`x == x` won't do this on Postgres,
    # where NaN = NaN is TRUE — see _is_real).
    avg_mean = func.avg(case((_is_real(m_mean), m_mean))).label("avg_mean")
    avg_max = func.avg(case((_is_real(m_max), m_max))).label("avg_max")

    query = (
        select(bucket_table.c.bucket_index, avg_mean, avg_max)
        .select_from(js)
        .join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == js.c.job_db_id,
                col(JobStatisticDB.name) == metric,
            ),
        )
        .join(
            bucket_table,
            _ran_between(js.c, bucket_table.c.bucket_start, bucket_table.c.bucket_end),
        )
        .group_by(bucket_table.c.bucket_index)
        .order_by(bucket_table.c.bucket_index)
    )
    query = _apply_job_filters(
        query,
        js.c,
        _resolve_cluster_ids(sess, clusters),
        cluster_user,
        job_states,
        _scope_or_view_as(sess, req, as_user),
    )

    cells = {}
    for r in sess.exec(query):
        cells[r.bucket_index] = (_nan_to_none(r.avg_mean), _nan_to_none(r.avg_max))

    buckets = list(_iter_buckets(begin_dt, finish_dt, parsed))
    return {
        "periods": [
            {"period_start": ps.strftime(fmt), "period_end": pe.strftime(fmt)}
            for ps, pe in buckets
        ],
        "series": [
            {
                "metric": metric,
                "mean": [cells.get(i, (None, None))[0] for i in range(len(buckets))],
                "max": [cells.get(i, (None, None))[1] for i in range(len(buckets))],
            }
        ],
    }


@router.get("/metrics/rgu_by_user")
def metrics_rgu_by_user(
    req: Requestor = Depends(requestor),
    as_user: str | None = _AS_USER_QUERY,
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

    Same pro-rated RGU.h measure as /rgu_usage, summed per cluster_user
    (requested = SUM(rgu * hours in the window); used = scaled by the mean
    ``metric``). The window is one bucket here, so the totals match what the
    per-bucket plots add up to. Sorted by descending requested RGU.h.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    window = (begin_dt.timestamp(), finish_dt.timestamp())

    # Aggregate by user: RGU rate x hours spent inside the window. Metric mean
    # via a targeted jobstatisticdb join (parametrized) — see rgu_usage.
    rgu_hours = col(JobSeriesDB.allocated_rgu_drac) * _overlap_hours(
        JobSeriesDB, *window
    )
    m_mean = col(JobStatisticDB.mean)
    # Split used vs unmeasured on whether the metric is a real value (not
    # NULL/NaN); a missing measurement is kept apart from "unused".
    m_present = _is_real(m_mean)
    rgu_used_term = case((m_present, rgu_hours * m_mean), else_=0.0)
    rgu_unmeasured_term = case((m_present, 0.0), else_=rgu_hours)
    user_expr = func.coalesce(col(JobSeriesDB.cluster_user), "unknown").label("user")
    rgu_requested_sum = func.sum(rgu_hours).label("rgu_requested")

    query = _apply_rgu_base_view(
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
        scope_user_id=_scope_or_view_as(sess, req, as_user),
    )
    query = (
        query.join(
            JobStatisticDB,
            and_(
                col(JobStatisticDB.job_id) == col(JobSeriesDB.job_db_id),
                col(JobStatisticDB.name) == metric,
            ),
            isouter=True,
        )
        .where(_ran_between(JobSeriesDB, *window))
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
    as_user: str | None = _AS_USER_QUERY,
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

    Lists GPU jobs that ran in the window (cluster/user/state filtered), one row
    per job: cluster, user, state, elapsed, GPU counts, billing, gpu_type, rgu,
    rgu_hours, per-job metric means and ``waste`` (rgu_hours * (1 - mean)).
    ``rgu_hours`` counts only the hours inside the window, like the plots, while
    ``elapsed`` still reports the job's whole run. Sorted by
    ``sort_by``/``sort_dir`` and paginated by ``limit``/``offset``. Returns
    {total, jobs}. ``total`` is the full filtered count, computed by a SEPARATE
    query (kept out of the page query so the page parallelises — see below) and
    only when ``include_total`` is set (None otherwise). The frontend requests it
    on every page so the count and page numbers stay current as scraping adds
    jobs; the separate query stays cheap precisely because it parallelises.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    window = (begin_dt.timestamp(), finish_dt.timestamp())

    # Limit-first pagination. A `page` subquery ranks, paginates and counts the
    # full filtered set from the source alone (plus the single stat the sort needs,
    # if any); the outer query then joins the 3 display stats back onto just that
    # page of rows. Without this split, the 3 stat joins + count(*) would run over
    # the whole window (millions of rows) just to return 50. See the perf note in
    # docs / the /metrics/jobs investigation.
    # Pro-rated like the plots; the rgu_hours and waste sorts rank on it too.
    rgu_hours_raw = col(JobSeriesDB.allocated_rgu_drac) * _overlap_hours(
        JobSeriesDB, *window
    )

    # One aliased jobstatisticdb row per distinct stat name, LEFT-joined on the
    # job id. Not the view's own gpu_*_mean/max columns: those would join over the
    # whole window, while the page subquery below joins the stats onto the page only.
    stat_names = {metric, "gpu_utilization", "gpu_sm_occupancy", "gpu_memory"}
    js = {name: aliased(JobStatisticDB) for name in sorted(stat_names)}
    metric_mean_raw = col(js[metric].mean)

    def _join_stat(query, name: str):
        alias = js[name]
        return query.join(
            alias,
            and_(
                col(alias.job_id) == col(JobSeriesDB.job_db_id), col(alias.name) == name
            ),
            isouter=True,
        )

    # Sortable columns -> ORDER BY expression. Raw (unlabelled) so they compose
    # with nulls_last/asc/desc cleanly. `nodes` is an array and is not sortable,
    # so it is intentionally absent. cluster_name/rgu come straight from the view;
    # the keys in `sort_needs_stat` need their stat alias, so the page joins just
    # that one; every other key ranks on the view (index-only) alone.
    sortable = {
        "cluster": col(JobSeriesDB.cluster_name),
        "job_id": col(JobSeriesDB.job_id),
        "submit_time": col(JobSeriesDB.submit_time),
        "user": col(JobSeriesDB.cluster_user),
        "job_state": col(JobSeriesDB.job_state),
        "elapsed": col(JobSeriesDB.elapsed_time),
        "requested_gpu": col(JobSeriesDB.requested_gres_gpu),
        "allocated_gpu": col(JobSeriesDB.allocated_gres_gpu),
        "billing": col(JobSeriesDB.allocated_billing),
        "gpu_type": func.coalesce(
            col(JobSeriesDB.harmonized_gpu_type), col(JobSeriesDB.allocated_gpu_type)
        ),
        "gpu_type_rgu": col(JobSeriesDB.gpu_type_rgu_drac),
        "rgu": col(JobSeriesDB.allocated_rgu_drac),
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
    # nulls_last only for keys nullable in the result set: the LEFT-joined stats
    # and the nullable gpu/billing cols. On a NOT NULL indexed key like submit_time
    # it defeats the index -- DESC NULLS LAST matches neither the btree nor its
    # reverse scan, forcing Seq Scan + Sort. Sorting by id last breaks ties between
    # equal keys, so offset paging never skips or repeats a row.
    nullable_sorts = {
        "requested_gpu",
        "allocated_gpu",
        "billing",
        "waste",
        "gpu_utilization_mean",
        "gpu_sm_occupancy_mean",
        "gpu_memory_max",
    }
    if sort_by in nullable_sorts:
        ordered = nulls_last(ordered)
    order_by = (ordered, col(JobSeriesDB.job_db_id))

    # Window only; the gpu_type/RGU validity filter now lives in _apply_rgu_base_view.
    # "Ran in the window", so the table lists the jobs the plots are drawn from.
    base_filters = (_ran_between(JobSeriesDB, *window),)

    scope_user_id = _scope_or_view_as(sess, req, as_user)

    # COUNT: the full filtered total, computed by its own query and only when
    # asked (include_total). The frontend requests it on every page so the count
    # and page numbers stay current as scraping adds jobs. It is deliberately kept
    # OUT of the page query below: a `count(*) OVER ()` there forces the whole
    # filtered set to be materialised AND disables parallelism, so every page would
    # pay the full-set cost. Isolated like this the count parallelises (and needs
    # neither the clusters nor the stat join), so paying it per page stays cheap.
    total: int | None = None
    if include_total:
        count_q = _apply_rgu_base_view(
            select(func.count()),
            sess,
            clusters,
            cluster_user,
            job_states,
            scope_user_id=scope_user_id,
        ).where(*base_filters)
        total = int(sess.exec(count_q).one())

    # PAGE: the page's job ids only. The scan/sort runs here on the view alone
    # (+ the sort's stat alias when needed). With no window count it parallelises,
    # and a small offset top-N heapsorts instead of sorting the whole set.
    page_q = _apply_rgu_base_view(
        select(col(JobSeriesDB.job_db_id).label("jid")),
        sess,
        clusters,
        cluster_user,
        job_states,
        scope_user_id=scope_user_id,
    )
    # Stat alias only for stat sorts; cluster_name is a view column, so the
    # cluster sort needs no extra join.
    if sort_by in sort_needs_stat:
        page_q = _join_stat(page_q, sort_needs_stat[sort_by])
    page = (
        page_q.where(*base_filters).order_by(*order_by).offset(offset).limit(limit)
    ).subquery()

    # FINAL: display columns + the 3 stats, fetched only for the page's rows
    # (joined back on the job id). The total comes from the separate count above.
    query = _apply_rgu_base_view(
        select(  # ty:ignore[no-matching-overload]
            col(JobSeriesDB.cluster_name).label("cluster_name"),
            col(JobSeriesDB.job_id),
            col(JobSeriesDB.submit_time).label("submit_time"),
            col(JobSeriesDB.cluster_user),
            col(JobSeriesDB.job_state),
            col(JobSeriesDB.elapsed_time).label("elapsed_time"),
            col(JobSeriesDB.nodes),
            col(JobSeriesDB.requested_gres_gpu),
            col(JobSeriesDB.allocated_gres_gpu),
            col(JobSeriesDB.allocated_billing),
            col(JobSeriesDB.allocated_gpu_type).label("allocated_gpu_type"),
            col(JobSeriesDB.harmonized_gpu_type),
            col(JobSeriesDB.gpu_type_rgu_drac).label("gpu_type_rgu_drac"),
            col(JobSeriesDB.allocated_rgu_drac).label("rgu"),
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
        scope_user_id=scope_user_id,
    ).join(page, page.c.jid == col(JobSeriesDB.job_db_id))
    for name in js:
        query = _join_stat(query, name)
    query = query.order_by(*order_by)

    jobs = []
    for row in sess.exec(query):
        mm = _nan_to_none(row.metric_mean)
        # Non-NULL for every row the filters let through, but still guarded: a
        # None here is a blank cell in the frontend rather than a crash.
        rh = _nan_to_none(row.rgu_hours)
        waste = round(rh * (1 - mm), 2) if (rh is not None and mm is not None) else None
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
                "rgu_hours": round(rh, 2) if rh is not None else None,
                "waste": waste,
                # Selected-metric mean (None when unmeasured): drives the
                # job-table row shading.
                "metric_mean": mm,
                "gpu_utilization_mean": _nan_to_none(
                    row.gpu_utilization_mean, replace_with=-1
                ),
                "gpu_sm_occupancy_mean": _nan_to_none(
                    row.gpu_sm_occupancy_mean, replace_with=-1
                ),
                "gpu_memory_max": _nan_to_none(row.gpu_memory_max),
            }
        )

    return {"total": total, "jobs": jobs}
