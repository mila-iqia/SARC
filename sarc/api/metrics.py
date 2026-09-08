import hashlib
import math
import re
from collections.abc import Generator, Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import (
    ARRAY,
    Float,
    Integer,
    literal,
    literal_column,
    nulls_last,
    text,
    true,
)
from sqlalchemy.sql.elements import Grouping
from sqlmodel import Session, and_, case, col, func, select

from sarc.api.v0 import Requestor, requestor
from sarc.config import config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import SlurmJobDB
from sarc.db.job_series import DASH_STATS, JobSeriesTable
from sarc.db.users import MatchingID, UserDB
from sarc.models.job import SlurmState

# The /dash GPU population: the job_series table covers every job, so every
# panel narrows to the subset with a computable RGU -- exactly the predicate
# the GPU-partial covering indexes (ix_job_series_end/user/submit) carry, so
# the planner can prove the index applies.
DASH_ELIGIBILITY = (
    col(JobSeriesTable.allocated_gres_gpu) > 0,
    col(JobSeriesTable.gpu_type_rgu_drac).is_not(None),
)


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


# Postgres defaults to 4MB. The window scans hash-aggregate a few thousand
# groups at most, but the distribution/heatmap bins sort mid-sized partials,
# and spilling those to disk cost more than the whole scan did. Local to this
# request's transaction, so pooled connections never carry it to /v0.
_WORK_MEM = "32MB"


def session_dep() -> Generator[Session]:
    with config.db.session() as sess:
        # LOCAL so it dies with this request's transaction instead of riding
        # the pooled connection into the next one (/v0 shares this engine).
        sess.connection().execute(text(f"SET LOCAL work_mem = '{_WORK_MEM}'"))
        yield sess


UTC = timezone.utc

_DEFAULT_WINDOW_DAYS = 1
_DEFAULT_PERIOD = "w"
# Dashboard date-range dropdown default: a rolling 6-week window ending
# today. Purely client-side (resolved to concrete start/end dates in the
# browser before any API call), so this only seeds the template.
_DEFAULT_RANGE = "last_6w"

# The one GPU statistic the dashboard reads as "used": it splits Used vs Unused
# RGU, shades the job table and draws every trend. Frozen rather than a request
# parameter; sarc/db/job_series.py freezes the same choice in `usage_metric`.
_USAGE_METRIC_NAME = "gpu_sm_occupancy"

# GPU/system metrics (stored per-job in JobStatisticDB) normalized to [0, 1]
_METRICS_0_1: set[str] = {
    "gpu_sm_occupancy",
    "gpu_utilization",
    "gpu_utilization_fp16",
    "gpu_utilization_fp32",
    "gpu_utilization_fp64",
    "gpu_memory",
    "system_memory",
}


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
    step = timedelta(days=float(num) * _PERIOD_MULTIPLIERS[unit])
    if not step:
        raise HTTPException(
            status_code=400, detail=f"Invalid period {s!r}. It must be positive."
        )
    return step


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


def _iter_buckets(
    begin_dt: datetime, finish_dt: datetime, period: timedelta | str
) -> Iterator[tuple[datetime, datetime]]:
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
    # Return a valid focus, or an empty window.
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


def _job_end(cols):
    """The instant the job's run stopped, through the indexed SQL function."""
    return func.slurm_job_end(cols.start_time, cols.elapsed_time)


def _job_span(cols):
    """A job's run as epoch seconds: ``[start, start + elapsed)``.

    Anchored on ``elapsed_time``, not ``end_time``, so the slices add back up to
    the view's cost columns, which are all built from elapsed.
    """
    start = func.extract("epoch", cols.start_time)
    return start, start + cols.elapsed_time


def _ran_between(cols, lo, hi):
    """SQL predicate: the job was running at some point within ``[lo, hi)``.

    Bounds are epoch seconds -- either Python floats for a fixed window, or the
    bucket bounds of ``_bucket_table``.

    Two comparisons over ``ix_slurm_jobs_end_gpu``, which indexes the end (see
    ``slurm_job_end`` in sarc/db/job.py); any other spelling reads every row.
    The degenerate cases are the function's NULL: a job that never started, and
    a run of no length. Half-open on both sides, so a run closing exactly on
    ``lo`` is out, and one opening exactly on ``hi`` too.
    """
    return and_(
        _job_end(cols) > func.to_timestamp(lo), cols.start_time < func.to_timestamp(hi)
    )


def _overlap_hours(cols, lo, hi):
    """SQL expression: hours of the job's run that fall inside ``[lo, hi)``.

    Guard it or it lies: ``least``/``greatest`` ignore NULL, so a job that
    never started would take the full width of ``[lo, hi)``, and one running
    outside the bounds a negative width -- nothing clamps either. The window is
    guarded by ``_ran_between``, each bucket by ``_bucket_table``.
    """
    start, end = _job_span(cols)
    return (func.least(end, hi) - func.greatest(start, lo)) / 3600.0


def _bucket_bounds(
    begin_dt: datetime, finish_dt: datetime, period: timedelta | str
) -> tuple[Grouping, Grouping]:
    """The bucket bounds as two SQL arrays, for ``width_bucket`` to search.

    Carries the bounds ``_iter_buckets`` already computes into SQL as epoch
    seconds: one definition of where a bucket starts, and no ``date_trunc``
    whose result depends on the session TimeZone. Buckets are identified by
    position, matched back against the caller's own ``_iter_buckets`` list.

    Arrays rather than a VALUES list: the Bind message counts parameters on an
    Int16, so three columns per bucket capped the endpoint at 65535/3 = 21845
    buckets (pg8000 raised before the server saw anything). Two parameters
    whatever the count, same plan, fixed-size statement.
    """
    bounds = list(_iter_buckets(begin_dt, finish_dt, period))
    # Grouping: without the parentheses Postgres reads `$1::float8[][pos]` as a
    # two-dimensional array type and hands back the whole array, silently.
    return (
        Grouping(literal([ps.timestamp() for ps, _ in bounds], ARRAY(Float))),
        Grouping(literal([pe.timestamp() for _, pe in bounds], ARRAY(Float))),
    )


def _submitted_bucket(
    cols, begin_dt: datetime, finish_dt: datetime, period: timedelta | str
):
    """The bucket a job was submitted in, as a ``bucket_index`` expression.

    The counterpart of ``_bucket_table`` for a plot that reads submissions: a
    submit instant falls in exactly one bucket, so there is nothing to expand and
    no LATERAL to join -- the same binary search over the same bounds, run once
    per job. The search is unbounded: a submit before the window gives -1, and
    one at or after it folds silently into the last bucket. The caller's
    ``submit_time`` filter is what keeps both out.
    """
    starts, _ = _bucket_bounds(begin_dt, finish_dt, period)
    # width_bucket numbers from 1; the caller indexes _iter_buckets from 0.
    return func.width_bucket(func.extract("epoch", cols.submit_time), starts) - 1


def _bucket_table(
    cols, begin_dt: datetime, finish_dt: datetime, period: timedelta | str
):
    """The buckets a job's run touches, as a LATERAL to join it against.

    ``width_bucket`` binary-searches the starts for the first and last bucket
    the run reaches, and ``generate_series`` walks that span, so a run expands
    to the buckets it covers instead of being tested against every one of them.
    Past either end of the array ``starts[pos]`` reads NULL, which the WHERE
    drops -- nothing needs clamping. A run with no start searches on NULL and
    expands to no bucket at all, which is what it should get.

    The overlap is spelled here in epoch seconds rather than through
    ``_ran_between``, which would re-derive an end this search already holds.
    The two must stay in step: a job with no start yields nothing, one with no
    length is excluded (``elapsed_time > 0``), and the bounds are half-open.

    This picks the buckets, *not* the jobs: pair it with ``_ran_between`` over
    the whole window, which is the half an index can answer.
    """
    starts, ends = _bucket_bounds(begin_dt, finish_dt, period)
    run_start, run_end = _job_span(cols)
    # width_bucket numbers from 1, so position - 1 is the caller's index.
    pos = func.generate_series(
        func.width_bucket(run_start, starts), func.width_bucket(run_end, starts)
    ).column_valued("pos")
    return (
        select(
            (pos - 1).label("bucket_index"),
            starts[pos].label("bucket_start"),
            ends[pos].label("bucket_end"),
        )
        .where(starts[pos] < run_end, ends[pos] > run_start, cols.elapsed_time > 0)
        .lateral("buckets")
    )


def _no_buckets(begin_dt: datetime, finish_dt: datetime) -> bool:
    """Return True for an empty window (start == end)."""
    return begin_dt >= finish_dt


def _uniform_plan(
    begin_dt: datetime, finish_dt: datetime, period: timedelta | str
) -> tuple[float, float, int] | None:
    """(anchor_epoch, step_seconds, n) when the buckets are evenly spaced, else None.

    Fixed periods are uniform by construction, and so are the calendar h/d/w
    (UTC has no DST gaps) -- only calendar months are not, and a single bucket
    is trivially uniform. The grid is rebuilt from the period itself (not from
    the clipped bounds, whose first interval is shortened when ``begin`` falls
    mid-bucket), and every bucket is checked against it. Clipping only ever
    touches the first and last bucket (the bounds of the requested range), so
    the window predicate plus a clamp to [0, n-1] keeps the arithmetic
    expansion identical to the array search of ``_bucket_table``.
    """
    bounds = list(_iter_buckets(begin_dt, finish_dt, period))
    if not bounds:
        return None
    if len(bounds) == 1:
        s, e = bounds[0]
        return s.timestamp(), max((e - s).total_seconds(), 1e-9), 1
    if isinstance(period, timedelta):
        grid = begin_dt
        step = period.total_seconds()
    else:
        grid = _calendar_trunc(begin_dt, period)
        step = (_calendar_next(grid, period) - grid).total_seconds()
    if step <= 0:
        return None
    for i, (s, e) in enumerate(bounds):
        if s != max(grid + timedelta(seconds=i * step), begin_dt) or e != min(
            grid + timedelta(seconds=(i + 1) * step), finish_dt
        ):
            return None
    return grid.timestamp(), step, len(bounds)


def _uniform_bucket_table(cols, plan: tuple[float, float, int], lo, hi):
    """The buckets a job's run touches, as a LATERAL -- evenly spaced case.

    The counterpart of ``_bucket_table`` without the array binary search: with
    a uniform grid the bucket index of an instant is one floor/ceil, so a run
    expands to ``generate_series(floor(start), ceil(end) - 1)`` clamped to
    ``[0, n-1]``. Bucket bounds come back as arithmetic on the grid
    (clipped to the window at both ends), so ``_overlap_hours`` reads the same
    three column names either way. Semantics match ``_bucket_table`` exactly:
    half-open on both ends, a run with no start or no length expands to no
    bucket, and the clamps are what the window predicate already guarantees.
    """
    anchor, step, n = plan
    start, end = _job_span(cols)
    k = func.generate_series(
        func.greatest(func.floor((start - anchor) / step), 0).cast(Integer),
        func.least(func.ceil((end - anchor) / step) - 1, n - 1).cast(Integer),
    ).column_valued("k")
    return (
        select(
            k.label("bucket_index"),
            func.greatest(anchor + k * step, lo).label("bucket_start"),
            func.least(anchor + (k + 1) * step, hi).label("bucket_end"),
        )
        .where(cols.elapsed_time > 0)
        .lateral("buckets")
    )


def _dash_bucket_table(cols, begin_dt: datetime, finish_dt: datetime, period):
    """``_bucket_table`` with the uniform-grid fast path picked automatically.

    The array/width_bucket spelling is general but pays two binary searches
    and array subscripts per job; the arithmetic one is a floor, a ceil and two
    multiplications. Same rows out either way (see ``_uniform_plan``).
    """
    lo, hi = begin_dt.timestamp(), finish_dt.timestamp()
    plan = _uniform_plan(begin_dt, finish_dt, period)
    if plan is not None:
        return _uniform_bucket_table(cols, plan, lo, hi)
    return _bucket_table(cols, begin_dt, finish_dt, period)


def _dash_submitted_bucket(cols, begin_dt: datetime, finish_dt: datetime, period):
    """``_submitted_bucket`` with the uniform-grid fast path (see above)."""
    plan = _uniform_plan(begin_dt, finish_dt, period)
    if plan is None:
        return _submitted_bucket(cols, begin_dt, finish_dt, period)
    anchor, step, n = plan
    idx = func.floor((func.extract("epoch", cols.submit_time) - anchor) / step).cast(
        Integer
    )
    return func.least(func.greatest(idx, 0), n - 1)


def _dash_stat_col(metric: str, kind: Literal["mean", "max"] = "mean"):
    """The pivoted job_series column holding one statistic (see DASH_STATS)."""
    mean_col, max_col = DASH_STATS[metric]
    return getattr(JobSeriesTable, mean_col if kind == "mean" else max_col)


def _nan_to_none(
    v: float | None, replace_with: float | int | None = None
) -> float | None:
    return replace_with if (isinstance(v, float) and math.isnan(v)) else v


def _weighted_mean_cols(value_expr, weight_expr, label_prefix: str):
    """SUM(weight * value) and SUM(weight) SQL columns, the two halves a
    weighted average needs, each gated on ``value_expr`` being a real
    (non-NULL/NaN) value -- a row that fails this contributes to neither, not
    just to a zero-weighted term. CASE with no ``else_`` is SQL NULL, which SUM
    (unlike AVG) does not skip on its own, hence the explicit 0.0.
    """
    is_real = _is_real(value_expr)
    num = func.sum(case((is_real, weight_expr * value_expr), else_=0.0))
    den = func.sum(case((is_real, weight_expr), else_=0.0))
    return num.label(f"{label_prefix}_num"), den.label(f"{label_prefix}_den")


def _weighted_mean(num: float, den: float) -> float | None:
    """The ratio of the two ``_weighted_mean_cols`` sums, or None when the
    weight is <= 0 -- nothing had a real value, excluded entirely rather than
    read as a 0 that would look measured."""
    return num / den if den > 0 else None


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


def _resolve_user_ids(sess: Session, email: str | None) -> list[int] | None:
    """User ids carrying this exact email (resolved from users, the source of
    truth for the copied ``job_series.email``), once up front so the window
    scans filter on the indexed ``sarc_user_id`` the covering indexes key on.
    None: no filter. An empty list is a filter that matches no job (unknown
    email)."""
    if email is None:
        return None
    # The actual DB can never return None for UserDB.id
    return list(sess.exec(select(col(UserDB.id)).where(UserDB.email == email)).all())  # ty: ignore[invalid-return-type]


def _apply_job_filters(
    query,
    cols,
    cluster_ids: list[int] | None,
    user_ids: list[int] | None,
    job_states: list[str],
    scope_user_id: int | Literal["admin"],
):
    """Common job filters on ``cols`` -- job_series columns.

    Filters by cluster_ids (resolved upfront, no clusters join needed); empty
    means no cluster filter. ``user_ids`` (from a user_email filter) restricts
    to those users' jobs. ``scope_user_id`` (a non-admin's UserDB id) restricts
    to that user's jobs; the sentinel ``"admin"`` applies no scoping.
    """
    if job_states:
        query = query.where(cols.job_state.in_(job_states))
    if cluster_ids:
        query = query.where(cols.cluster_id.in_(cluster_ids))
    if user_ids is not None:
        query = query.where(col(cols.sarc_user_id).in_(user_ids or [-1]))
    if scope_user_id != "admin":
        query = query.where(cols.sarc_user_id == scope_user_id)
    return query


def _apply_dash_base(
    query,
    cluster_ids: list[int] | None,
    user_ids: list[int] | None,
    job_states: list[str],
    *,
    scope_user_id: int | Literal["admin"],
):
    """The shared base of every /dash query: select from ``job_series`` with the
    common filters.

    The GPU/RGU population the old ``_gpu_only`` predicate carved out is the
    table's ``DASH_ELIGIBILITY`` subset (and the predicate the GPU-partial
    covering indexes are keyed on); ``allocated_rgu_drac`` is the per-job RGU
    rate, trigger-maintained where the view computed it via gpurgudb. The
    statistics ride in the scan's own columns (``_dash_stat_col``), not in
    per-job jobstatisticdb joins.

    Takes resolved cluster ids and user ids, like ``_apply_job_filters``: the
    404 on an unknown name is the caller's to raise, before any early return of
    its own.
    """
    return _apply_job_filters(
        query.select_from(JobSeriesTable).where(*DASH_ELIGIBILITY),
        JobSeriesTable,
        cluster_ids,
        user_ids,
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
            "default_range": _DEFAULT_RANGE,
            "usage_metric": _USAGE_METRIC_NAME,
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
    user_email: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    submitted: bool = Query(default=False),
    sess: Session = Depends(session_dep),
):
    """Job count per time bucket, of the jobs running or submitted.

    Returns one {period_start, period_end, count} per ``period`` bucket of the
    window, empty buckets reported as 0, after the cluster/user/state filters.

    Default -- the jobs *running* in each bucket. A count is not an integral over
    time, so nothing is pro-rated: a job spanning three buckets is one job in
    each, and the counts do not add up to a number of distinct jobs. It reads as
    occupancy, and is the population every other plot draws from.

    ``submitted=true`` -- the jobs *submitted* in each bucket. Each job lands in
    exactly one, so these counts do add up, and they read as a submission rate.
    The window then selects on ``submit_time``, so this is deliberately not the
    population of the other plots: a job submitted in the window may run outside
    it, and one running in it may have been submitted long before.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)
    scope_user_id = _scope_or_view_as(sess, req, as_user)
    cluster_ids = _resolve_cluster_ids(sess, clusters)
    user_ids = _resolve_user_ids(sess, user_email)
    if _no_buckets(begin_dt, finish_dt):
        return []

    if submitted:
        bucket_index = _dash_submitted_bucket(
            JobSeriesTable, begin_dt, finish_dt, parsed
        ).label("bucket_index")
        query = select(bucket_index, func.count().label("count")).where(
            col(JobSeriesTable.submit_time) >= begin_dt,
            col(JobSeriesTable.submit_time) < finish_dt,
        )
    else:
        bucket_table = _dash_bucket_table(JobSeriesTable, begin_dt, finish_dt, parsed)
        bucket_index = bucket_table.c.bucket_index
        query = (
            select(bucket_index, func.count().label("count"))
            .join_from(JobSeriesTable, bucket_table, true())
            # The window filter, which the bucket LATERAL does not do: it splits
            # the jobs it is handed, it does not choose them.
            .where(
                _ran_between(
                    JobSeriesTable, begin_dt.timestamp(), finish_dt.timestamp()
                )
            )
        )

    # DASH_ELIGIBILITY, like every other /dash query: job_series covers all
    # jobs, and the counts panels read the same GPU population as the rest.
    query = _apply_job_filters(
        query.where(*DASH_ELIGIBILITY),
        JobSeriesTable,
        cluster_ids,
        user_ids,
        job_states,
        scope_user_id,
    )
    # By output column name, not by the expression again: rendered twice, the
    # submitted-mode expression gets a second set of placeholders for the bucket
    # bounds, and Postgres does not match $6 against $1 as one expression.
    key = literal_column("bucket_index")
    query = query.group_by(key).order_by(key)

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
    user_email: str | None = Query(default=None),
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

    This endpoint is selected on submit_time, not on the run: both grids measure
    how well a job guessed its limit, and neither the queue wait nor the whole
    elapsed belongs to a slice of time.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    cluster_ids = _resolve_cluster_ids(sess, clusters)
    user_ids = _resolve_user_ids(sess, user_email)

    # job_series carries every column these grids read, and ix_job_series_submit
    # covers them: the window is an index-only range scan, no joins.
    wait_expr = func.extract(
        "epoch", col(JobSeriesTable.start_time) - col(JobSeriesTable.submit_time)
    )
    # start_time spelled out: no STRICT slurm_job_end to imply it here.
    base_filters = [
        *DASH_ELIGIBILITY,
        col(JobSeriesTable.submit_time) >= begin_dt,
        col(JobSeriesTable.submit_time) < finish_dt,
        col(JobSeriesTable.time_limit).is_not(None),
        col(JobSeriesTable.start_time).is_not(None),
    ]
    if cluster_ids:
        base_filters.append(col(JobSeriesTable.cluster_id).in_(cluster_ids))
    if user_ids is not None:
        base_filters.append(col(JobSeriesTable.sarc_user_id).in_(user_ids or [-1]))
    if job_states:
        base_filters.append(col(JobSeriesTable.job_state).in_(job_states))
    scope_user_id = _scope_or_view_as(sess, req, as_user)
    if scope_user_id != "admin":
        base_filters.append(col(JobSeriesTable.sarc_user_id) == scope_user_id)

    max_l, max_e, max_w = sess.exec(
        select(
            func.max(col(JobSeriesTable.time_limit)).label("max_l"),
            func.max(col(JobSeriesTable.elapsed_time)).label("max_e"),
            func.max(wait_expr).label("max_w"),
        ).where(*base_filters)
    ).one()

    if max_l is None:
        # No matching rows
        return {"elapsed_vs_limit": None, "wait_vs_limit": None, "total_jobs": 0}

    elapsed_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        col(JobSeriesTable.time_limit),
        col(JobSeriesTable.elapsed_time),
        float(max_l),
        float(max_e),
    )
    wait_hmap = _build_heatmap_payload(
        sess,
        base_filters,
        col(JobSeriesTable.time_limit),
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
    user_email: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Duration-weighted distribution of the usage metric.

    Over GPU jobs running in the window, bins each job's mean value into 50 bins
    weighted by the RGU-seconds it spent *inside* the window, so long/big jobs
    count more and a job running past the window edge weighs only for the part
    inside. Returns {primary: {values, weights}}. The paired heatmap against a
    second metric is a separate endpoint, /metrics/metric_comparison.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    window = (begin_dt.timestamp(), finish_dt.timestamp())
    user_ids = _resolve_user_ids(sess, user_email)

    # Weight = RGU-seconds inside the window (rate x overlap), keeping the unit
    # allocated_gpu_cost had. The usage metric is a pivoted job_series column
    # (_dash_stat_col) -- the same values the old targeted jobstatisticdb join
    # returned, without the join.
    m1 = _dash_stat_col(_USAGE_METRIC_NAME)
    weight = (
        col(JobSeriesTable.allocated_rgu_drac)
        * _overlap_hours(JobSeriesTable, *window)
        * 3600.0
    )
    bin_width = 1.0 / _DENSITY_BINS

    bin_expr = _density_bin_expr(m1).label("bin")
    q = (
        _apply_dash_base(
            select(bin_expr, func.sum(weight).label("w")),
            _resolve_cluster_ids(sess, clusters),
            user_ids,
            job_states,
            scope_user_id=_scope_or_view_as(sess, req, as_user),
        )
        .where(_ran_between(JobSeriesTable, *window), _valid_metric_filter(m1))
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
    user_email: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    metric2: str = Query(default="gpu_memory"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """100x100 paired heatmap of the usage metric against a second one.

    Counts GPU jobs that ran in the window into a 100x100 grid of (usage metric,
    ``metric2``) mean values; a job contributes only if it carries both stats. No
    sampling: every job lands in exactly one cell (like the elapsed/wait
    heatmaps), and a count is not pro-rated. Returns {x, y, z} with z[iby][ibx]
    the job count of that cell (Plotly heatmap order).
    """
    if metric2 not in _METRICS_0_1:
        raise HTTPException(status_code=400, detail=f"Unknown metric: {metric2!r}")

    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    window = (begin_dt.timestamp(), finish_dt.timestamp())
    user_ids = _resolve_user_ids(sess, user_email)

    # Both metrics are pivoted job_series columns; a job "carries both" exactly
    # when neither column is NULL (a missing jobstatisticdb row, same as the
    # old LEFT joins' NULLs).
    m1 = _dash_stat_col(_USAGE_METRIC_NAME)
    m2 = _dash_stat_col(metric2)

    bx = _density_bin_expr(m1, _PAIRED_BINS).label("bx")
    by = _density_bin_expr(m2, _PAIRED_BINS).label("by")
    # group_by by label, not by expression: pg8000's server-side binding renders
    # the expression with fresh placeholders in GROUP BY (error 42803).
    q = (
        _apply_dash_base(
            select(bx, by, func.count().label("n")),
            _resolve_cluster_ids(sess, clusters),
            user_ids,
            job_states,
            scope_user_id=_scope_or_view_as(sess, req, as_user),
        )
        .where(
            _ran_between(JobSeriesTable, *window),
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
    user_email: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    min_usage: float = Query(default=0.15, ge=0.0, le=1.0),
    whole: bool = Query(default=False),
    sess: Session = Depends(session_dep),
):
    """Allocated vs effectively-used RGU.h per time bucket.

    Over GPU jobs *running* in each ``period`` bucket, each charged only for the
    time it spent there: ``rgu_allocated`` = SUM(rgu * hours in the bucket);
    ``rgu_used`` = the same scaled by each job's mean usage metric;
    ``rgu_wasted`` = the per-job shortfall below ``min_usage`` (SUM of
    rgu_hours * (min_usage - mean) over measured jobs with mean < min_usage).
    Returns one row per bucket.

    ``whole=true`` returns the range as a single bucket instead, and ``period``
    is then ignored (still validated). Not the same as adding the rows up: the
    sums would come out the same, but a job runs through several buckets and is
    weighted in each by the slice of it that lands there, so recombining
    ``metric_means`` would double-count the rest of a job crossing the
    boundary -- and would move with ``period``. Over one bucket each job is
    weighted once, by its whole rgu_hours.

    Each row also carries ``metric_means``: the usage metric, mapped to its
    rgu_hours-weighted mean (hours in the bucket x allocated GPU count x RGU
    weight) over the jobs running in that bucket with a real (non-NULL/NaN)
    value for it -- a job without one contributes to neither the sum nor the
    weight. Its numerator and denominator are exactly ``rgu_used``
    and ``rgu_allocated - rgu_unmeasured``, so the curve this draws always
    matches the bars' Used share.
    """
    begin_dt, finish_dt = _date_range(start, end)
    # A period of exactly the range yields a single bucket, so `whole` needs no
    # second path through the bucketing below. `period` is parsed either way: a
    # parameter the endpoint ignores is still a parameter it accepts or refuses,
    # and 400 on a typo should not depend on the view the caller asked for.
    parsed = _parse_period(period)
    if whole:
        parsed = finish_dt - begin_dt
    fmt = _label_fmt(parsed)
    scope_user_id = _scope_or_view_as(sess, req, as_user)
    cluster_ids = _resolve_cluster_ids(sess, clusters)
    user_ids = _resolve_user_ids(sess, user_email)
    if _no_buckets(begin_dt, finish_dt):
        return []

    # Per bucket: allocated = RGU rate x hours landing inside (_overlap_hours);
    # used = the same scaled by the usage-metric mean. The view's *_waste columns
    # were frozen whole-job and pro-rating here is per-bucket -- but the pivoted
    # job_series column gives the same per-job value, so no stat join is needed.
    buckets = _dash_bucket_table(JobSeriesTable, begin_dt, finish_dt, parsed)
    rgu_hours = col(JobSeriesTable.allocated_rgu_drac) * _overlap_hours(
        JobSeriesTable, buckets.c.bucket_start, buckets.c.bucket_end
    )
    # One column, always: the usage metric drives both the bars
    # (used/unmeasured/wasted) and metric_means below -- the dashboard plots
    # exactly one reference metric at a time.
    m_mean = _dash_stat_col(_USAGE_METRIC_NAME)
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

    query = _apply_dash_base(
        select(
            buckets.c.bucket_index,
            func.sum(rgu_hours).label("rgu_allocated"),
            func.sum(rgu_used_term).label("rgu_used"),
            func.sum(rgu_unmeasured_term).label("rgu_unmeasured"),
            func.sum(rgu_wasted_term).label("rgu_wasted"),
        ),  # ty:ignore[no-matching-overload]
        cluster_ids,
        user_ids,
        job_states,
        scope_user_id=scope_user_id,
    )
    # A job spanning several buckets yields one row per bucket, which is what
    # A job spanning several buckets yields one row per bucket, which is what
    # splits its RGU.h across them -- and what makes metric_means below read
    # as "over the jobs running in this bucket".
    query = (
        query.join(buckets, true())
        .where(
            _ran_between(JobSeriesTable, begin_dt.timestamp(), finish_dt.timestamp())
        )
        .group_by(buckets.c.bucket_index)
        .order_by(buckets.c.bucket_index)
    )

    sums = {}
    trends = {}
    for row in sess.exec(query):
        key = row.bucket_index
        allocated = float(row.rgu_allocated or 0.0)
        used = float(row.rgu_used or 0.0)
        unmeasured = float(row.rgu_unmeasured or 0.0)
        wasted = float(row.rgu_wasted or 0.0)
        sums[key] = (allocated, used, unmeasured, wasted)
        trends[key] = {
            _USAGE_METRIC_NAME: {"mean": _weighted_mean(used, allocated - unmeasured)}
        }

    empty_means = {_USAGE_METRIC_NAME: {"mean": None}}
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

    return period_data


@router.get("/metrics/rgu_by_cluster")
def metrics_rgu_by_cluster(
    req: Requestor = Depends(requestor),
    as_user: str | None = _AS_USER_QUERY,
    start: date = Query(default=None),
    end: date = Query(default=None),
    period: str = Query(default=_DEFAULT_PERIOD),
    clusters: list[str] = Query(default=[]),
    user_email: str | None = Query(default=None),
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
    scope_user_id = _scope_or_view_as(sess, req, as_user)
    cluster_ids = _resolve_cluster_ids(sess, clusters)
    user_ids = _resolve_user_ids(sess, user_email)
    if _no_buckets(begin_dt, finish_dt):
        return {"periods": [], "series": []}

    # job_series carries allocated_rgu_drac (the per-job RGU rate) but not the cluster's
    # name: group by the indexed cluster_id and map the handful of ids back to
    # names here (clusters is a tiny table; joining it would widen the scan).
    bucket_table = _dash_bucket_table(JobSeriesTable, begin_dt, finish_dt, parsed)
    rgu_hours = col(JobSeriesTable.allocated_rgu_drac) * _overlap_hours(
        JobSeriesTable, bucket_table.c.bucket_start, bucket_table.c.bucket_end
    )
    query = _apply_dash_base(
        select(
            bucket_table.c.bucket_index,
            col(JobSeriesTable.cluster_id).label("cluster_id"),
            func.sum(rgu_hours).label("rgu"),
        ),
        cluster_ids,
        user_ids,
        job_states,
        scope_user_id=scope_user_id,
    )
    query = (
        query.join(bucket_table, true())
        .where(
            _ran_between(JobSeriesTable, begin_dt.timestamp(), finish_dt.timestamp())
        )
        .group_by(bucket_table.c.bucket_index, "cluster_id")
        .order_by(bucket_table.c.bucket_index)
    )
    names = {c.id: c.name for c in sess.exec(select(SlurmClusterDB)).all()}

    sums = {}
    totals = {}
    for r in sess.exec(query):
        name = names.get(r.cluster_id)
        if not name:
            continue
        v = float(r.rgu or 0.0)
        sums[(r.bucket_index, name)] = v
        totals[name] = totals.get(name, 0.0) + v

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
    user_email: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    sess: Session = Depends(session_dep),
):
    """Per-period rgu_hours-weighted average of the usage metric's per-job
    ``mean`` and ``max``.

    For each period bucket, weighs the per-job statistic values by rgu_hours
    (hours in the bucket x allocated GPU count x RGU weight) over the jobs
    *running* in that bucket -- same weighting as /rgu_usage's metric_means, so
    a job spanning several buckets is weighted in each by only the slice that
    landed there. Jobs lacking the statistic are simply absent from the average
    (inner join), and only GPU jobs are counted. Returns a single ``series``
    entry on a period axis; buckets with no data yield null (a curve gap), not 0.
    """
    begin_dt, finish_dt = _date_range(start, end)
    parsed = _parse_period(period)
    fmt = _label_fmt(parsed)
    scope_user_id = _scope_or_view_as(sess, req, as_user)
    cluster_ids = _resolve_cluster_ids(sess, clusters)
    user_ids = _resolve_user_ids(sess, user_email)
    if _no_buckets(begin_dt, finish_dt):
        return {
            "periods": [],
            "series": [{"metric": _USAGE_METRIC_NAME, "mean": [], "max": []}],
        }

    # GPU jobs only, like every other plot (job_series's whole population),
    # including for system_memory -- the one metric CPU jobs also report, but
    # this dashboard does not plot them anywhere else. allocated_rgu_drac is the
    # duration weight; mean/max are the usage metric's pivoted job_series
    # columns. (The old INNER JOIN onto jobstatisticdb dropped jobs without the
    # stat; here their NULL values contribute to neither summand of the weighted
    # mean -- the same rows out, without the join.)
    bucket_table = _dash_bucket_table(JobSeriesTable, begin_dt, finish_dt, parsed)
    rgu_hours = col(JobSeriesTable.allocated_rgu_drac) * _overlap_hours(
        JobSeriesTable, bucket_table.c.bucket_start, bucket_table.c.bucket_end
    )
    m_mean = _dash_stat_col(_USAGE_METRIC_NAME)
    m_max = _dash_stat_col(_USAGE_METRIC_NAME, "max")
    mean_num, mean_den = _weighted_mean_cols(m_mean, rgu_hours, "mean")
    max_num, max_den = _weighted_mean_cols(m_max, rgu_hours, "max")

    query = _apply_dash_base(
        select(bucket_table.c.bucket_index, mean_num, mean_den, max_num, max_den),  # ty: ignore[no-matching-overload]
        cluster_ids,
        user_ids,
        job_states,
        scope_user_id=scope_user_id,
    ).join(bucket_table, true())
    query = (
        query.where(
            _ran_between(JobSeriesTable, begin_dt.timestamp(), finish_dt.timestamp())
        )
        .group_by(bucket_table.c.bucket_index)
        .group_by(bucket_table.c.bucket_index)
        .order_by(bucket_table.c.bucket_index)
    )

    cells = {}
    for r in sess.exec(query):
        mean_v = _weighted_mean(
            float(_nan_to_none(r.mean_num) or 0.0),
            float(_nan_to_none(r.mean_den) or 0.0),
        )
        max_v = _weighted_mean(
            float(_nan_to_none(r.max_num) or 0.0), float(_nan_to_none(r.max_den) or 0.0)
        )
        cells[r.bucket_index] = (mean_v, max_v)

    buckets = list(_iter_buckets(begin_dt, finish_dt, parsed))
    return {
        "periods": [
            {"period_start": ps.strftime(fmt), "period_end": pe.strftime(fmt)}
            for ps, pe in buckets
        ],
        "series": [
            {
                "metric": _USAGE_METRIC_NAME,
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
    user_email: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    min_usage: float = Query(default=0.15, ge=0.0, le=1.0),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Requested vs used RGU.h aggregated per user (not over time).

    Same pro-rated RGU.h measure as /rgu_usage, summed per user email
    (requested = SUM(rgu * hours in the window); used = scaled by the mean usage
    metric; ``rgu_wasted`` = the same per-job shortfall below ``min_usage``,
    so a user's critical waste reads the same here as in the bars). The window
    is one bucket here, so the totals match what the per-bucket plots add up to.
    Sorted by descending requested RGU.h.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    window = (begin_dt.timestamp(), finish_dt.timestamp())

    # Aggregate by user: RGU rate x hours spent inside the window. The usage
    # metric is a pivoted job_series column (see rgu_usage). Grouped by
    # sarc_user_id -- the indexed column -- and the handful of ids' emails are
    # looked up after the aggregation: joining users over the whole window to
    # group by email string cost more than this second tiny query.
    rgu_hours = col(JobSeriesTable.allocated_rgu_drac) * _overlap_hours(
        JobSeriesTable, *window
    )
    m_mean = _dash_stat_col(_USAGE_METRIC_NAME)
    # Split used vs unmeasured on whether the metric is a real value (not
    # NULL/NaN); a missing measurement is kept apart from "unused".
    m_present = _is_real(m_mean)
    rgu_used_term = case((m_present, rgu_hours * m_mean), else_=0.0)
    rgu_unmeasured_term = case((m_present, 0.0), else_=rgu_hours)
    # Per-job shortfall below min_usage, exactly as /rgu_usage sums it: a job
    # over the threshold contributes 0, so a user's critical waste is their own
    # jobs' and does not dilute in their good ones.
    rgu_wasted_term = case(
        (and_(m_present, m_mean < min_usage), rgu_hours * (min_usage - m_mean)),
        else_=0.0,
    )
    rgu_requested_sum = func.sum(rgu_hours).label("rgu_requested")

    query = _apply_dash_base(
        select(
            col(JobSeriesTable.sarc_user_id).label("user_id"),
            rgu_requested_sum,
            func.sum(rgu_used_term).label("rgu_used"),
            func.sum(rgu_unmeasured_term).label("rgu_unmeasured"),
            func.sum(rgu_wasted_term).label("rgu_wasted"),
        ),  # ty:ignore[no-matching-overload]
        _resolve_cluster_ids(sess, clusters),
        _resolve_user_ids(sess, user_email),
        job_states,
        scope_user_id=_scope_or_view_as(sess, req, as_user),
    )
    query = (
        query.where(_ran_between(JobSeriesTable, *window))
        .group_by("user_id")
        .order_by(rgu_requested_sum.desc())
    )

    rows = list(sess.exec(query))
    emails = dict(
        sess.exec(
            select(col(UserDB.id), col(UserDB.email)).where(
                col(UserDB.id).in_([r.user_id for r in rows] or [-1])
            )
        ).all()
    )
    # Two users rows can share one email; merge them back like the old
    # group-by-email did.
    merged: dict[str, dict] = {}
    for row in rows:
        email = emails.get(row.user_id) or "unknown"
        d = merged.setdefault(
            email,
            {
                "user": email,
                "rgu_requested": 0.0,
                "rgu_used": 0.0,
                "rgu_unmeasured": 0.0,
                "rgu_wasted": 0.0,
            },
        )
        d["rgu_requested"] += float(row.rgu_requested or 0.0)
        d["rgu_used"] += float(row.rgu_used or 0.0)
        d["rgu_unmeasured"] += float(row.rgu_unmeasured or 0.0)
        d["rgu_wasted"] += float(row.rgu_wasted or 0.0)
    out = list(merged.values())
    # Same tie-break the SQL ordering had: descending RGU, then email.
    out.sort(key=lambda d: (-d["rgu_requested"], d["user"]))
    return out


@router.get("/metrics/jobs")
def metrics_jobs(
    req: Requestor = Depends(requestor),
    as_user: str | None = _AS_USER_QUERY,
    start: date = Query(default=None),
    end: date = Query(default=None),
    clusters: list[str] = Query(default=[]),
    user_email: str | None = Query(default=None),
    job_states: list[str] = Query(default=[]),
    limit: int = Query(default=50, gt=0, le=500),
    offset: int = Query(default=0, ge=0),
    include_total: bool = Query(default=True),
    sort_by: str = Query(default="rgu_hours"),
    sort_dir: str = Query(default="desc"),
    focus_start: datetime | None = Query(default=None),
    focus_end: datetime | None = Query(default=None),
    sess: Session = Depends(session_dep),
):
    """Paginated, sortable table of individual jobs.

    Lists GPU jobs that ran in the window (cluster/user/state filtered), one row
    per job: cluster, user, state, submit/start times, elapsed, GPU counts,
    billing, gpu_type, rgu, rgu_hours, per-job metric means and ``waste``
    (rgu_hours * (1 - mean)). Every quantity counts only what falls inside the
    window, like the plots: ``elapsed`` is the slice ``rgu_hours`` is built
    from, and ``elapsed_total`` rides along so a job that crosses a boundary
    reads as partial rather than as disagreeing with itself. Sorted by
    ``sort_by``/``sort_dir`` and paginated by ``limit``/``offset``. Returns
    {total, jobs}. ``total`` is the full filtered count, computed by a SEPARATE
    query (kept out of the page query so the page parallelises — see below) and
    only when ``include_total`` is set (None otherwise). The frontend requests it
    on every page so the count and page numbers stay current as scraping adds
    jobs; the separate query stays cheap precisely because it parallelises.
    """
    begin_dt, finish_dt = _apply_focus(*_date_range(start, end), focus_start, focus_end)
    window = (begin_dt.timestamp(), finish_dt.timestamp())

    # Limit-first pagination. A `page` subquery ranks and paginates the full
    # filtered set from job_series alone -- every common sort key is in the
    # covering window index, so the ranking is an index-only scan + top-N
    # heapsort; the outer query then joins the display columns (nodes, names,
    # emails) back onto just that page of rows. See the perf note in docs / the
    # /metrics/jobs investigation.
    # Pro-rated like the plots; the rgu_hours and waste sorts rank on it too.
    overlap_hours = _overlap_hours(JobSeriesTable, *window)
    rgu_hours_raw = col(JobSeriesTable.allocated_rgu_drac) * overlap_hours
    # The same slice in seconds, to show beside the whole elapsed_time. Sorting
    # "Elapsed" ranks on this, not on the column: every other number in the row
    # is this slice, so ordering by the whole run would rank by a quantity the
    # table does not otherwise use.
    elapsed_in_window = overlap_hours * 3600.0

    # The usage metric's mean, a pivoted job_series column -- the same value the
    # old targeted jobstatisticdb LEFT join returned.
    metric_mean_raw = _dash_stat_col(_USAGE_METRIC_NAME)

    # Sortable columns -> ORDER BY expression. Raw (unlabelled) so they compose
    # with nulls_last/asc/desc cleanly. `nodes` is an array and is not sortable,
    # so it is intentionally absent. Everything but "cluster" and "user" ranks
    # on job_series columns (index-only in the covering scan); those two name
    # columns live in clusters/users and pull their join into the page query
    # when sorted on -- they stay sortable, just on the slow path.
    sortable = {
        "cluster": col(SlurmClusterDB.name),
        "job_id": col(JobSeriesTable.job_id),
        "submit_time": col(JobSeriesTable.submit_time),
        "start_time": col(JobSeriesTable.start_time),
        "user": col(UserDB.email),
        "job_state": col(JobSeriesTable.job_state),
        "elapsed": elapsed_in_window,
        "requested_gpu": col(JobSeriesTable.requested_gres_gpu),
        "allocated_gpu": col(JobSeriesTable.allocated_gres_gpu),
        "billing": col(JobSeriesTable.allocated_billing),
        # harmonized_gpu_type is NOT NULL on job_series (its whole population has
        # one), so this is the coalesce(harmonized, allocated) the view showed.
        "gpu_type": col(JobSeriesTable.harmonized_gpu_type),
        "gpu_type_rgu": col(JobSeriesTable.gpu_type_rgu_drac),
        "rgu": col(JobSeriesTable.allocated_rgu_drac),
        "rgu_hours": rgu_hours_raw,
        "waste": rgu_hours_raw * (1 - metric_mean_raw),
        "gpu_utilization_mean": _dash_stat_col("gpu_utilization"),
        "gpu_sm_occupancy_mean": _dash_stat_col("gpu_sm_occupancy"),
        "gpu_memory_max": _dash_stat_col("gpu_memory", "max"),
    }
    sort_needs_join = {"cluster": SlurmClusterDB, "user": UserDB}
    sort_expr = sortable.get(sort_by, rgu_hours_raw)
    ordered = sort_expr.asc() if sort_dir == "asc" else sort_expr.desc()
    # nulls_last only for keys nullable in the result set: the metric stats and
    # the nullable gpu/billing cols. On a NOT NULL indexed key like submit_time
    # it defeats the index -- DESC NULLS LAST matches neither the btree nor its
    # reverse scan, forcing Seq Scan + Sort. Sorting by id last breaks ties between
    # equal keys, so offset paging never skips or repeats a row.
    nullable_sorts = {
        "requested_gpu",
        "billing",
        "waste",
        "gpu_utilization_mean",
        "gpu_sm_occupancy_mean",
        "gpu_memory_max",
    }
    if sort_by in nullable_sorts:
        ordered = nulls_last(ordered)
    order_by = (ordered, col(JobSeriesTable.job_db_id))

    # Window only; DASH_ELIGIBILITY rides in via _apply_dash_base. "Ran in
    # the window", so the table lists the plots' population.
    base_filters = (_ran_between(JobSeriesTable, *window),)

    scope_user_id = _scope_or_view_as(sess, req, as_user)
    cluster_ids = _resolve_cluster_ids(sess, clusters)
    user_ids = _resolve_user_ids(sess, user_email)

    # COUNT: the full filtered total, computed by its own query and only when
    # asked (include_total). The frontend requests it on every page so the count
    # and page numbers stay current as scraping adds jobs. It is deliberately kept
    # OUT of the page query below: a `count(*) OVER ()` there forces the whole
    # filtered set to be materialised AND disables parallelism, so every page would
    # pay the full-set cost. Isolated like this the count parallelises, so paying
    # it per page stays cheap.
    total: int | None = None
    if include_total:
        count_q = _apply_dash_base(
            select(func.count()),
            cluster_ids,
            user_ids,
            job_states,
            scope_user_id=scope_user_id,
        ).where(*base_filters)
        total = int(sess.exec(count_q).one())

    # PAGE: the page's job ids only. The scan/sort runs here on job_series alone
    # (+ clusters/users when sorted on their names). With no window count it
    # parallelises, and a small offset top-N heapsorts instead of sorting the
    # whole set.
    page_q = _apply_dash_base(
        select(col(JobSeriesTable.job_db_id).label("jid")),
        cluster_ids,
        user_ids,
        job_states,
        scope_user_id=scope_user_id,
    )
    if sort_by in sort_needs_join:
        page_q = page_q.join(
            sort_needs_join[sort_by],
            (
                col(JobSeriesTable.cluster_id) == col(SlurmClusterDB.id)
                if sort_by == "cluster"
                else col(JobSeriesTable.sarc_user_id) == col(UserDB.id)
            ),
            isouter=True,
        )
    page = (
        page_q.where(*base_filters).order_by(*order_by).offset(offset).limit(limit)
    ).subquery()

    # FINAL: display columns, fetched only for the page's rows (joined back on
    # the job id): job_series holds the numbers and stats, slurm_jobs the node
    # list, clusters/users the names. The total comes from the separate count.
    query = _apply_dash_base(
        select(  # ty:ignore[no-matching-overload]
            col(SlurmClusterDB.name).label("cluster_name"),
            col(JobSeriesTable.job_id),
            col(JobSeriesTable.submit_time).label("submit_time"),
            col(JobSeriesTable.start_time).label("start_time"),
            col(UserDB.email),
            col(JobSeriesTable.job_state),
            col(JobSeriesTable.elapsed_time).label("elapsed_time"),
            elapsed_in_window.label("elapsed_in_window"),
            col(SlurmJobDB.nodes),
            col(JobSeriesTable.requested_gres_gpu),
            col(JobSeriesTable.allocated_gres_gpu),
            col(JobSeriesTable.allocated_billing),
            col(JobSeriesTable.harmonized_gpu_type),
            col(JobSeriesTable.gpu_type_rgu_drac).label("gpu_type_rgu_drac"),
            col(JobSeriesTable.allocated_rgu_drac).label("rgu"),
            rgu_hours_raw.label("rgu_hours"),
            metric_mean_raw.label("metric_mean"),
            _dash_stat_col("gpu_utilization").label("gpu_utilization_mean"),
            _dash_stat_col("gpu_sm_occupancy").label("gpu_sm_occupancy_mean"),
            _dash_stat_col("gpu_memory", "max").label("gpu_memory_max"),
        ),
        cluster_ids,
        user_ids,
        job_states,
        scope_user_id=scope_user_id,
    )
    # All LEFT: the page drives the row set; a concurrent delete of a job (or
    # its cluster/user) can only blank a display cell, never drop a row.
    query = (
        query.join(page, page.c.jid == col(JobSeriesTable.job_db_id))
        .join(SlurmJobDB, col(SlurmJobDB.id) == page.c.jid, isouter=True)
        .join(
            SlurmClusterDB,
            col(SlurmClusterDB.id) == col(JobSeriesTable.cluster_id),
            isouter=True,
        )
        .join(UserDB, col(UserDB.id) == col(JobSeriesTable.sarc_user_id), isouter=True)
        .order_by(*order_by)
    )

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
                "start_time": row.start_time.isoformat() if row.start_time else None,
                "user": row.email or "",
                "job_state": row.job_state.value if row.job_state is not None else "",
                "elapsed": _nan_to_none(row.elapsed_in_window),
                "elapsed_total": row.elapsed_time or 0,
                "nodes": ", ".join(row.nodes or []) or None,
                "requested_gpu": row.requested_gres_gpu,
                "allocated_gpu": row.allocated_gres_gpu,
                "billing": row.allocated_billing,
                # Harmonised name (the one RGU is computed from) when known;
                # raw Slurm name otherwise.
                "gpu_type": row.harmonized_gpu_type or "",
                "gpu_type_rgu": _nan_to_none(row.gpu_type_rgu_drac),
                "rgu": round(float(row.rgu), 2),
                "rgu_hours": round(rh, 2) if rh is not None else None,
                "waste": waste,
                # Usage-metric mean (None when unmeasured): drives the
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
