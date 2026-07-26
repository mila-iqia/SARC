import contextvars
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import cached_property, partial
from typing import TypeVar

from sqlmodel import col, func, select

from sarc.api.metrics import _is_real
from sarc.config import config
from sarc.db.job_series import JobSeriesDB

_MAX_WORKERS = 4

T = TypeVar("T")


def _run_concurrently(tasks: Sequence[Callable[[], T]]) -> list[T]:
    # Order-preserving: results match the order of `tasks`, not completion
    # order. On any task exception, that exception propagates from
    # .result(), but only after every already-dispatched task has finished
    # (ThreadPoolExecutor's __exit__ waits for all of them; there's no cheap
    # way to cancel already-running threads) -- fine for a handful of
    # iterations with no external SLA.
    #
    # Each task gets its own contextvars.copy_context() rather than one
    # shared copy: gifnoc's active-configuration overlay (set via `with
    # gifnoc.overlay(...):`) lives in a ContextVar, which a fresh worker
    # thread won't see unless the context is copied in explicitly -- and a
    # single Context object can't be entered concurrently by more than one
    # thread, so reusing one copy across submissions raises
    # "RuntimeError: cannot enter context" as soon as two tasks overlap.
    if not tasks:
        return []

    def _run_in_context(ctx: contextvars.Context, task: Callable[[], T]) -> T:
        return ctx.run(task)

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        # contextvars.copy_context() is called here, in the submitting
        # thread, for each task -- NOT inside _run_in_context, which runs on
        # the worker thread and would otherwise copy that (uninitialized)
        # worker context instead of this thread's overlaid one.
        futures: list[Future[T]] = [
            executor.submit(_run_in_context, contextvars.copy_context(), task)
            for task in tasks
        ]
        return [f.result() for f in futures]


def usage_cycle_length_weeks():
    assert config.notifications, (
        f"{config.notifications=}"
    )  # Avoid warning "usage_cycle_length_weeks" is not a known attribute of "None"
    return config.notifications.usage_cycle_length_weeks


def restrictive_action_run_cycles():
    # A personalized-action (⚑) peak sustained over this many consecutive cycles
    # (the peak cycle plus the (n-1) cycles following it) escalates to a
    # restrictive action.
    assert config.notifications, f"{config.notifications=}"
    return config.notifications.restrictive_action_run_cycles


@dataclass
class UsageClusterBreakdown:
    cluster: str
    # RGU-hours allocated for this cluster in the window.
    rgu_hours: float
    rgu_hours_used: float
    # Ceiling-adjusted RGU-hours wasted (rgu_hours - credited_used).
    wasted: float


@dataclass
class UsageJob:
    job_id: int
    cluster: str
    submit_time: datetime
    # RGU-hours unused for this job.
    wasted: float | None
    rgu_hours_used: float | None
    gpu_sm_occupancy: float


@dataclass
class UnderuserRow:
    email: str
    display_name: str
    user_id: int
    # Total RGU-hours allocated over the window.
    rgu_hours: float
    # RGU-hours wasted over the window (= rgu_hours - rgu_used). Used for the
    # activity floor: the floor is compared against *wasted* RGU-hours, so that
    # users who waste a significant absolute amount are flagged regardless of
    # their total allocation size.
    wasted: float
    # waste_ratio = wasted / rgu_hours  (= 1 - rgu_used / rgu_hours)
    waste_ratio: float
    # Unadjusted reference values (utilization_ceiling=1.0 → equal to wasted/waste_ratio).
    true_wasted: float = 0.0
    true_waste_ratio: float = 0.0
    by_cluster: list[UsageClusterBreakdown] = field(default_factory=list)
    # Top-N GPU jobs by RGU-hours unused, descending.
    top_jobs: list[UsageJob] = field(default_factory=list)

    # avg_utilization = 1 - waste_ratio  (= rgu_used / rgu_hours)
    @cached_property
    def avg_utilization(self) -> float:
        return 1.0 - self.waste_ratio


@dataclass
class UsageRow:
    email: str
    display_name: str
    user_id: int
    rgu_hours: float
    rgu_hours_used: float
    by_cluster: list[UsageClusterBreakdown] = field(default_factory=list)
    top_jobs: list[UsageJob] = field(default_factory=list)

    @cached_property
    def avg_utilization(self) -> float:
        return self.rgu_hours_used / self.rgu_hours


@dataclass
class RecurringUserRow:
    email: str
    display_name: str
    cluster: str
    # Wasted RGU-h for this user in this cluster over the recurrence window.
    wasted_current_active_window: float
    # Fraction of the cluster's total wasted RGU-h in the same window (0..1).
    cluster_share: float
    # Cycle membership: was this user flagged by get_underusers for each window?
    # Index 0 = W0 (most recent), last = W-(2*(n-1)).
    # None = future cycle (anchor > end at run time); bool = past/present cycle.
    cycles: list[bool | None]
    # True iff the user's ceiling-adjusted waste in the active-cycles window meets the floor.
    flagged_for_personalized_action: bool
    # True (unadjusted) wasted RGU-h for this user in this cluster over the recurrence window.
    true_wasted: float = 0.0
    # Per-anchor PA flags, one per displayed cycle (length
    # recurrence_display_cycles): index 0 = most-recent anchor, True iff the
    # recurrence_active_cycles-cycle window ending at that anchor has
    # ceiling-adjusted cross-cluster waste ≥
    # personalized_action_min_waste_rgu_hours (and the user is a single-cycle
    # underuser in that cycle). These drive the per-cell ⚑ peak marker.
    pa_flags: list[bool] = field(default_factory=list)

    @cached_property
    def restrictive_action_flags(self) -> list[bool]:
        """Per-cycle escalation flags derived from pa_flags. Index i is True iff
        this cycle and the ``restrictive_action_run_cycles``-1 cycles following
        it in time (older cycles, i+1, i+2, …) are all personalized-action (⚑)
        peaks — a sustained run signalling that a restrictive action could be
        enforced. The flag lands on the newest cell of each such run. The run
        length is the ``restrictive_action_run_cycles`` notifications config
        knob."""
        n = len(self.pa_flags)
        run = restrictive_action_run_cycles()
        return [i + run <= n and all(self.pa_flags[i : i + run]) for i in range(n)]


def _rgu_exprs(utilization_ceiling: float = 1.0):
    """Return (rgu_h_expr, true_used_expr, credited_used_expr), all derived
    from job_series_view columns alone (m = the job's gpu_sm_occupancy mean):

    rgu_h_expr = allocated_gpu_cost / 3600 — allocated RGU-hours (== rgu *
                 elapsed / 3600).
    true_wasted = allocated_gpu_waste / 3600 = rgu_h * (1 - m). Jobs with no
                  gpu_sm_occupancy stat recorded (NaN/NULL allocated_gpu_waste)
                  are excluded upstream by `_with_rgu_window`, so wasted_raw is
                  always real here.
    true_used_expr = rgu_h - true_wasted.
    credited_used_expr = rgu_h - GREATEST(0, true_wasted - rgu_h * (1 - T)),
                         with T = utilization_ceiling — algebraically identical
                         to LEAST(rgu_h, rgu_h * (1 - T + m)) since rgu_h >= 0.
                         Waste = rgu_h - credited_used = max(0, rgu_h * (T -
                         m)). At T=1.0, credited == true.
    """
    rgu_h_expr = col(JobSeriesDB.allocated_gpu_cost) / 3600.0
    # Subtract the ceiling on the raw RGU-second columns, before the /3600, so
    # that allocated_gpu_waste - allocated_gpu_cost * (1 - T) cancels exactly
    # when the job's occupancy equals T (both sides compute the identical
    # product).
    wasted_raw = col(JobSeriesDB.allocated_gpu_waste)
    true_used_expr = rgu_h_expr - wasted_raw / 3600.0
    credited_used_expr = (
        rgu_h_expr
        - func.greatest(
            0.0,
            wasted_raw
            - col(JobSeriesDB.allocated_gpu_cost) * (1.0 - utilization_ceiling),
        )
        / 3600.0
    )
    return rgu_h_expr, true_used_expr, credited_used_expr


def _with_rgu_window(stmt, start, end, *, clusters=None):
    """Apply the end-time / GPU-type / RGU window filters.

    Also excludes jobs with no recorded gpu_sm_occupancy stat (NULL/NaN
    allocated_gpu_waste) — such jobs carry no usage signal and are dropped
    entirely rather than counted as fully utilized.
    """
    stmt = stmt.where(
        col(JobSeriesDB.end_time) >= start,
        col(JobSeriesDB.end_time) < end,
        col(JobSeriesDB.allocated_gpu_type).is_not(None),
        col(JobSeriesDB.allocated_rgu_drac).is_not(None),
        _is_real(col(JobSeriesDB.allocated_gpu_waste)),
    )
    if clusters:
        stmt = stmt.where(col(JobSeriesDB.cluster_name).in_(clusters))
    return stmt


def _select_user_jobs(
    user_ids: list[int] | None,
    start: datetime,
    end: datetime,
    clusters: list[str] | None = None,
    utilization_ceiling: float = 1.0,
):
    """Return one row per job (no aggregation) for *user_ids* (all users when
    None): job_db_id, sarc_user_id, cluster_name, submit_time, rgu_hours,
    rgu_used, allocated_gpu_cost, allocated_gpu_waste. Used to fetch top-job
    detail rows for users already identified via `_select_jobs_usage`."""
    rgu_h_expr, true_used_expr, _ = _rgu_exprs(utilization_ceiling)
    stmt = select(  # ty:ignore[no-matching-overload]
        col(JobSeriesDB.job_db_id),
        col(JobSeriesDB.sarc_user_id),
        col(JobSeriesDB.cluster_name),
        col(JobSeriesDB.submit_time),
        rgu_h_expr.label("rgu_hours"),
        true_used_expr.label("rgu_used"),
        col(JobSeriesDB.allocated_gpu_cost),
        col(JobSeriesDB.allocated_gpu_waste),
    )
    if user_ids is not None:
        stmt = stmt.where(col(JobSeriesDB.sarc_user_id).in_(user_ids))
    return _with_rgu_window(stmt, start, end, clusters=clusters)


def _select_jobs_usage(
    user_ids: list[int] | None,
    start: datetime,
    end: datetime,
    *,
    by_cluster: bool,
    clusters: list[str] | None = None,
    utilization_ceiling: float = 1.0,
    user_emails: list[str] | None = None,
):
    """Return an already-grouped per-user RGU-usage aggregate statement
    (sum_rgu_hours, sum_rgu_true_used, sum_rgu_used) — the caller does not
    attach its own `.group_by(...)`.

    by_cluster=True: also selects cluster_name; grouped by (sarc_user_id,
    cluster_name) — one row per (user, cluster).
    by_cluster=False: grouped by sarc_user_id only — one cross-cluster row
    per user.

    *user_ids* optionally restricts to a subset of users (None = all users in
    the window). *user_emails* optionally restricts to a subset of users by
    email (None = all users in the window); independent of *user_ids*.
    """
    rgu_h_expr, true_used_expr, credited_used_expr = _rgu_exprs(utilization_ceiling)
    _by_cluster = [col(JobSeriesDB.cluster_name)] if by_cluster else []
    if user_emails is None:
        user_emails, not_user_emails = None, None
    else:
        user_emails, not_user_emails = (
            [email for email in user_emails if not email.startswith("~")],
            [email[1:] for email in user_emails if email.startswith("~")],
        )
        if not user_emails and not_user_emails:
            # Every entry was an exclusion (~email) — the caller expressed no
            # positive allow-list restriction, only exclusions. Contrast with
            # an explicit user_emails=[] (no exclusions either), which is a
            # real "match nobody" filter and must still apply below.
            user_emails = None

    stmt = select(  # ty:ignore[no-matching-overload]
        col(JobSeriesDB.sarc_user_id),
        # Use func.any_value for email and display_name to allow aggregation
        # across multiple rows per (user_id, cluster) without requiring
        # these fields in the GROUP BY clause.
        # https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUP
        # "In general, if a table is grouped, columns that are not listed in
        # GROUP BY cannot be referenced except in aggregate expressions."
        func.any_value(JobSeriesDB.email).label("email"),
        func.any_value(JobSeriesDB.display_name).label("display_name"),
        *_by_cluster,
        func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
        func.coalesce(func.sum(true_used_expr), 0).label("sum_rgu_true_used"),
        func.coalesce(func.sum(credited_used_expr), 0).label("sum_rgu_used"),
    )
    if user_ids is not None:
        stmt = stmt.where(col(JobSeriesDB.sarc_user_id).in_(user_ids))
    if user_emails is not None:
        stmt = stmt.where(col(JobSeriesDB.email).in_(user_emails))
    if not_user_emails is not None:
        stmt = stmt.where(~col(JobSeriesDB.email).in_(not_user_emails))
    return _with_rgu_window(stmt, start, end, clusters=clusters).group_by(
        JobSeriesDB.sarc_user_id, *_by_cluster
    )


def _job_occupancy(cost: float, waste: float) -> float:
    """Recover a job's gpu_sm_occupancy mean from the view's cost/waste columns
    (waste = (1 - m) * cost), clamped to <= 1.0. Zero cost -> 1.0 (fully
    used)."""
    return min(1.0, 1.0 - waste / cost)


def _split_waste(row) -> tuple[float, float, float]:
    """Return (rgu_h, rgu_h_true_used, rgu_h - rgu_h_used)."""
    rgu_h = float(row.sum_rgu_hours or 0.0)
    rgu_h_used = float(row.sum_rgu_used or 0.0)
    rgu_h_true_used = float(row.sum_rgu_true_used or 0.0)
    return rgu_h, rgu_h_true_used, rgu_h - rgu_h_used


def get_underusers(
    start: datetime,
    end: datetime,
    *,
    min_waste_ratio: float,
    min_waste_rgu_hours: float,
    top_jobs_per_user: int,
    clusters: list[str] | None = None,
    utilization_ceiling: float = 1.0,
    user_emails: list[str] | None = None,
) -> list[UnderuserRow]:
    with config.db.session() as session:
        stmt = _select_jobs_usage(
            None,
            start,
            end,
            by_cluster=True,
            clusters=clusters,
            utilization_ceiling=utilization_ceiling,
            user_emails=user_emails,
        )
        agg_rows = session.exec(stmt).all()

        user_data: dict[int, dict] = {}
        for row in agg_rows:
            uid = row.sarc_user_id
            if uid not in user_data:
                user_data[uid] = {
                    "email": row.email,
                    "display_name": row.display_name,
                    "clusters": [],
                }
            rgu_h, rgu_h_true_used, rgu_h_wasted = _split_waste(row)
            user_data[uid]["clusters"].append(
                UsageClusterBreakdown(
                    cluster=row.cluster_name or "unknown",
                    rgu_hours=rgu_h,
                    rgu_hours_used=rgu_h_true_used,
                    wasted=rgu_h_wasted,
                )
            )

        # Identify users who meet both threshold conditions: their cross-cluster
        # aggregated waste ratio and total wasted RGU-hours exceed
        # `min_waste_ratio` and `min_waste_rgu_hours` respectively.
        underuser_ids: list[int] = []
        for uid, u in user_data.items():
            breakdowns: list[UsageClusterBreakdown] = u["clusters"]
            total_rgu_h = sum(c.rgu_hours for c in breakdowns)
            total_wasted = sum(c.wasted for c in breakdowns)
            u["total_rgu_h"] = total_rgu_h
            u["total_wasted"] = total_wasted
            u["total_true_wasted"] = total_rgu_h - sum(
                c.rgu_hours_used for c in breakdowns
            )
            waste_ratio = total_wasted / total_rgu_h if total_rgu_h > 0 else 0.0
            u["waste_ratio"] = waste_ratio
            if waste_ratio >= min_waste_ratio and total_wasted >= min_waste_rgu_hours:
                underuser_ids.append(uid)

        # Per-job data for the identified underusers — same RGU × utilisation
        # pattern.
        jobs_by_user: dict[int, list[UsageJob]] = {uid: [] for uid in underuser_ids}
        if top_jobs_per_user > 0 and jobs_by_user:
            job_rows = session.exec(
                _select_user_jobs(
                    underuser_ids,
                    start,
                    end,
                    clusters=clusters,
                    utilization_ceiling=utilization_ceiling,
                )
            ).all()

            for jr in job_rows:
                uid = jr.sarc_user_id
                rgu_h = float(jr.rgu_hours or 0.0)
                rgu_h_credited_used = float(jr.rgu_used or 0.0)
                gpu_sm_occupancy = _job_occupancy(
                    jr.allocated_gpu_cost, jr.allocated_gpu_waste
                )
                jobs_by_user[uid].append(
                    UsageJob(
                        job_id=jr.job_db_id,
                        cluster=jr.cluster_name or "unknown",
                        submit_time=jr.submit_time,
                        wasted=rgu_h - rgu_h_credited_used,
                        rgu_hours_used=None,
                        gpu_sm_occupancy=gpu_sm_occupancy,
                    )
                )

    result = []
    for uid in underuser_ids:
        u = user_data[uid]
        total_rgu_h = u["total_rgu_h"]
        total_wasted = u["total_wasted"]
        waste_ratio = u["waste_ratio"]

        by_cluster = sorted(u["clusters"], key=lambda c: c.wasted, reverse=True)

        top_jobs = sorted(
            jobs_by_user[uid], key=lambda j: j.wasted, reverse=True
        )[  # ty:ignore[no-matching-overload]
            :top_jobs_per_user
        ]

        total_true_wasted = u["total_true_wasted"]
        result.append(
            UnderuserRow(
                email=u["email"],
                display_name=u["display_name"],
                user_id=uid,
                rgu_hours=total_rgu_h,
                wasted=total_wasted,
                waste_ratio=waste_ratio,
                true_wasted=total_true_wasted,
                true_waste_ratio=total_true_wasted / total_rgu_h
                if total_rgu_h > 0
                else 0.0,
                by_cluster=by_cluster,
                top_jobs=top_jobs,
            )
        )

    return result


def get_all_users_usage(
    start: datetime,
    end: datetime,
    *,
    min_usage_rgu_hours: float = 0.0,
    top_jobs_per_user: int,
    clusters: list[str] | None = None,
    user_emails: list[str] | None = None,
) -> list[UsageRow]:
    with config.db.session() as session:
        stmt = _select_jobs_usage(
            None,
            start,
            end,
            by_cluster=True,
            clusters=clusters,
            user_emails=user_emails,
        )
        agg_rows = session.exec(stmt).all()

        user_data: dict[int, dict] = {}
        for row in agg_rows:
            uid = row.sarc_user_id
            if uid not in user_data:
                user_data[uid] = {
                    "email": row.email,
                    "display_name": row.display_name,
                    "clusters": [],
                }
            rgu_h, rgu_h_true_used, rgu_h_wasted = _split_waste(row)
            user_data[uid]["clusters"].append(
                UsageClusterBreakdown(
                    cluster=row.cluster_name or "unknown",
                    rgu_hours=rgu_h,
                    rgu_hours_used=rgu_h_true_used,
                    wasted=rgu_h_wasted,
                )
            )

        all_user_ids = list(user_data.keys())

        jobs_by_user: dict[int, list[UsageJob]] = {uid: [] for uid in all_user_ids}
        if top_jobs_per_user > 0 and all_user_ids:
            job_rows = session.exec(
                _select_user_jobs(all_user_ids, start, end, clusters)
            ).all()

            for jr in job_rows:
                uid = jr.sarc_user_id
                rgu_used_h = float(jr.rgu_used or 0.0)
                jobs_by_user[uid].append(
                    UsageJob(
                        job_id=jr.job_db_id,
                        cluster=jr.cluster_name or "unknown",
                        submit_time=jr.submit_time,
                        wasted=None,
                        rgu_hours_used=rgu_used_h,
                        gpu_sm_occupancy=_job_occupancy(
                            jr.allocated_gpu_cost, jr.allocated_gpu_waste
                        ),
                    )
                )

    result = []
    for uid, u in user_data.items():
        breakdowns = u["clusters"]
        total_rgu_h = sum(c.rgu_hours for c in breakdowns)
        total_used = sum(c.rgu_hours_used for c in breakdowns)
        if total_rgu_h <= min_usage_rgu_hours:
            continue

        by_cluster = sorted(breakdowns, key=lambda c: c.rgu_hours_used, reverse=True)
        # Sorted by GPU utilization (not usage volume) so these are honestly
        # the user's *most efficient* jobs, per the usage report's framing.
        top_jobs = sorted(
            jobs_by_user[uid], key=lambda j: j.gpu_sm_occupancy, reverse=True
        )[:top_jobs_per_user]

        result.append(
            UsageRow(
                email=u["email"],
                display_name=u["display_name"],
                user_id=uid,
                rgu_hours=total_rgu_h,
                rgu_hours_used=total_used,
                by_cluster=by_cluster,
                top_jobs=top_jobs,
            )
        )

    return result


# ── Recurring-underusers table ──


def _week_anchor(end: datetime) -> datetime:
    """Return day of the current (or next) week that is a multiple of the
    configured usage_cycle_length_weeks.

    Advances *end* forward so that the resulting ISO week number is divisible by
    usage_cycle_length_weeks. The anchor may therefore be in the future relative
    to *end*.

    Limitation: ISO years with 53 weeks cause week 53 to be treated as an
    off-cycle week (53 % usage_cycle_length_weeks != 0 for
    usage_cycle_length_weeks=2), effectively skipping the DM cycle that would
    otherwise align with it.
    """
    cycle_length_weeks = usage_cycle_length_weeks()
    remainder = end.isocalendar().week % cycle_length_weeks
    end += timedelta(weeks=(cycle_length_weeks - remainder) % cycle_length_weeks)
    return end


def get_cycle_dates(end: datetime, n: int = 5) -> list[date]:
    """Return n cycle end-dates [W0, W-k, ..., W-(k*(n-1))] as date objects,
    where k = the configured usage_cycle_length_weeks.

    Each date is the day of an aligned ISO week, spaced usage_cycle_length_weeks
    apart, anchored to the current (or next) aligned week from *end*.
    """
    cycle_length_weeks = usage_cycle_length_weeks()
    anchor = _week_anchor(end)
    return [(anchor - timedelta(weeks=i * cycle_length_weeks)).date() for i in range(n)]


def _fetch_pa_window_rows(
    user_ids: list[int],
    pa_start: datetime,
    pa_end: datetime,
    i: int,
    clusters: list[str] | None,
    utilization_ceiling: float,
):
    """One position's personalized-action rolling-window query, run inside
    `get_recurring_underusers`'s capped thread pool -- opens its own session
    rather than sharing one, since sessions aren't thread-safe. Module-level
    (not a nested closure) so it's directly monkeypatch-able from tests.
    """
    with config.db.session() as session:
        pa_stmt = _select_jobs_usage(
            user_ids,
            pa_start,
            pa_end,
            by_cluster=False,
            clusters=clusters,
            utilization_ceiling=utilization_ceiling,
        )
        return i, session.exec(pa_stmt).all()


def get_recurring_underusers(
    end: datetime,
    *,
    min_waste_ratio: float,
    min_waste_rgu_hours: float,
    cluster_share_threshold: float,
    recurrence_active_cycles: int = 3,
    recurrence_display_cycles: int = 5,
    clusters: list[str] | None = None,
    utilization_ceiling: float = 1.0,
    personalized_action_min_waste_rgu_hours: float = 0.0,
) -> dict[str, list[RecurringUserRow]]:
    """Return per-cluster top wasters for the recurring-underusers digest table.

    Selection: for each cluster, rank users by wasted RGU-h over the rolling
    *window_weeks* × 7-day window (ending at the aligned-week anchor) and
    include the top users until their cumulative waste reaches >=
    *cluster_share_threshold* of that cluster's total wasted RGU-h.

    Cycle flags: for each of the *recurrence_display_cycles* most-recent
    windows of the configured usage_cycle_length_weeks weeks, call
    get_underusers to determine per-user membership. Cycles whose end date is in the future relative to *end* are
    marked None (no data yet).

    Personalized-action flags: for each of the *recurrence_display_cycles* most-recent
    positions, ``pa_flags[i]`` is True iff the user's wasted RGU-h over the
    *recurrence_active_cycles*-cycle window ending at position *i* reaches
    *personalized_action_min_waste_rgu_hours* **and** the user is a single-cycle
    underuser in that position's most-recent cycle (i.e. present in
    ``cycle_flagged[i]``). The aggregate ``flagged_for_personalized_action`` is
    ``pa_flags[0]`` restricted to a current cycle that is not in the future.
    Requires ``recurrence_active_cycles <= recurrence_display_cycles``.

    Returns a dict of cluster_name -> list[RecurringUserRow] (sorted desc by
    wasted_6w within each cluster), ordered by cluster name.
    """
    cycle_length_weeks = usage_cycle_length_weeks()

    # The per-position PA loop indexes cycle_flagged (sized by
    # recurrence_display_cycles), so the active window must fit within the
    # displayed cycles.
    assert recurrence_active_cycles <= recurrence_display_cycles, (
        f"{recurrence_active_cycles=} must be <= {recurrence_display_cycles=}"
    )

    window_weeks = recurrence_active_cycles * cycle_length_weeks
    anchor = _week_anchor(end)
    agg_start = anchor - timedelta(weeks=window_weeks)

    # ── Per-(user, cluster) aggregate over the full recurrence window ─────────
    with config.db.session() as session:
        stmt = _select_jobs_usage(
            None,
            agg_start,
            anchor,
            by_cluster=True,
            clusters=clusters,
            utilization_ceiling=utilization_ceiling,
        )
        agg_rows = session.exec(stmt).all()

    # ── Organise wasted RGU-h per (cluster, user) ─────────────────────────────
    # cluster -> user_id -> {email, display_name, wasted, true_wasted}
    cluster_users: dict[str, dict[int, dict]] = {}
    for row in agg_rows:
        cluster = row.cluster_name or "unknown"
        uid = row.sarc_user_id
        rgu_h, rgu_h_true_used, rgu_h_wasted = _split_waste(row)
        if rgu_h_wasted <= 0:
            continue
        rgu_h_true_wasted = rgu_h - rgu_h_true_used
        if cluster not in cluster_users:
            cluster_users[cluster] = {}
        assert uid not in cluster_users[cluster], (
            f"A {uid=} should not appear twice for the same {cluster=}"
        )
        cluster_users[cluster][uid] = {
            "email": row.email,
            "display_name": row.display_name,
            "wasted": rgu_h_wasted,
            "true_wasted": rgu_h_true_wasted,
        }

    # ── Cycle membership sets ─────────────────────────────────────────────────
    # Each cycle ends at anchor - i*cycle_length_weeks (always aligned). Cycles
    # whose end is in the future relative to `end` yield None (no data). Each
    # position's get_underusers call is independent of every other, so they
    # run on a small capped thread pool instead of one after another.
    cycle_flagged: list[set[int] | None] = [None] * recurrence_display_cycles
    active_cycles: list[tuple[int, datetime, datetime]] = []
    for i in range(recurrence_display_cycles):
        c_end = anchor - timedelta(weeks=i * cycle_length_weeks)
        if c_end > end:
            continue
        c_start = c_end - timedelta(weeks=cycle_length_weeks)
        active_cycles.append((i, c_start, c_end))

    membership_results = _run_concurrently(
        [
            partial(
                get_underusers,
                c_start,
                c_end,
                min_waste_ratio=min_waste_ratio,
                min_waste_rgu_hours=min_waste_rgu_hours,
                # Only user_id is used for membership — top jobs are discarded.
                top_jobs_per_user=0,
                clusters=clusters,
                utilization_ceiling=utilization_ceiling,
            )
            for _, c_start, c_end in active_cycles
        ]
    )
    for (i, _, _), flagged_rows in zip(active_cycles, membership_results):
        cycle_flagged[i] = {r.user_id for r in flagged_rows}

    # ── Personalized-action aggregate (per active anchor, cross-cluster) ──────
    # For position i, the window is [anchor − (i+active_cycles)·cl, anchor −
    # i·cl]. Index 0 = most-recent anchor (matches the former single-window
    # query). Same independence-per-position as above, so also run concurrently
    # — each position opens its own session (_fetch_pa_window_rows) rather than
    # sharing one across positions.
    user_ids = list({uid for uids in cluster_users.values() for uid in uids})
    pa_window_weeks = recurrence_active_cycles * cycle_length_weeks
    active_pa_positions: list[tuple[int, datetime, datetime]] = []
    for i in range(recurrence_display_cycles):
        if cycle_flagged[i] is None:
            continue
        pa_end = anchor - timedelta(weeks=i * cycle_length_weeks)
        pa_start = pa_end - timedelta(weeks=pa_window_weeks)
        active_pa_positions.append((i, pa_start, pa_end))

    pa_results = _run_concurrently(
        [
            partial(
                _fetch_pa_window_rows,
                user_ids,
                pa_start,
                pa_end,
                i,
                clusters,
                utilization_ceiling,
            )
            for i, pa_start, pa_end in active_pa_positions
        ]
    )

    user_pa_flags: dict[int, list[bool]] = {}
    for i, rows in pa_results:
        # PA at position i requires both the waste floor and single-cycle
        # underuse in that position's most-recent cycle.
        _this_cycle_flagged: set[int] = cycle_flagged[i] or set()
        for row in rows:
            uid = row.sarc_user_id
            if uid not in user_pa_flags:
                user_pa_flags[uid] = [False] * recurrence_display_cycles
            _, _, pa_rgu_wasted_h = _split_waste(row)
            user_pa_flags[uid][i] = (
                pa_rgu_wasted_h >= personalized_action_min_waste_rgu_hours
                and uid in _this_cycle_flagged
            )

    # ── Per-cluster greedy selection (cumulative share >= cluster_share_threshold) ──
    result: dict[str, list[RecurringUserRow]] = {}
    for cluster, users in sorted(cluster_users.items()):
        cluster_total = sum(u["wasted"] for u in users.values())

        sorted_users = sorted(
            users.items(), key=lambda kv: kv[1]["wasted"], reverse=True
        )

        selected: list[tuple[int, dict]] = []
        cumulative = 0.0
        for uid, u in sorted_users:
            selected.append((uid, u))
            cumulative += u["wasted"]
            if cumulative / cluster_total >= cluster_share_threshold:
                break

        rows_out = []
        for uid, u in selected:
            cycles_for_user = [
                (None if cf is None else uid in cf) for cf in cycle_flagged
            ]
            # A selected user always has a user_pa_flags entry: the position-0 PA
            # window equals the aggregate window that selected them. The [] default
            # (and the `else False` below) is therefore defensive/unreachable.
            pa_flags = user_pa_flags.get(uid, [])
            rows_out.append(
                RecurringUserRow(
                    email=u["email"],
                    display_name=u["display_name"],
                    cluster=cluster,
                    wasted_current_active_window=u["wasted"],
                    cluster_share=u["wasted"] / cluster_total,
                    cycles=cycles_for_user,
                    # pa_flags[0] already encodes the "current cycle not in the
                    # future" guard (cycle_flagged[0] is None for a future cycle,
                    # and anchor >= end always), so no extra anchor check is needed.
                    flagged_for_personalized_action=pa_flags[0] if pa_flags else False,
                    true_wasted=u["true_wasted"],
                    pa_flags=pa_flags,
                )
            )
        result[cluster] = rows_out

    return result
