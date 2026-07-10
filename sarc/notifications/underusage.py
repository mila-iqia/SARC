from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import cached_property

from sqlalchemy import select
from sqlmodel import Session, and_, case, col, func

from sarc.config import UTC, config
from sarc.db.job_series import JobSeriesDB

DEFAULT_USAGE_CYCLE_LENGTH_WEEKS = 2


def usage_cycle_length_weeks():
    return (
        config.notifications.usage_cycle_length_weeks
        if config.notifications
        else DEFAULT_USAGE_CYCLE_LENGTH_WEEKS
    )


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
    # RGU-hours unused for this job. Equals 0 RGU-hours when utilization is
    # missing (utilization assumed 100%).
    wasted: float | None
    rgu_hours_used: float | None
    # 100% when no gpu_sm_occupancy stat was recorded for this job.
    gpu_sm_occupancy: float | None


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
    personalized_action: bool
    # True (unadjusted) wasted RGU-h for this user in this cluster over the recurrence window.
    true_wasted: float = 0.0
    # Per-active-anchor PA flags: index 0 = most-recent anchor, True iff the
    # recurrence_active_cycles-cycle window ending at that anchor has ceiling-adjusted
    # cross-cluster waste ≥ personalized_action_min_waste_rgu_hours.
    pa_flags: list[bool] = field(default_factory=list)


def _rgu_exprs(utilization_ceiling: float = 1.0):
    """Return (rgu_h_expr, true_used_expr, credited_used_expr), all derived
    from job_series_view columns alone (m = the job's gpu_sm_occupancy mean):

    rgu_h_expr = allocated_gpu_cost / 3600 — allocated RGU-hours (== rgu *
                 elapsed / 3600).
    true_wasted = allocated_gpu_waste / 3600 = rgu_h * (1 - m), or 0 (fully used,
                  zero waste) when m is NaN/NULL — no gpu_sm_occupancy stat
                  recorded.
    true_used_expr = rgu_h - true_wasted.
    credited_used_expr = rgu_h - GREATEST(0, true_wasted - rgu_h * (1 - T)),
                         with T = utilization_ceiling — algebraically identical
                         to LEAST(rgu_h, rgu_h * (1 - T + m)) since rgu_h >= 0.
                         Waste = rgu_h - credited_used = max(0, rgu_h * (T -
                         m)). At T=1.0, credited == true.

    The `w == w` idiom (NaN != NaN in SQL, and NULL == NULL is NULL) routes
    both SQL NULL and NaN allocated_gpu_waste to the else branch (zero waste,
    user not flagged).
    """
    rgu_h_expr = col(JobSeriesDB.allocated_gpu_cost) / 3600.0
    wasted_raw = col(JobSeriesDB.allocated_gpu_waste)
    # Guard and subtract the ceiling on the raw RGU-second columns, before the
    # /3600, so that allocated_gpu_waste - allocated_gpu_cost * (1 - T) cancels
    # exactly when the job's occupancy equals T (both sides compute the
    # identical product).
    finite_wasted_raw = case(
        (
            and_(
                wasted_raw == wasted_raw,  # noqa: PLR0124  — excludes NaN/NULL
                wasted_raw > float("-inf"),
                wasted_raw < float("inf"),  # excludes ±Infinity
            ),
            wasted_raw,
        ),
        else_=0.0,
    )
    true_used_expr = rgu_h_expr - finite_wasted_raw / 3600.0
    credited_used_expr = (
        rgu_h_expr
        - func.greatest(
            0.0,
            finite_wasted_raw
            - col(JobSeriesDB.allocated_gpu_cost) * (1.0 - utilization_ceiling),
        )
        / 3600.0
    )
    return rgu_h_expr, true_used_expr, credited_used_expr


def _with_rgu_window(
    stmt, start, end, *, exclude_zero_usage, rgu_used_expr, clusters=None
):
    """Apply the end-time / GPU-type / RGU window filters."""
    stmt = stmt.where(
        col(JobSeriesDB.end_time) >= start,
        col(JobSeriesDB.end_time) < end,
        col(JobSeriesDB.allocated_gpu_type).is_not(None),
        col(JobSeriesDB.allocated_rgu_drac).is_not(None),
    )
    if clusters:
        stmt = stmt.where(col(JobSeriesDB.cluster_name).in_(clusters))
    if exclude_zero_usage:
        stmt = stmt.having(func.coalesce(func.sum(rgu_used_expr), 0) > 0)
    return stmt


def _job_occupancy(cost: float | None, waste: float | None) -> float:
    """Recover a job's gpu_sm_occupancy mean from the view's cost/waste columns
    (waste = (1 - m) * cost), clamped to <= 1.0. Missing stat (waste NULL/NaN)
    or zero cost -> 1.0 (fully used)."""
    if cost and waste is not None:
        return min(1.0, 1.0 - waste / cost)
    return 1.0


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
    resource: str = "gpu",
    exclude_zero_usage: bool = False,
    clusters: list[str] | None = None,
    utilization_ceiling: float = 1.0,
) -> list[UnderuserRow]:
    if resource != "gpu":
        raise ValueError(f"Unsupported resource: {resource!r}")

    with config.db.session() as session:
        rgu_h_expr, true_used_expr, credited_used_expr = _rgu_exprs(utilization_ceiling)
        stmt = _with_rgu_window(
            select(
                col(JobSeriesDB.sarc_user_id),
                # Use func.any_value for email and display_name to allow
                # aggregation across multiple rows per (user_id, cluster)
                # without requiring these fields in the GROUP BY clause.
                # https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUP
                # "In general, if a table is grouped, columns that are not
                # listed in GROUP BY cannot be referenced except in aggregate
                # expressions."
                func.any_value(JobSeriesDB.email).label("email"),
                func.any_value(JobSeriesDB.display_name).label("display_name"),
                col(JobSeriesDB.cluster_name),
                func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
                func.coalesce(func.sum(true_used_expr), 0).label("sum_rgu_true_used"),
                func.coalesce(func.sum(credited_used_expr), 0).label("sum_rgu_used"),
            ),
            start,
            end,
            exclude_zero_usage=exclude_zero_usage,
            rgu_used_expr=rgu_h_expr,
            clusters=clusters,
        ).group_by(JobSeriesDB.sarc_user_id, JobSeriesDB.cluster_name)
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
            waste_ratio = total_wasted / total_rgu_h
            u["waste_ratio"] = waste_ratio
            if waste_ratio >= min_waste_ratio and total_wasted >= min_waste_rgu_hours:
                underuser_ids.append(uid)

        # Per-job data for the identified underusers — same RGU × utilisation
        # pattern.
        jobs_by_user: dict[int, list[UsageJob]] = {uid: [] for uid in underuser_ids}
        if underuser_ids:
            job_rows = session.exec(
                _with_rgu_window(
                    select(
                        col(JobSeriesDB.job_db_id),
                        col(JobSeriesDB.sarc_user_id),
                        col(JobSeriesDB.cluster_name),
                        col(JobSeriesDB.submit_time),
                        rgu_h_expr.label("rgu_hours"),
                        credited_used_expr.label("rgu_used"),
                        col(JobSeriesDB.allocated_gpu_cost),
                        col(JobSeriesDB.allocated_gpu_waste),
                    ).where(col(JobSeriesDB.sarc_user_id).in_(underuser_ids)),
                    start,
                    end,
                    # Must be False here: this is a per-job SELECT with no GROUP
                    # BY. _with_rgu_window implements exclude_zero_usage via
                    # HAVING sum(rgu_used_expr) > 0, which requires a grouped
                    # query. Passing True would generate HAVING without GROUP
                    # BY, which PostgreSQL rejects.
                    # The underusers were already filtered by the aggregated
                    # query above; no per-job filter is needed.
                    exclude_zero_usage=False,
                    rgu_used_expr=rgu_h_expr,
                    clusters=clusters,
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

        top_jobs = sorted(jobs_by_user[uid], key=lambda j: j.wasted, reverse=True)[
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
    top_jobs_per_user: int,
    resource: str = "gpu",
    clusters: list[str] | None = None,
    usage_report_min_usage_rgu_hours: float = 0.0,
) -> list[UsageRow]:
    if resource != "gpu":
        raise ValueError(f"Unsupported resource: {resource!r}")

    with config.db.session() as session:
        rgu_h_expr, true_used_expr, credited_used_expr = _rgu_exprs()
        stmt = _with_rgu_window(
            select(
                col(JobSeriesDB.sarc_user_id),
                func.any_value(JobSeriesDB.email).label("email"),
                func.any_value(JobSeriesDB.display_name).label("display_name"),
                col(JobSeriesDB.cluster_name),
                func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
                func.coalesce(func.sum(true_used_expr), 0).label("sum_rgu_true_used"),
                func.coalesce(func.sum(credited_used_expr), 0).label("sum_rgu_used"),
            ),
            start,
            end,
            exclude_zero_usage=False,
            rgu_used_expr=rgu_h_expr,
            clusters=clusters,
        ).group_by(JobSeriesDB.sarc_user_id, JobSeriesDB.cluster_name)
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
        if all_user_ids:
            job_rows = session.exec(
                _with_rgu_window(
                    select(
                        col(JobSeriesDB.job_db_id),
                        col(JobSeriesDB.sarc_user_id),
                        col(JobSeriesDB.cluster_name),
                        col(JobSeriesDB.submit_time),
                        rgu_h_expr.label("rgu_hours"),
                        true_used_expr.label("rgu_used"),
                        col(JobSeriesDB.allocated_gpu_cost),
                        col(JobSeriesDB.allocated_gpu_waste),
                    ),
                    start,
                    end,
                    exclude_zero_usage=False,
                    rgu_used_expr=rgu_h_expr,
                    clusters=clusters,
                )
            ).all()

            for jr in job_rows:
                uid = jr.sarc_user_id
                if uid not in jobs_by_user:
                    continue
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
        if total_rgu_h <= usage_report_min_usage_rgu_hours:
            continue

        by_cluster = sorted(breakdowns, key=lambda c: c.rgu_hours_used, reverse=True)
        top_jobs = sorted(
            jobs_by_user[uid], key=lambda j: j.rgu_hours_used, reverse=True
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


# ── 6-month historical stats ──


@dataclass
class MonthlyStats:
    label: str  # "YYYY-MM"
    avg_waste_ratio: float


@dataclass
class HistoricalStats:
    # 6 monthly data-points, oldest first.
    months: list[MonthlyStats]
    # Same 6 months one year prior. None when that period has no data at all.
    yoy_months: list[MonthlyStats] | None


def _iter_months(end: datetime, n: int) -> list[tuple[datetime, datetime]]:
    """Return n complete calendar months immediately before *end*, oldest first."""
    year, month = end.year, end.month
    buckets: list[tuple[datetime, datetime]] = []
    for _ in range(n):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        m_start = datetime(year, month, 1, tzinfo=UTC)
        next_month = month + 1
        next_year = year
        if next_month == 13:
            next_month = 1
            next_year += 1
        m_end = datetime(next_year, next_month, 1, tzinfo=UTC)
        buckets.append((m_start, m_end))
    return list(reversed(buckets))


def _query_month_agg(
    session: Session,
    start: datetime,
    end: datetime,
    *,
    exclude_zero_usage: bool = False,
    clusters: list[str] | None = None,
) -> MonthlyStats:
    """Aggregate fleet-level waste stats for a single calendar month window."""
    rgu_h_expr, true_used_expr, credited_used_expr = _rgu_exprs()
    stmt = _with_rgu_window(
        select(
            col(JobSeriesDB.sarc_user_id),
            func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
            func.coalesce(func.sum(true_used_expr), 0).label("sum_rgu_true_used"),
            func.coalesce(func.sum(credited_used_expr), 0).label("sum_rgu_used"),
        ),
        start,
        end,
        exclude_zero_usage=exclude_zero_usage,
        rgu_used_expr=rgu_h_expr,
        clusters=clusters,
    ).group_by(JobSeriesDB.sarc_user_id)
    agg_rows = session.exec(stmt).all()

    total_rgu_h = 0.0
    total_wasted = 0.0
    for row in agg_rows:
        rgu_h, _, rgu_h_wasted = _split_waste(row)
        total_rgu_h += rgu_h
        total_wasted += rgu_h_wasted

    avg_ratio = total_wasted / total_rgu_h if total_rgu_h > 0 else 0.0
    label = start.strftime("%Y-%m")
    return MonthlyStats(label=label, avg_waste_ratio=avg_ratio)


def get_historical_stats(
    end: datetime,
    *,
    resource: str = "gpu",
    months: int = 6,
    exclude_zero_usage: bool = False,
    clusters: list[str] | None = None,
) -> HistoricalStats:
    """Compute 6-month fleet-level waste trend and year-over-year comparison.

    *end* is typically the current run date (datetime.now(UTC)).
    Returns monthly stats for the *months* complete calendar months before
    *end*, plus the same window one year prior (yoy_months=None when no data
    exists).
    """
    # TODO: it could be interesting to have the number of underusers per month
    if resource != "gpu":
        raise ValueError(f"Unsupported resource: {resource!r}")

    current_buckets = _iter_months(end, months)
    yoy_buckets = [
        (
            datetime(s.year - 1, s.month, s.day, tzinfo=UTC),
            datetime(e.year - 1, e.month, e.day, tzinfo=UTC),
        )
        for s, e in current_buckets
    ]

    with config.db.session() as session:
        current_stats = [
            _query_month_agg(
                session, s, e, exclude_zero_usage=exclude_zero_usage, clusters=clusters
            )
            for s, e in current_buckets
        ]
        yoy_raw = [
            _query_month_agg(
                session, s, e, exclude_zero_usage=exclude_zero_usage, clusters=clusters
            )
            for s, e in yoy_buckets
        ]

    has_yoy_data = any(m.avg_waste_ratio > 0 for m in yoy_raw)
    return HistoricalStats(
        months=current_stats, yoy_months=yoy_raw if has_yoy_data else None
    )


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


def get_recurring_underusers(
    end: datetime,
    *,
    min_waste_ratio: float,
    min_waste_rgu_hours: float,
    resource: str = "gpu",
    cluster_share_threshold: float,
    exclude_zero_usage: bool = False,
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
    marked None (no data yet). The "personalized action" flag is set iff a user
    was flagged True in all *recurrence_active_cycles* most-recent cycles.

    Returns a dict of cluster_name -> list[RecurringUserRow] (sorted desc by
    wasted_6w within each cluster), ordered by cluster name.
    """
    cycle_length_weeks = usage_cycle_length_weeks()
    if resource != "gpu":
        raise ValueError(f"Unsupported resource: {resource!r}")

    window_weeks = recurrence_active_cycles * cycle_length_weeks
    anchor = _week_anchor(end)
    agg_start = anchor - timedelta(weeks=window_weeks)

    # ── Per-(user, cluster) aggregate over the full recurrence window ─────────
    with config.db.session() as session:
        rgu_h_expr, true_used_expr, credited_used_expr = _rgu_exprs(utilization_ceiling)
        stmt = _with_rgu_window(
            select(
                col(JobSeriesDB.sarc_user_id),
                func.any_value(JobSeriesDB.email).label("email"),
                func.any_value(JobSeriesDB.display_name).label("display_name"),
                col(JobSeriesDB.cluster_name),
                func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
                func.coalesce(func.sum(true_used_expr), 0).label("sum_rgu_true_used"),
                func.coalesce(func.sum(credited_used_expr), 0).label("sum_rgu_used"),
            ),
            agg_start,
            anchor,
            exclude_zero_usage=exclude_zero_usage,
            rgu_used_expr=rgu_h_expr,
            clusters=clusters,
        ).group_by(JobSeriesDB.sarc_user_id, JobSeriesDB.cluster_name)
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
        if uid not in cluster_users[cluster]:
            cluster_users[cluster][uid] = {
                "email": row.email,
                "display_name": row.display_name,
                "wasted": 0.0,
                "true_wasted": 0.0,
            }
        cluster_users[cluster][uid]["wasted"] += rgu_h_wasted
        cluster_users[cluster][uid]["true_wasted"] += rgu_h_true_wasted

    # ── Cycle membership sets ─────────────────────────────────────────────────
    # Each cycle ends at anchor - i*cycle_length_weeks (always aligned). Cycles
    # whose end is in the future relative to `end` yield None (no data).
    cycle_flagged: list[set[int] | None] = []
    for i in range(recurrence_display_cycles):
        c_end = anchor - timedelta(weeks=i * cycle_length_weeks)
        if c_end > end:
            cycle_flagged.append(None)
            continue
        c_start = c_end - timedelta(weeks=cycle_length_weeks)
        flagged_rows = get_underusers(
            c_start,
            c_end,
            min_waste_ratio=min_waste_ratio,
            min_waste_rgu_hours=min_waste_rgu_hours,
            # Only user_id is used for membership — top jobs are discarded.
            top_jobs_per_user=1,
            resource=resource,
            exclude_zero_usage=exclude_zero_usage,
            clusters=clusters,
            utilization_ceiling=utilization_ceiling,
        )
        cycle_flagged.append({r.user_id for r in flagged_rows})

    # ── Personalized-action aggregate (per active anchor, cross-cluster) ──────
    # For position i, the window is [anchor − (i+active_cycles)·cl, anchor −
    # i·cl]. Index 0 = most-recent anchor (matches the former single-window
    # query).
    pa_window_weeks = recurrence_active_cycles * cycle_length_weeks
    user_pa_flags: dict[int, list[bool]] = {}
    with config.db.session() as session:
        for i in range(recurrence_active_cycles):
            pa_end = anchor - timedelta(weeks=i * cycle_length_weeks)
            pa_start = pa_end - timedelta(weeks=pa_window_weeks)
            pa_stmt = _with_rgu_window(
                select(
                    col(JobSeriesDB.sarc_user_id),
                    func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
                    func.coalesce(func.sum(true_used_expr), 0).label(
                        "sum_rgu_true_used"
                    ),
                    func.coalesce(func.sum(credited_used_expr), 0).label(
                        "sum_rgu_used"
                    ),
                ),
                pa_start,
                pa_end,
                exclude_zero_usage=exclude_zero_usage,
                rgu_used_expr=rgu_h_expr,
                clusters=clusters,
            ).group_by(JobSeriesDB.sarc_user_id)
            for row in session.exec(pa_stmt).all():
                uid = row.sarc_user_id
                if uid not in user_pa_flags:
                    user_pa_flags[uid] = [False] * recurrence_active_cycles
                _, _, pa_rgu_wasted_h = _split_waste(row)
                user_pa_flags[uid][i] = (
                    pa_rgu_wasted_h >= personalized_action_min_waste_rgu_hours
                )

    # ── Per-cluster greedy selection (cumulative share >= cluster_share_threshold) ──
    result: dict[str, list[RecurringUserRow]] = {}
    for cluster, users in sorted(cluster_users.items()):
        cluster_total = sum(u["wasted"] for u in users.values())
        if cluster_total <= 0:
            continue

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
            pa_flags = user_pa_flags.get(uid, [])
            rows_out.append(
                RecurringUserRow(
                    email=u["email"],
                    display_name=u["display_name"],
                    cluster=cluster,
                    wasted_current_active_window=u["wasted"],
                    cluster_share=u["wasted"] / cluster_total,
                    cycles=cycles_for_user,
                    # personalized action should not be triggered for a current
                    # cycle that would ends in the future
                    personalized_action=pa_flags[0] and anchor == end
                    if pa_flags
                    else False,
                    true_wasted=u["true_wasted"],
                    pa_flags=pa_flags,
                )
            )
        result[cluster] = rows_out

    return result
