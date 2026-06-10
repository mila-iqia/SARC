from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy.orm import aliased
from sqlmodel import Session, and_, case, col, func, select

from sarc.config import UTC, config
from sarc.db.job import JobStatisticDB
from sarc.db.job_series import JobSeriesDB

_TOP_JOBS_N = 5


@dataclass
class ClusterBreakdown:
    cluster: str
    # RGU-hours requested for this cluster in the window.
    rgu_hours: float
    # RGU-hours wasted (rgu_hours - rgu_used).  Jobs without a gpu_utilization
    # stat contribute their full RGU-hours to wasted (utilization assumed 0).
    wasted: float
    # Same as rgu_hours; kept as a separate field for clarity in waste_ratio
    # computations (wasted / requested).
    requested: float


@dataclass
class UnderuserJob:
    job_id: int
    cluster: str
    submit_time: datetime
    # RGU-hours unused for this job.  Equals full RGU-hours when utilization
    # is missing (utilization assumed 0).
    rgu_hours_unused: float
    # None when no gpu_utilization stat was recorded for this job.
    gpu_utilization: float | None


@dataclass
class UnderuserRow:
    email: str
    display_name: str
    user_id: int
    # Total RGU-hours requested over the window.
    rgu_hours: float
    # RGU-hours wasted over the window (= rgu_hours - rgu_used).  Used for the
    # activity floor: the floor is compared against *wasted* RGU-hours, not
    # requested, so that users who waste a significant absolute amount are
    # flagged regardless of their total allocation size.
    wasted: float
    requested: float
    # waste_ratio = wasted / requested  (= 1 - rgu_used / rgu_requested)
    waste_ratio: float
    # avg_utilization = 1 - waste_ratio  (= rgu_used / rgu_requested)
    avg_utilization: float
    # Human-readable label for wasted (same value, different name for messages).
    rgu_hours_unused: float
    by_cluster: list[ClusterBreakdown] = field(default_factory=list)
    # Top-N GPU jobs by RGU-hours unused, descending.
    top_jobs: list[UnderuserJob] = field(default_factory=list)


@dataclass
class RecurringUserRow:
    email: str
    display_name: str
    cluster: str
    # Wasted RGU-h for this user in this cluster over the recurrence window.
    wasted_6w: float
    # Fraction of the cluster's total wasted RGU-h in the same window (0..1).
    cluster_share: float
    # Cycle membership: was this user flagged by get_underusers for each window?
    w0: bool  # [end - 14d, end]
    w2: bool  # [end - 28d, end - 14d]
    w4: bool  # [end - 42d, end - 28d]
    w6: bool  # [end - 56d, end - 42d]
    # True iff flagged in all 4 cycles (continuous underuse over ~8 weeks).
    personalized_action: bool


def _rgu_exprs():
    """Return (util_alias, m_mean, rgu_h_expr, rgu_used_expr) for GPU-utilization queries.

    rgu_h_expr    = rgu * elapsed_time / 3600  (RGU-hours per job)
    rgu_used_expr = rgu_h_expr * gpu_utilization_mean, or 0.0 when NaN or NULL.
    The `m == m` NaN idiom (NaN != NaN in SQL) combined with the LEFT JOIN
    else_=0.0 fallback means jobs with no utilization stat count as fully
    wasted (conservative assumption).
    """
    util_alias = aliased(JobStatisticDB)
    m_mean = col(util_alias.mean)
    rgu_h_expr = col(JobSeriesDB.rgu) * col(JobSeriesDB.elapsed_time) / 3600.0
    rgu_used_expr = case(
        (m_mean == m_mean, rgu_h_expr * m_mean),  # noqa: PLR0124
        else_=0.0,
    )
    return util_alias, m_mean, rgu_h_expr, rgu_used_expr


def _with_rgu_window(stmt, util_alias, start, end, *, exclude_zero_usage, rgu_used_expr):
    """Apply the gpu_utilization LEFT JOIN and submit-time / GPU-type / RGU filters."""
    stmt = (
        stmt
        .join(
            util_alias,
            and_(
                col(util_alias.job_id) == col(JobSeriesDB.job_db_id),
                col(util_alias.name) == "gpu_utilization",
            ),
            isouter=True,
        )
        .where(
            col(JobSeriesDB.submit_time) >= start,
            col(JobSeriesDB.submit_time) < end,
            col(JobSeriesDB.allocated_gpu_type).is_not(None),
            col(JobSeriesDB.rgu).is_not(None),
        )
    )
    if exclude_zero_usage:
        stmt = stmt.having(func.coalesce(func.sum(rgu_used_expr), 0) > 0)
    return stmt


def _split_waste(row) -> tuple[float, float, float]:
    rgu_h = float(row.sum_rgu_hours or 0.0)
    rgu_used_h = float(row.sum_rgu_used or 0.0)
    return rgu_h, rgu_used_h, rgu_h - rgu_used_h


def get_underusers(
    start: datetime,
    end: datetime,
    *,
    min_ratio: float,
    min_rgu_hours: float,
    resource: str = "gpu",
    exclude_zero_usage: bool = False,
) -> list[UnderuserRow]:
    if resource != "gpu":
        raise ValueError(f"Unsupported resource: {resource!r}")

    with config().db.session() as session:
        util, m_mean, rgu_h_expr, rgu_used_expr = _rgu_exprs()
        stmt = (
            _with_rgu_window(
                select(
                    JobSeriesDB.sarc_user_id,
                    JobSeriesDB.email,
                    JobSeriesDB.display_name,
                    JobSeriesDB.cluster_name,
                    func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
                    func.coalesce(func.sum(rgu_used_expr), 0).label("sum_rgu_used"),
                ),
                util,
                start,
                end,
                exclude_zero_usage=exclude_zero_usage,
                rgu_used_expr=rgu_used_expr,
            )
            .group_by(
                JobSeriesDB.sarc_user_id,
                JobSeriesDB.email,
                JobSeriesDB.display_name,
                JobSeriesDB.cluster_name,
            )
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
            rgu_h, rgu_used_h, wasted_h = _split_waste(row)
            user_data[uid]["clusters"].append(
                ClusterBreakdown(
                    cluster=row.cluster_name or "unknown",
                    rgu_hours=rgu_h,
                    wasted=wasted_h,
                    requested=rgu_h,
                )
            )

        underuser_ids: list[int] = []
        for uid, u in user_data.items():
            clusters = u["clusters"]
            total_rgu_h = sum(c.rgu_hours for c in clusters)
            total_wasted = sum(c.wasted for c in clusters)
            u["total_rgu_h"] = total_rgu_h
            u["total_wasted"] = total_wasted
            if total_rgu_h <= 0:
                continue
            waste_ratio = total_wasted / total_rgu_h
            u["waste_ratio"] = waste_ratio
            if waste_ratio >= min_ratio and total_wasted >= min_rgu_hours:
                underuser_ids.append(uid)

        # Per-job data for the identified underusers — same RGU × utilisation pattern.
        jobs_by_user: dict[int, list[UnderuserJob]] = {uid: [] for uid in underuser_ids}
        if underuser_ids:
            util, m_mean, rgu_h_expr, rgu_used_expr = _rgu_exprs()
            job_rows = session.exec(
                _with_rgu_window(
                    select(
                        JobSeriesDB.job_db_id,
                        JobSeriesDB.sarc_user_id,
                        JobSeriesDB.cluster_name,
                        JobSeriesDB.submit_time,
                        rgu_h_expr.label("rgu_hours"),
                        rgu_used_expr.label("rgu_used"),
                        m_mean.label("util_mean"),
                    ).where(col(JobSeriesDB.sarc_user_id).in_(underuser_ids)),
                    util,
                    start,
                    end,
                    exclude_zero_usage=False,
                    rgu_used_expr=rgu_used_expr,
                )
            ).all()

            for jr in job_rows:
                rgu_h = float(jr.rgu_hours or 0.0)
                rgu_used_h = float(jr.rgu_used or 0.0)
                util = float(jr.util_mean) if jr.util_mean is not None else None
                jobs_by_user[jr.sarc_user_id].append(
                    UnderuserJob(
                        job_id=jr.job_db_id,
                        cluster=jr.cluster_name or "unknown",
                        submit_time=jr.submit_time,
                        rgu_hours_unused=rgu_h - rgu_used_h,
                        gpu_utilization=util,
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
            jobs_by_user[uid], key=lambda j: j.rgu_hours_unused, reverse=True
        )[:_TOP_JOBS_N]

        result.append(
            UnderuserRow(
                email=u["email"],
                display_name=u["display_name"],
                user_id=uid,
                rgu_hours=total_rgu_h,
                wasted=total_wasted,
                requested=total_rgu_h,
                waste_ratio=waste_ratio,
                avg_utilization=1.0 - waste_ratio,
                rgu_hours_unused=total_wasted,
                by_cluster=by_cluster,
                top_jobs=top_jobs,
            )
        )

    return result


# ── 6-month historical stats (Module C digest) ────────────────────────────────


@dataclass
class MonthlyStats:
    label: str  # "YYYY-MM"
    avg_waste_ratio: float
    above_threshold_count: int


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
    min_ratio: float,
    min_rgu_hours: float,
    exclude_zero_usage: bool = False,
) -> MonthlyStats:
    """Aggregate fleet-level waste stats for a single calendar month window."""
    util, m_mean, rgu_h_expr, rgu_used_expr = _rgu_exprs()
    stmt = (
        _with_rgu_window(
            select(
                JobSeriesDB.sarc_user_id,
                func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
                func.coalesce(func.sum(rgu_used_expr), 0).label("sum_rgu_used"),
            ),
            util,
            start,
            end,
            exclude_zero_usage=exclude_zero_usage,
            rgu_used_expr=rgu_used_expr,
        )
        .group_by(JobSeriesDB.sarc_user_id)
    )
    agg_rows = session.exec(stmt).all()

    total_rgu_h = 0.0
    total_wasted = 0.0
    above_count = 0
    for row in agg_rows:
        rgu_h, rgu_used_h, wasted_h = _split_waste(row)
        total_rgu_h += rgu_h
        total_wasted += wasted_h
        if rgu_h > 0:
            ratio = wasted_h / rgu_h
            if ratio >= min_ratio and wasted_h >= min_rgu_hours:
                above_count += 1

    avg_ratio = total_wasted / total_rgu_h if total_rgu_h > 0 else 0.0
    label = start.strftime("%Y-%m")
    return MonthlyStats(
        label=label, avg_waste_ratio=avg_ratio, above_threshold_count=above_count
    )


def get_historical_stats(
    end: datetime,
    *,
    min_ratio: float,
    min_rgu_hours: float,
    resource: str = "gpu",
    months: int = 6,
    exclude_zero_usage: bool = False,
) -> HistoricalStats:
    """Compute 6-month fleet-level waste trend and year-over-year comparison.

    *end* is typically the current run date (datetime.now(UTC)).
    Returns monthly stats for the *months* complete calendar months before *end*,
    plus the same window one year prior (yoy_months=None when no data exists).
    """
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

    with config().db.session() as session:
        current_stats = [
            _query_month_agg(
                session,
                s,
                e,
                min_ratio=min_ratio,
                min_rgu_hours=min_rgu_hours,
                exclude_zero_usage=exclude_zero_usage,
            )
            for s, e in current_buckets
        ]
        yoy_raw = [
            _query_month_agg(
                session,
                s,
                e,
                min_ratio=min_ratio,
                min_rgu_hours=min_rgu_hours,
                exclude_zero_usage=exclude_zero_usage,
            )
            for s, e in yoy_buckets
        ]

    has_yoy_data = any(
        m.avg_waste_ratio > 0 or m.above_threshold_count > 0 for m in yoy_raw
    )
    return HistoricalStats(
        months=current_stats, yoy_months=yoy_raw if has_yoy_data else None
    )


# ── Recurring-underusers table (Module C digest) ─────────────────────────────


def get_recurring_underusers(
    end: datetime,
    *,
    min_ratio: float,
    min_rgu_hours: float,
    resource: str = "gpu",
    window_weeks: int = 6,
    cluster_share_threshold: float = 0.30,
    exclude_zero_usage: bool = False,
) -> dict[str, list[RecurringUserRow]]:
    """Return per-cluster top wasters for the recurring-underusers digest table.

    Selection: for each cluster, rank users by wasted RGU-h over the rolling
    *window_weeks* × 7-day window (ending at *end*) and include the top users
    until their cumulative waste reaches >= *cluster_share_threshold* of that
    cluster's total wasted RGU-h.

    Cycle flags: for each of the 4 most-recent 14-day windows (W0, W-2, W-4,
    W-6 — rolling from *end*), call get_underusers to determine per-user
    membership.  The "personalized action" flag is set iff a user was flagged in
    all 4 cycles.

    Returns a dict of cluster_name -> list[RecurringUserRow] (sorted desc by
    wasted_6w within each cluster), ordered by cluster name.
    """
    if resource != "gpu":
        raise ValueError(f"Unsupported resource: {resource!r}")

    agg_start = end - timedelta(days=window_weeks * 7)

    # ── Per-(user, cluster) aggregate over the full recurrence window ─────────
    with config().db.session() as session:
        util, m_mean, rgu_h_expr, rgu_used_expr = _rgu_exprs()
        stmt = (
            _with_rgu_window(
                select(
                    JobSeriesDB.sarc_user_id,
                    JobSeriesDB.email,
                    JobSeriesDB.display_name,
                    JobSeriesDB.cluster_name,
                    func.coalesce(func.sum(rgu_h_expr), 0).label("sum_rgu_hours"),
                    func.coalesce(func.sum(rgu_used_expr), 0).label("sum_rgu_used"),
                ),
                util,
                agg_start,
                end,
                exclude_zero_usage=exclude_zero_usage,
                rgu_used_expr=rgu_used_expr,
            )
            .group_by(
                JobSeriesDB.sarc_user_id,
                JobSeriesDB.email,
                JobSeriesDB.display_name,
                JobSeriesDB.cluster_name,
            )
        )
        agg_rows = session.exec(stmt).all()

    # ── Organise wasted RGU-h per (cluster, user) ─────────────────────────────
    # cluster -> user_id -> {email, display_name, wasted}
    cluster_users: dict[str, dict[int, dict]] = {}
    for row in agg_rows:
        cluster = row.cluster_name or "unknown"
        uid = row.sarc_user_id
        rgu_h, rgu_used_h, wasted = _split_waste(row)
        if wasted <= 0:
            continue
        if cluster not in cluster_users:
            cluster_users[cluster] = {}
        if uid not in cluster_users[cluster]:
            cluster_users[cluster][uid] = {
                "email": row.email,
                "display_name": row.display_name,
                "wasted": 0.0,
            }
        cluster_users[cluster][uid]["wasted"] += wasted

    # ── Cycle membership sets (W0 .. W-6) ────────────────────────────────────
    # W0 = [end-14d, end], W-2 = [end-28d, end-14d], ...
    cycle_flagged: list[set[int]] = []
    for i in range(4):
        c_end = end - timedelta(days=i * 14)
        c_start = c_end - timedelta(days=14)
        flagged_rows = get_underusers(
            c_start,
            c_end,
            min_ratio=min_ratio,
            min_rgu_hours=min_rgu_hours,
            resource=resource,
            exclude_zero_usage=exclude_zero_usage,
        )
        cycle_flagged.append({r.user_id for r in flagged_rows})

    # ── Per-cluster greedy selection (cumulative share >= threshold) ──────────
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

        w0_set, w2_set, w4_set, w6_set = cycle_flagged
        rows_out = []
        for uid, u in selected:
            w0 = uid in w0_set
            w2 = uid in w2_set
            w4 = uid in w4_set
            w6 = uid in w6_set
            rows_out.append(
                RecurringUserRow(
                    email=u["email"],
                    display_name=u["display_name"],
                    cluster=cluster,
                    wasted_6w=u["wasted"],
                    cluster_share=u["wasted"] / cluster_total,
                    w0=w0,
                    w2=w2,
                    w4=w4,
                    w6=w6,
                    personalized_action=(w0 and w2 and w4 and w6),
                )
            )
        result[cluster] = rows_out

    return result
