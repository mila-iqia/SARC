from collections import defaultdict

from sarc.notifications.underusage import (
    HistoricalStats,
    MonthlyStats,
    RecurringUserRow,
    UnderuserJob,
    UnderuserRow,
)


def _fmt_rgu_int(hours: float) -> str:
    """Format RGU-hours as an integer with a space thousands separator."""
    return f"{int(round(hours)):,}".replace(",", " ")


def _first_name(display_name: str) -> str:
    return display_name.split()[0]


def _pct(fraction: float) -> str:
    return f"{fraction * 100:.1f} %"


def _fmt_h(hours: float) -> str:
    return f"{hours:.1f}"


def _tree_prefix(i: int, n: int) -> str:
    if n == 1 or i == n - 1:
        return "  └─"
    return "  ┌─" if i == 0 else "  ├─"


def _jobs_section(top_jobs: list[UnderuserJob]) -> str:
    # Group by cluster preserving descending-waste order within each cluster.
    by_cluster: dict[str, list[UnderuserJob]] = defaultdict(list)
    for job in top_jobs:
        by_cluster[job.cluster].append(job)

    cluster_order = sorted(
        by_cluster,
        key=lambda c: sum(j.rgu_hours_unused for j in by_cluster[c]),
        reverse=True,
    )

    lines = []
    for cluster in cluster_order:
        jobs = by_cluster[cluster]
        lines.append(f"  Cluster {cluster}")
        for i, job in enumerate(jobs):
            prefix = _tree_prefix(i, len(jobs))
            date_str = job.submit_time.strftime("%Y-%m-%d")
            util_str = (
                f"{job.gpu_utilization * 100:.0f} %"
                if job.gpu_utilization is not None
                else "n/a"
            )
            lines.append(
                f"{prefix} job_{job.job_id} ({date_str})"
                f" — {_fmt_h(job.rgu_hours_unused)} RGU-h unused"
                f"  (GPU utilization: {util_str})"
            )
    return "\n".join(lines)


def build_user_dm(
    row: UnderuserRow,
    *,
    window_days: int,
    dashboard_url: str | None = None,
    help_section: str | None = None,
) -> str:
    """Build a Module A plain-text DM for a single underusing researcher.

    Pure function — no I/O, deterministic for fixed input.
    """
    parts = [
        f"Hi {_first_name(row.display_name)},",
        "",
        f"Over the last {window_days} days, your jobs utilized on average"
        f" {_pct(row.avg_utilization)} of requested GPUs,"
        f" leaving {_fmt_h(row.rgu_hours_unused)} RGU-hours unused.",
    ]

    if row.top_jobs:
        parts += [
            "",
            "Jobs with the lowest GPU utilization:",
            "",
            _jobs_section(row.top_jobs),
        ]

    if dashboard_url is not None:
        parts += ["", f"Track your usage over time: {dashboard_url}"]

    if help_section is not None:
        parts += ["", help_section]

    return "\n".join(parts)


# def _clamp01(value: float) -> float:
#     return max(0.0, min(1.0, value))


def _month_table(title: str, months: list[MonthlyStats]) -> list[str]:
    rows = [title, f"{'Month':<9}  {'Avg waste ratio':>17}  {'Above threshold':>15}"]
    for m in months:
        rows.append(
            f"  {m.label}   {_pct(m.avg_waste_ratio):>17}  {m.above_threshold_count:>12} user(s)"
        )
    return rows


def _historical_section(stats: HistoricalStats) -> str:
    lines = ["", *_month_table("── 6-Month Trend ──", stats.months)]

    if stats.yoy_months is not None:
        lines += ["", *_month_table("── Year-over-Year (same 6 months, prior year) ──", stats.yoy_months)]

    return "\n".join(lines)


def build_recurring_table(
    recurring: dict[str, list[RecurringUserRow]],
    *,
    window_weeks: int = 6,
    cluster_share_threshold: float = 0.30,
) -> str:
    """Build the recurring-underusers per-cluster table for the admin digest.

    Pure function — no I/O, deterministic for fixed input.
    """
    if not recurring:
        return ""

    share_pct = f"{cluster_share_threshold * 100:.0f} %"
    sections = []

    for cluster, rows in sorted(recurring.items()):
        if not rows:
            continue

        email_w = max(len(r.email) for r in rows)
        # Header aligns "User" with the email column (after the tree prefix).
        header = (
            f"  {'':3} {'User':<{email_w}}"
            f"  {'Wasted RGU-h':>12}"
            f"  {'Share':>6}"
            f"   W0  W-2  W-4  W-6  Action"
        )
        lines = [
            f"Recurring underusers (last {window_weeks} weeks) — Cluster {cluster}",
            f"(top users accounting for ≥ {share_pct} of the cluster's wasted RGU-h)",
            header,
        ]

        n = len(rows)
        for i, row in enumerate(rows):
            pfx = _tree_prefix(i, n)

            flags = "".join(
                f"   {'✓' if f else '✗'}"
                for f in (row.w0, row.w2, row.w4, row.w6)
            )
            action = "   ⚑ personalized" if row.personalized_action else ""
            lines.append(
                f"{pfx} {row.email:<{email_w}}"
                f"  {_fmt_rgu_int(row.wasted_6w):>12}"
                f"  {row.cluster_share * 100:>5.0f} %"
                + flags
                + action
            )

        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def build_admin_digest(
    rows: list[UnderuserRow],
    *,
    period: str,
    top_n: int = 16,
    historical: HistoricalStats | None = None,
    recurring: dict[str, list[RecurringUserRow]] | None = None,
) -> str:
    """Build a Module C plain-text admin digest.

    Ranks underusers by RGU-hours wasted (descending), capped at top_n.
    Pure function — no I/O, deterministic for fixed input.
    """
    ranked = sorted(rows, key=lambda r: r.wasted, reverse=True)[:top_n]

    lines = [
        f"Weekly GPU Underusage Digest — {period}",
        f"{len(rows)} user(s) flagged this week.",
        "",
    ]

    for i, row in enumerate(ranked, start=1):
        primary = row.by_cluster[0].cluster if row.by_cluster else "unknown"
        lines.append(
            f" {i:2d}. {row.display_name} ({row.email})"
            f"  —  {primary}"
            f"  |  {_fmt_h(row.wasted)} RGU-h wasted"
            f"  |  waste ratio: {_pct(row.waste_ratio)}"
        )

    if historical is not None:
        lines.append(_historical_section(historical))

    if recurring is not None:
        recurring_text = build_recurring_table(recurring)
        if recurring_text:
            lines += ["", recurring_text]

    return "\n".join(lines)
