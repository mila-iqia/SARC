from collections import defaultdict
from datetime import date

from sarc.notifications.underusage import (
    HistoricalStats,
    MonthlyStats,
    RecurringUserRow,
    UnderuserJob,
    UnderuserRow,
    UsageJob,
    UsageRow,
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


def _usage_jobs_section(top_jobs: list[UsageJob]) -> str:
    by_cluster: dict[str, list[UsageJob]] = defaultdict(list)
    for job in top_jobs:
        by_cluster[job.cluster].append(job)

    cluster_order = sorted(
        by_cluster,
        key=lambda c: sum(j.rgu_hours_used for j in by_cluster[c]),
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
                f" — {_fmt_h(job.rgu_hours_used)} RGU-h"
                f"  (GPU utilization: {util_str})"
            )
    return "\n".join(lines)


def build_usage_report(
    row: UsageRow,
    *,
    window_days: int,
    dashboard_url: str | None = None,
    help_section: str | None = None,
) -> str:
    """Build a Phase 3 plain-text usage report for a single researcher.

    Neutral wording — shows used volume, no waste/unused framing.
    Pure function — no I/O, deterministic for fixed input.
    """
    parts = [
        f"Hi {_first_name(row.display_name)},",
        "",
        f"Over the last {window_days} days, your jobs used on average"
        f" {_pct(row.avg_utilization)} of the GPU resources you"
        f" requested ({_fmt_h(row.rgu_hours_used)} RGU-hours total).",
    ]

    if row.top_jobs:
        parts += [
            "",
            "Your top jobs by GPU usage:",
            "",
            _usage_jobs_section(row.top_jobs),
        ]

    if dashboard_url is not None:
        parts += ["", f"Track your usage over time: {dashboard_url}"]

    if help_section is not None:
        parts += ["", help_section]

    return "\n".join(parts)


# def _clamp01(value: float) -> float:
#     return max(0.0, min(1.0, value))


def _month_table(title: str, months: list[MonthlyStats]) -> list[str]:
    pct_strs = [_pct(m.avg_waste_ratio) for m in months]
    count_strs = [f"{m.above_threshold_count} user(s)" for m in months]

    month_w = max(len("Month"), max((len(m.label) for m in months), default=0))
    pct_w = max(len("Avg waste ratio"), max((len(s) for s in pct_strs), default=0))
    count_w = max(len("Above threshold"), max((len(s) for s in count_strs), default=0))

    rows = [
        title,
        f"{'Month'.ljust(month_w)}  {'Avg waste ratio'.rjust(pct_w)}  {'Above threshold'.rjust(count_w)}",
    ]
    for m, pct_s, count_s in zip(months, pct_strs, count_strs):
        rows.append(
            f"{m.label.ljust(month_w)}  {pct_s.rjust(pct_w)}  {count_s.rjust(count_w)}"
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
    cycle_dates: list[date] | None = None,
) -> str:
    """Build the recurring-underusers per-cluster table for the admin digest.

    *cycle_dates* — four date objects [W0, W-2, W-4, W-6] — when provided,
    renders column headers as "MM-DD" strings; when None, falls back to the
    fixed labels "W0", "W-2", "W-4", "W-6".  Cycle cells whose flag is None
    (future cycle, no data yet) are rendered as blank.

    Pure function — no I/O, deterministic for fixed input.
    """
    if not recurring:
        return ""

    share_pct = f"{cluster_share_threshold * 100:.0f} %"
    flag_attrs = ("w0", "w2", "w4", "w6")
    if cycle_dates is not None:
        flag_labels = tuple(d.strftime("%m-%d") for d in cycle_dates)
    else:
        flag_labels = ("W0", "W-2", "W-4", "W-6")
    flag_ws = [len(lbl) for lbl in flag_labels]
    flag_header = "".join(f"   {lbl}" for lbl in flag_labels)
    sections = []

    for cluster, rows in sorted(recurring.items()):
        if not rows:
            continue

        email_w = max(len(r.email) for r in rows)
        header = (
            f"  {'':2} {'User':<{email_w}}"
            f"  {'Wasted RGU-h':>12}"
            f"  {'Share':>7}"
            + flag_header
            + "   Action"
        )
        lines = [
            f"Recurring underusers (last {window_weeks} weeks) — Cluster {cluster}",
            f"(top users accounting for ≥ {share_pct} of the cluster's wasted RGU-h)",
            header,
        ]

        n = len(rows)
        for i, row in enumerate(rows):
            pfx = _tree_prefix(i, n)

            def _flag_cell(flag: bool | None, w: int) -> str:
                if flag is None:
                    return " " * (3 + w)
                return f"   {('✓' if flag else '✗').rjust(w)}"

            flags = "".join(
                _flag_cell(getattr(row, attr), w)
                for attr, w in zip(flag_attrs, flag_ws)
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


def split_usage_report_recipients(
    usage_rows: list[UsageRow],
    underuser_emails: set[str],
) -> tuple[list[UsageRow], list[UsageRow]]:
    """Partition active users into (report_recipients, underuser_skipped).

    Users whose email is in *underuser_emails* receive the underusage alert
    instead of the usage report.  The two lists are disjoint; their union
    covers all of *usage_rows*.  Pure function — no I/O.
    """
    report = [r for r in usage_rows if r.email not in underuser_emails]
    skipped = [r for r in usage_rows if r.email in underuser_emails]
    return report, skipped


def build_admin_digest(
    rows: list[UnderuserRow],
    *,
    period: str,
    top_n: int = 16,
    historical: HistoricalStats | None = None,
    recurring: dict[str, list[RecurringUserRow]] | None = None,
    cycle_dates: list[date] | None = None,
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

    clusters = [r.by_cluster[0].cluster if r.by_cluster else "unknown" for r in ranked]
    wasted_s = [_fmt_h(r.wasted) for r in ranked]
    ratio_s = [_pct(r.waste_ratio) for r in ranked]

    if ranked:
        name_w = max(len(r.display_name) for r in ranked)
        email_w = max(len(r.email) for r in ranked)
        cluster_w = max(len(c) for c in clusters)
        wasted_w = max(len(s) for s in wasted_s)
        ratio_w = max(len(s) for s in ratio_s)

        for i, (row, cluster, ws, rs) in enumerate(
            zip(ranked, clusters, wasted_s, ratio_s), start=1
        ):
            lines.append(
                f" {i:2d}.  "
                f"{row.display_name.ljust(name_w)}  "
                f"{row.email.ljust(email_w)}  "
                f"{cluster.ljust(cluster_w)}  "
                f"{ws.rjust(wasted_w)} RGU-h wasted  "
                f"{rs.rjust(ratio_w)}"
            )

    if historical is not None:
        lines.append(_historical_section(historical))

    if recurring is not None:
        recurring_text = build_recurring_table(recurring, cycle_dates=cycle_dates)
        if recurring_text:
            lines += ["", recurring_text]

    return "\n".join(lines)
