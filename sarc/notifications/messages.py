from collections import defaultdict

from sarc.notifications.underusage import HistoricalStats, UnderuserJob, UnderuserRow


def _first_name(display_name: str) -> str:
    return display_name.split()[0]


def _pct(fraction: float) -> str:
    return f"{fraction * 100:.1f} %"


def _fmt_h(hours: float) -> str:
    return f"{hours:.1f}"


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
            if len(jobs) == 1:
                prefix = "  └─"
            elif i == 0:
                prefix = "  ┌─"
            elif i == len(jobs) - 1:
                prefix = "  └─"
            else:
                prefix = "  ├─"
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


def _historical_section(stats: HistoricalStats) -> str:
    lines = ["", "── 6-Month Trend ──"]
    lines.append(f"{'Month':<9}  {'Avg waste ratio':>17}  {'Above threshold':>15}")
    for m in stats.months:
        lines.append(
            f"  {m.label}   {_pct(m.avg_waste_ratio):>17}  {m.above_threshold_count:>12} user(s)"
        )

    if stats.yoy_months is not None:
        lines.append("")
        lines.append("── Year-over-Year (same 6 months, prior year) ──")
        lines.append(f"{'Month':<9}  {'Avg waste ratio':>17}  {'Above threshold':>15}")
        for m in stats.yoy_months:
            lines.append(
                f"  {m.label}   {_pct(m.avg_waste_ratio):>17}  {m.above_threshold_count:>12} user(s)"
            )
    return "\n".join(lines)


def build_admin_digest(
    rows: list[UnderuserRow],
    *,
    period: str,
    top_n: int = 16,
    historical: HistoricalStats | None = None,
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

    return "\n".join(lines)
