from collections import defaultdict
from collections.abc import Callable
from datetime import date

from sarc.config import ConfigurationError, config
from sarc.notifications.mrkdwn import to_slack_mrkdwn
from sarc.notifications.underusage import (
    HistoricalStats,
    MonthlyStats,
    RecurringUserRow,
    UnderuserRow,
    UsageRow,
    usage_cycle_length_weeks,
)


def _fmt_rgu_int(hours: float) -> str:
    """Format RGU-hours as an integer with a space thousands separator."""
    return f"{int(round(hours)):,}".replace(",", " ")


def _first_name(display_name: str | None) -> str:
    parts = (display_name or "").split()
    return parts[0] if parts else "there"


def _pct(fraction: float) -> str:
    return f"{fraction * 100:.1f} %"


def _fmt_h(hours: float) -> str:
    return f"{hours:.1f}"


def _tree_prefix(i: int, n: int) -> str:
    if n == 1 or i == n - 1:
        return "└─"
    return "┌─" if i == 0 else "├─"


def _footer_lines(
    dashboard_url: str | None,
    resources_section: str | None,
    help_section: str | None = None,
) -> list[str]:
    lines: list[str] = []
    if dashboard_url is not None:
        lines += ["", f"Track your usage over time: {dashboard_url}"]
    if resources_section is not None:
        lines += ["", resources_section]
    if help_section is not None:
        lines += ["", help_section]
    return lines


def _jobs_section(top_jobs: list, *, rgu_value: Callable, suffix: str) -> str:
    by_cluster: dict[str, list] = defaultdict(list)
    for job in top_jobs:
        by_cluster[job.cluster].append(job)

    cluster_order = sorted(
        by_cluster, key=lambda c: sum(rgu_value(j) for j in by_cluster[c]), reverse=True
    )

    lines = []
    for cluster in cluster_order:
        jobs = by_cluster[cluster]
        # 2-space gutter matches the width of the "┌─"/"└─" job-line prefixes
        # below, so cluster names align with job names.
        lines.append(f"{'':2} Cluster {cluster}")
        for i, job in enumerate(jobs):
            prefix = _tree_prefix(i, len(jobs))
            date_str = job.submit_time.strftime("%Y-%m-%d")
            util_str = (
                f"{job.gpu_sm_occupancy * 100:.0f} %"
                if job.gpu_sm_occupancy is not None
                else "n/a"
            )
            lines.append(
                f"{prefix} job_{job.job_id} ({date_str})"
                f" — {_fmt_h(rgu_value(job))} {suffix}"
                f"  (GPU utilization: {util_str})"
            )
    return "\n".join(lines)


def build_user_dm(row: UnderuserRow, *, window_weeks: int) -> str:
    """Build a plain-text DM for a single underusing researcher."""
    if not config.notifications:
        raise ConfigurationError("No notifications configuration found in config")
    parts = [
        config.notifications.underusage_report_template.format(
            name=_first_name(row.display_name),
            window_weeks=window_weeks,
            avg_utilization=_pct(row.avg_utilization),
            rgu_hours_wasted=_fmt_h(row.wasted),
            jobs_section=_jobs_section(
                row.top_jobs, rgu_value=lambda j: j.wasted, suffix="RGU-h unused"
            ),
        ).rstrip()
    ]

    parts += _footer_lines(
        config.notifications.dashboard_url,
        config.notifications.resources_section,
        config.notifications.help_section,
    )
    return to_slack_mrkdwn("\n".join(parts))


def build_usage_report(row: UsageRow, *, window_weeks: int) -> str:
    """Build a plain-text usage report for a single researcher.

    Neutral wording — shows used volume, no waste/unused framing.
    """
    if not config.notifications:
        raise ConfigurationError("No notifications configuration found in config")
    parts = [
        config.notifications.usage_report_template.format(
            name=_first_name(row.display_name),
            window_weeks=window_weeks,
            avg_utilization=_pct(row.avg_utilization),
            rgu_hours_allocated=_fmt_h(row.rgu_hours),
            jobs_section=_jobs_section(
                row.top_jobs, rgu_value=lambda j: j.rgu_hours_used, suffix="RGU-h"
            ),
        ).rstrip()
    ]

    parts += _footer_lines(
        config.notifications.dashboard_url, config.notifications.resources_section
    )
    return to_slack_mrkdwn("\n".join(parts))


def _month_table(title: str, months: list[MonthlyStats]) -> list[str]:
    pct_strs = [_pct(m.avg_waste_ratio) for m in months]

    month_w = max(len("Month"), max((len(m.label) for m in months), default=0))
    pct_w = max(len("Avg waste ratio"), max((len(s) for s in pct_strs), default=0))

    rows = [title, f"{'Month'.ljust(month_w)}  {'Avg waste ratio'.rjust(pct_w)}"]
    for m, pct_s in zip(months, pct_strs):
        rows.append(f"{m.label.ljust(month_w)}  {pct_s.rjust(pct_w)}")
    return rows


def _historical_section(stats: HistoricalStats) -> str:
    n = len(stats.months)
    lines = ["", *_month_table(f"── {n}-Month Trend ──", stats.months)]

    if stats.yoy_months is not None:
        lines += [
            "",
            *_month_table(
                f"── Year-over-Year (same {n} months, prior year) ──", stats.yoy_months
            ),
        ]

    return "\n".join(lines)


def build_recurring_table(
    recurring: dict[str, list[RecurringUserRow]],
    *,
    cluster_share_threshold: float,
    active_cycles: int,
    cycle_dates: list[date] | None = None,
) -> str:
    """Build the recurring-underusers per-cluster table for the admin digest.

    *cycle_dates* — n date objects [W0, W-k, W-2k, …] — when provided, renders
    column headers as "MM-DD" strings; when None, derives labels from the
    configured usage_cycle_length_weeks (e.g. "W0", "W-2", "W-4", …).  Cycle
    cells whose flag is None (future cycle, no data yet) are rendered as blank.

    A "|" separator is rendered after the last active cycle (index *active_cycles*).
    Per-cycle ⚑ is shown on ▲ cells in positions 0..active_cycles-1 whose
    pa_flags entry indicates ceiling-adjusted cross-cluster waste ≥ the action threshold.

    Pure function — no I/O, deterministic for fixed input.
    """
    if not recurring:
        return ""

    cycle_length_weeks = usage_cycle_length_weeks()
    window_weeks = active_cycles * cycle_length_weeks
    share_pct = f"{cluster_share_threshold * 100:.0f} %"
    flag_window = active_cycles
    if cycle_dates is not None:
        flag_labels = tuple(d.strftime("%m-%d") for d in cycle_dates)
    else:
        first_nonempty = next((rows for rows in recurring.values() if rows), None)
        if first_nonempty is None:
            return ""
        n_cycles = len(first_nonempty[0].cycles)
        flag_labels = tuple(
            "W0" if i == 0 else f"W-{i * cycle_length_weeks}" for i in range(n_cycles)
        )
    flag_ws = [len(lbl) for lbl in flag_labels]

    def _flag_cell(flag: bool | None, w: int, has_peak: bool = False) -> str:
        if flag is None:
            return " " * (2 + w)
        if flag:
            symbol = "⚑▲" if has_peak else "▲"
        else:
            symbol = "✓"
        return f"  {symbol.rjust(w)}"

    def _build_flag_header() -> str:
        parts = []
        for i, lbl in enumerate(flag_labels):
            if i == flag_window:
                parts.append("  |")
            parts.append(f"  {lbl}")
        return "".join(parts)

    def _build_flag_cells(row: RecurringUserRow) -> str:
        cycle_vals = row.cycles
        parts = []
        for i, w in enumerate(flag_ws):
            if i == flag_window:
                parts.append("  |")
            flag = cycle_vals[i]
            has_peak = (
                i < active_cycles and i < len(row.pa_flags) and bool(row.pa_flags[i])
            )
            parts.append(_flag_cell(flag, w, has_peak))
        return "".join(parts)

    flag_header = _build_flag_header()
    sections = []

    for cluster, rows in sorted(recurring.items()):
        if not rows:
            continue

        email_w = max(len(r.email) for r in rows)
        header = (
            f"{'':2} {'User':<{email_w}}"
            f"  {'Unused RGU-h':>12}"
            f"  {'Share':>5}" + flag_header + "  Action"
        )
        lines = [
            f"Recurring underusers (last {window_weeks} weeks) — Cluster {cluster}",
            f"(top users accounting for {share_pct} of the cluster's unused RGU-h)",
            header,
        ]

        n = len(rows)
        for i, row in enumerate(rows):
            pfx = _tree_prefix(i, n)
            flags = _build_flag_cells(row)
            action = "  ⚑ personalized" if row.flagged_for_personalized_action else ""
            lines.append(
                f"{pfx} {row.email:<{email_w}}"
                f"  {_fmt_rgu_int(row.wasted_current_active_window):>12}"
                f"  {row.cluster_share * 100:>3.0f} %" + flags + action
            )

        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def build_admin_digest(
    rows: list[UnderuserRow],
    *,
    period: str,
    cluster_share_threshold: float,
    active_cycles: int,
    top_n: int,
    historical: HistoricalStats | None = None,
    recurring: dict[str, list[RecurringUserRow]] | None = None,
    cycle_dates: list[date] | None = None,
) -> str:
    """Build a plain-text admin digest.

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
                f"{ws.rjust(wasted_w)} RGU-h unused  "
                f"{rs.rjust(ratio_w)}"
            )

    if historical is not None:
        lines.append(_historical_section(historical))

    if recurring is not None:
        recurring_text = build_recurring_table(
            recurring,
            cluster_share_threshold=cluster_share_threshold,
            active_cycles=active_cycles,
            cycle_dates=cycle_dates,
        )
        if recurring_text:
            lines += ["", recurring_text]

    return "\n".join(lines)
