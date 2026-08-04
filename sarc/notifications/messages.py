from collections import defaultdict
from collections.abc import Callable
from datetime import date

from sarc.config import ConfigurationError, config
from sarc.notifications.mrkdwn import to_slack_mrkdwn
from sarc.notifications.slack import MENTION_TOKEN
from sarc.notifications.usage import (
    RecurringUserRow,
    UsageRow,
    usage_cycle_length_weeks,
)


def _fmt_rgu_int(hours: float, hours_per_unit: float) -> str:
    """Format RGU-hours as an integer with a space thousands separator."""
    return f"{int(round(hours / hours_per_unit)):,}".replace(",", " ")


def _fmt_rgu(hours: float, hours_per_unit: float) -> str:
    return f"{hours / hours_per_unit:.1f}"


def _pct(fraction: float) -> str:
    return f"{fraction * 100:.1f} %"


def _date_range(start: date, end: date) -> str:
    return f"{start:%b %d, %Y} – {end:%b %d, %Y}"


def _dashboard_url(base_url: str, start: date, end: date) -> str:
    return f"{base_url}?start={start:%Y-%m-%d}&end={end:%Y-%m-%d}"


def _tree_prefix(i: int, n: int) -> str:
    if n == 1 or i == n - 1:
        return "└─"
    return "┌─" if i == 0 else "├─"


def _jobs_section(
    jobs: list, *, rgu_value: Callable, hours_per_unit: float, suffix: str
) -> str:
    by_cluster: dict[str, list] = defaultdict(list)
    for job in jobs:
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
                f" — {_fmt_rgu(rgu_value(job), hours_per_unit)} {suffix}"
                f"  (SM occupancy: {util_str})"
            )
    return "\n".join(lines)


def build_user_dm(
    row: UsageRow, *, window_weeks: int, window_start: date, window_end: date
) -> str:
    """Build a plain-text DM for a single underusing researcher."""
    if not config.notifications:
        raise ConfigurationError("No notifications configuration found in config")
    ncfg = config.notifications
    text = ncfg.underusage_report_template.format(
        name=MENTION_TOKEN,
        window_weeks=window_weeks,
        window_range=_date_range(window_start, window_end),
        rgu_allocated=_fmt_rgu(row.rgu_hours, ncfg.rgu_unit.hours_per_unit),
        rgu_wasted=_fmt_rgu(row.wasted, ncfg.rgu_unit.hours_per_unit),
        user_allocated=_fmt_rgu(row.rgu_hours, ncfg.user_unit.hours_per_unit),
        user_wasted=_fmt_rgu(row.wasted, ncfg.user_unit.hours_per_unit),
        avg_utilization=_pct(row.avg_utilization),
        rgu_unit=ncfg.rgu_unit.unit_long,
        user_unit=ncfg.user_unit.unit_long,
        bottom_jobs_count=len(row.bottom_jobs),
        bottom_jobs_section=_jobs_section(
            row.bottom_jobs,
            rgu_value=lambda j: j.wasted,
            hours_per_unit=ncfg.rgu_unit.hours_per_unit,
            suffix=f"{ncfg.rgu_unit.unit} unused",
        ),
        dashboard_url=_dashboard_url(ncfg.dashboard_url, window_start, window_end),
    ).rstrip()
    return to_slack_mrkdwn(text)


def build_usage_report(
    row: UsageRow, *, window_weeks: int, window_start: date, window_end: date
) -> str:
    """Build a plain-text usage report for a single researcher.

    Neutral wording — shows used volume, no waste/unused framing.
    """
    if not config.notifications:
        raise ConfigurationError("No notifications configuration found in config")
    ncfg = config.notifications
    text = ncfg.usage_report_template.format(
        name=MENTION_TOKEN,
        window_weeks=window_weeks,
        window_range=_date_range(window_start, window_end),
        rgu_allocated=_fmt_rgu(row.rgu_hours, ncfg.rgu_unit.hours_per_unit),
        user_allocated=_fmt_rgu(row.rgu_hours, ncfg.user_unit.hours_per_unit),
        avg_utilization=_pct(row.avg_utilization),
        rgu_unit=ncfg.rgu_unit.unit_long,
        user_unit=ncfg.user_unit.unit_long,
        top_jobs_count=len(row.top_jobs),
        top_jobs_section=_jobs_section(
            row.top_jobs,
            rgu_value=lambda j: j.rgu_hours_used,
            hours_per_unit=ncfg.rgu_unit.hours_per_unit,
            suffix=ncfg.rgu_unit.unit,
        ),
        bottom_jobs_count=len(row.bottom_jobs),
        bottom_jobs_section=_jobs_section(
            row.bottom_jobs,
            rgu_value=lambda j: j.wasted,
            hours_per_unit=ncfg.rgu_unit.hours_per_unit,
            suffix=f"{ncfg.rgu_unit.unit} unused",
        ),
        dashboard_url=_dashboard_url(ncfg.dashboard_url, window_start, window_end),
    ).rstrip()
    return to_slack_mrkdwn(text)


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
    configured usage_cycle_length_weeks (e.g. "W0", "W-2", "W-4", …). Cycle
    cells whose flag is None (future cycle, no data yet) are rendered as blank.

    A "|" separator is rendered after the last active cycle (index
    *active_cycles*). Per-cycle ⚑ is shown on every ▲ cell whose pa_flags entry
    indicates ceiling-adjusted cross-cluster waste ≥ the action threshold
    (rendered "⚑▲"). A sustained run of ⚑ peaks escalates to a
    restrictive-action marker "!!⚑▲" on the newest cell of the run (see
    RecurringUserRow.restrictive_action_flags).

    Pure function — no I/O, deterministic for fixed input.
    """
    if not recurring:
        return ""

    if not config.notifications:
        raise ConfigurationError("No notifications configuration found in config")
    ncfg = config.notifications

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

    def _flag_cell(symbol: str, w: int) -> str:
        if not symbol:
            return " " * (2 + w)
        return f"  {symbol.rjust(w)}"

    def _build_flag_header() -> str:
        parts = []
        for i, lbl in enumerate(flag_labels):
            if i == flag_window:
                parts.append("  |")
            parts.append(f"  {lbl}")
        return "".join(parts)

    def _build_flag_cells(row: RecurringUserRow) -> str:
        parts = []
        for i, w in enumerate(flag_ws):
            if i == flag_window:
                parts.append("  |")
            parts.append(_flag_cell(row.cycle_symbol(i), w))
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
            f"  {'Share':>5}" + flag_header
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
            lines.append(
                f"{pfx} {row.email:<{email_w}}"
                f"  {_fmt_rgu_int(row.wasted_current_active_window, ncfg.rgu_unit.hours_per_unit):>12}"
                f"  {row.cluster_share * 100:>3.0f} %" + flags
            )

        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def build_admin_digest(
    rows: list[UsageRow],
    *,
    period: str,
    cluster_share_threshold: float,
    active_cycles: int,
    top_n: int,
    recurring: dict[str, list[RecurringUserRow]] | None = None,
    cycle_dates: list[date] | None = None,
) -> str:
    """Build a plain-text admin digest.

    Ranks underusers by RGU-hours wasted (descending), capped at top_n.
    Pure function — no I/O, deterministic for fixed input.
    """
    if not config.notifications:
        raise ConfigurationError("No notifications configuration found in config")
    ncfg = config.notifications

    ranked = sorted(rows, key=lambda r: r.wasted, reverse=True)[:top_n]

    lines = [
        f"Weekly GPU Underusage Digest — {period}",
        f"{len(rows)} user(s) flagged this week.",
        "",
    ]

    clusters = [r.by_cluster[0].cluster if r.by_cluster else "unknown" for r in ranked]
    wasted_s = [_fmt_rgu(r.wasted, ncfg.rgu_unit.hours_per_unit) for r in ranked]
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
                f"{ws.rjust(wasted_w)} {ncfg.rgu_unit.unit} unused  "
                f"{rs.rjust(ratio_w)}"
            )

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
