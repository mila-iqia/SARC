import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import gifnoc
import simple_parsing

from sarc.config import config
from sarc.notifications.messages import (
    build_admin_digest,
    build_usage_report,
    build_user_dm,
    split_usage_report_recipients,
)
from sarc.notifications.slack import SendStatus, SlackClient
from sarc.notifications.underusage import (
    get_all_users_usage,
    get_cycle_dates,
    get_historical_stats,
    get_recurring_underusers,
    get_underusers,
)

logger = logging.getLogger(__name__)


@dataclass
class _DeliveryResult:
    email: str
    display_name: str
    status: str  # "dm_sent" | "skipped" | "failed"
    detail: str = field(default="")


def _delivery_counts(results: list[_DeliveryResult]) -> dict[str, int]:
    return {
        "dm_sent": sum(1 for r in results if r.status == "dm_sent"),
        "skipped": sum(1 for r in results if r.status == "skipped"),
        "failed": sum(1 for r in results if r.status == "failed"),
    }


def _build_delivery_footer(
    results: list[_DeliveryResult], *, title: str, count_label: str, count: int
) -> str:
    counts = _delivery_counts(results)
    lines = [
        f"=== {title} ===",
        (
            f"{count_label}={count}  dm_sent={counts['dm_sent']}"
            f"  skipped={counts['skipped']}  failed={counts['failed']}"
        ),
    ]
    failures = [r for r in results if r.status == "failed"]
    if failures:
        lines.append("Failures:")
        lines.extend(f"- {r.email}: {r.detail}" for r in failures)
    return "\n".join(lines)


def _deliver(
    rows: list, build_fn: Callable, *, slack: SlackClient
) -> list[_DeliveryResult]:
    results: list[_DeliveryResult] = []
    for row in rows:
        text = build_fn(row)
        slack_res = slack.dm_user(row.email, text, preformatted=True)
        if slack_res.status == SendStatus.OK:
            results.append(_DeliveryResult(row.email, row.display_name, "dm_sent"))
        else:
            results.append(
                _DeliveryResult(row.email, row.display_name, "failed", slack_res.detail)
            )
    return results


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _today_utc() -> datetime:
    return _now_utc().replace(hour=0, minute=0, second=0, microsecond=0)


def _iso_week(dt: datetime) -> int:
    return dt.isocalendar().week


def _userfacing_print(*args, **kwargs) -> None:
    print(*args, **kwargs)  # noqa: T201


@dataclass
class UnderusageNotifyCommand:
    """Preview or send resource-underusage notifications (dry-run by default)."""

    config: Path | None = None
    min_waste_ratio: float | None = simple_parsing.field(
        default=None,
        alias=["--min-ratio"],
        help="Minimum waste ratio threshold (overrides config).",
    )
    min_waste_rgu_hours: float | None = simple_parsing.field(
        default=None,
        alias=["--min-rgu-hours"],
        help="Minimum RGU-hours floor (overrides config).",
    )
    resource: str = "gpu"
    send: bool = simple_parsing.field(
        action="store_true", help="Send notifications (default: dry-run, prints only)."
    )
    no_dms: bool = simple_parsing.field(
        action="store_true",
        alias=["--no-dms"],
        help="Skip per-user DMs even when --send is set.",
    )
    as_of: str | None = simple_parsing.field(
        default=None,
        alias=["--as-of"],
        help="Simulate a run as of this date (YYYY-MM-DD, UTC). Default: now. "
        "Anchors the window, all queries, and the usage-cycle eligibility.",
    )

    def execute(self) -> int:
        if self.config is None:
            return self._exec()
        with gifnoc.use(self.config):
            return self._exec()

    def _exec(self) -> int:
        ncfg = config.notifications
        if ncfg is None:
            logger.error("No notifications configuration found in config")
            return -1
        if not ncfg.enabled:
            logger.info("Underusage notifications disabled (enabled=false); skipping")
            return 0

        window_weeks = ncfg.usage_cycle_length_weeks
        min_waste_ratio = (
            self.min_waste_ratio
            if self.min_waste_ratio is not None
            else ncfg.min_waste_ratio
        )
        min_waste_rgu_hours = (
            self.min_waste_rgu_hours
            if self.min_waste_rgu_hours is not None
            else ncfg.min_waste_rgu_hours
        )

        if self.as_of is not None:
            try:
                parsed = datetime.fromisoformat(self.as_of)
                end = (
                    parsed.replace(tzinfo=UTC)
                    if parsed.tzinfo is None
                    else parsed.astimezone(UTC)
                )
            except ValueError:
                logger.error("Invalid --as-of date %r: expected YYYY-MM-DD", self.as_of)
                return -1
        else:
            end = _today_utc()
        start = end - timedelta(weeks=window_weeks)
        period = f"{start.date()} – {end.date()}"

        week_num = _iso_week(end)
        # underusage_report_eligible: week is a multiple of window_weeks
        # (controls preview section)
        underusage_report_eligible = week_num % window_weeks == 0
        # dms_will_send: additionally blocked by --no-dms gate and requires
        # send_underusage_report config (controls actual sends)
        dms_will_send = (
            underusage_report_eligible
            and not self.no_dms
            and ncfg.send_underusage_report
        )

        usage_report_window_weeks = ncfg.usage_report_cycles * window_weeks
        usage_report_eligible = week_num % usage_report_window_weeks == 0
        usage_report_will_send = (
            usage_report_eligible and not self.no_dms and ncfg.send_usage_report
        )

        if self.as_of is not None and end > _today_utc():
            _userfacing_print(
                "Note: --as-of is in the future; the window may contain no jobs.",
                file=sys.stderr,
            )
            _userfacing_print(file=sys.stderr)

        if not self.send:
            _userfacing_print("=== DRY RUN — nothing will be sent ===", file=sys.stderr)
            _userfacing_print(file=sys.stderr)

        if underusage_report_eligible:
            _userfacing_print(
                f"ISO week {week_num} (multiple of {window_weeks}) — Underusage report eligible this run.",
                file=sys.stderr,
            )
        if usage_report_eligible:
            _userfacing_print(
                f"ISO week {week_num} (multiple of {usage_report_window_weeks}) — Usage report eligible this run.",
                file=sys.stderr,
            )
        _userfacing_print(file=sys.stderr)

        clusters = ncfg.clusters or None

        rows = get_underusers(
            start,
            end,
            min_waste_ratio=min_waste_ratio,
            min_waste_rgu_hours=min_waste_rgu_hours,
            top_jobs_per_user=ncfg.top_jobs_per_user,
            resource=self.resource,
            exclude_zero_usage=True,
            clusters=clusters,
            utilization_ceiling=ncfg.utilization_ceiling,
        )
        historical = get_historical_stats(
            end,
            resource=self.resource,
            months=ncfg.historical_months,
            exclude_zero_usage=True,
            clusters=clusters,
        )
        recurring = get_recurring_underusers(
            end,
            min_waste_ratio=min_waste_ratio,
            min_waste_rgu_hours=min_waste_rgu_hours,
            resource=self.resource,
            cluster_share_threshold=ncfg.recurrence_cluster_share,
            exclude_zero_usage=True,
            recurrence_display_cycles=ncfg.recurrence_display_cycles,
            recurrence_active_cycles=ncfg.recurrence_active_cycles,
            clusters=clusters,
            utilization_ceiling=ncfg.utilization_ceiling,
            personalized_action_min_waste_rgu_hours=ncfg.personalized_action_min_waste_rgu_hours,
        )

        _userfacing_print(f"Recipients ({len(rows)} user(s) flagged):")
        for row in rows:
            _userfacing_print(f"  - {row.display_name} ({row.email})")
        _userfacing_print()

        cycle_dates = get_cycle_dates(end, ncfg.recurrence_display_cycles)
        digest = build_admin_digest(
            rows,
            period=period,
            cluster_share_threshold=ncfg.recurrence_cluster_share,
            active_cycles=ncfg.recurrence_active_cycles,
            top_n=ncfg.digest_top_n,
            historical=historical,
            recurring=recurring,
            cycle_dates=cycle_dates,
        )
        _userfacing_print("=== Admin Digest ===")
        _userfacing_print(digest)

        if rows and underusage_report_eligible:
            _userfacing_print()
            _userfacing_print("=== Under Usage Report Previews ===")
            for row in rows:
                _userfacing_print(f"\n--- {row.display_name} ({row.email}) ---")
                dm = build_user_dm(
                    row,
                    window_weeks=window_weeks,
                    dashboard_url=ncfg.dashboard_url,
                    help_section=ncfg.help_section,
                )
                _userfacing_print(dm)

        report_recipients = []
        usage_report_skipped = []
        if usage_report_eligible:
            usage_start = end - timedelta(weeks=usage_report_window_weeks)
            usage_rows = get_all_users_usage(
                usage_start,
                end,
                top_jobs_per_user=ncfg.top_jobs_per_user,
                resource=self.resource,
                clusters=clusters,
                usage_report_min_usage_rgu_hours=ncfg.usage_report_min_usage_rgu_hours,
            )
            underuser_emails = {r.email for r in rows}
            report_recipients, usage_report_skipped = split_usage_report_recipients(
                usage_rows, underuser_emails
            )
            if report_recipients:
                _userfacing_print()
                skip_note = (
                    f" ({len(usage_report_skipped)} already getting the underusage alert)"
                    if usage_report_skipped
                    else ""
                )
                _userfacing_print(
                    f"=== Usage Report Previews ({len(report_recipients)} recipient(s)){skip_note} ==="
                )
                for row in report_recipients:
                    _userfacing_print(f"\n--- {row.display_name} ({row.email}) ---")
                    report_text = build_usage_report(
                        row,
                        window_weeks=usage_report_window_weeks,
                        dashboard_url=ncfg.dashboard_url,
                        help_section=ncfg.help_section,
                    )
                    _userfacing_print(report_text)

        if not self.send:
            return 0

        # === SEND MODE ===
        slack_client = SlackClient(ncfg.slack.token)

        delivery_results: list[_DeliveryResult] = []

        if dms_will_send:
            delivery_results = _deliver(
                rows,
                lambda row: build_user_dm(
                    row,
                    window_weeks=window_weeks,
                    dashboard_url=ncfg.dashboard_url,
                    help_section=ncfg.help_section,
                ),
                slack=slack_client,
            )
        elif rows:
            if not underusage_report_eligible:
                reason = "off_cycle_week"
            elif self.no_dms:
                reason = "no_dms_flag"
            else:
                reason = "send_underusage_report_disabled"
            for row in rows:
                delivery_results.append(
                    _DeliveryResult(row.email, row.display_name, "skipped", reason)
                )

        report_results: list[_DeliveryResult] = []
        if usage_report_will_send:
            report_results = _deliver(
                report_recipients,
                lambda row: build_usage_report(
                    row,
                    window_weeks=usage_report_window_weeks,
                    dashboard_url=ncfg.dashboard_url,
                    help_section=ncfg.help_section,
                ),
                slack=slack_client,
            )
        elif usage_report_eligible and report_recipients:
            reason = "no_dms_flag" if self.no_dms else "send_usage_report_disabled"
            for row in report_recipients:
                report_results.append(
                    _DeliveryResult(row.email, row.display_name, "skipped", reason)
                )

        footer = None
        report_footer = None
        if underusage_report_eligible:
            footer = _build_delivery_footer(
                delivery_results,
                title="Delivery Summary",
                count_label="flagged",
                count=len(rows),
            )
        if usage_report_eligible:
            report_footer = _build_delivery_footer(
                report_results,
                title="Usage Report Summary",
                count_label="eligible",
                count=len(report_recipients),
            )

        channel_res = slack_client.post_channel(
            ncfg.slack.channel, digest, preformatted=True
        )
        if channel_res.status != SendStatus.OK:
            logger.error(
                "Failed to post admin digest to %s: %s",
                ncfg.slack.channel,
                channel_res.detail,
            )

        # Delivery summaries go in the digest's thread: rapporteur only relays
        # the last few ERROR logs, so counts and failed-user emails need a
        # guaranteed home in the channel. If the digest post failed, ts is None
        # and the replies land as regular channel messages instead.
        if footer:
            slack_client.post_channel(
                ncfg.slack.channel, footer, thread_ts=channel_res.ts
            )
        if report_footer:
            slack_client.post_channel(
                ncfg.slack.channel, report_footer, thread_ts=channel_res.ts
            )

        counts = _delivery_counts(delivery_results)
        report_counts = _delivery_counts(report_results)
        logger.info(
            "Underusage notification run: flagged=%d dm_sent=%d skipped=%d failed=%d"
            " | report_eligible=%s report_sent=%d report_skipped=%d report_failed=%d",
            len(rows),
            counts["dm_sent"],
            counts["skipped"],
            counts["failed"],
            usage_report_eligible,
            report_counts["dm_sent"],
            report_counts["skipped"],
            report_counts["failed"],
        )

        _userfacing_print()
        _userfacing_print("=== Send Complete ===")
        if footer:
            _userfacing_print(footer)
        if report_footer:
            _userfacing_print()
            _userfacing_print(report_footer)

        return 0
